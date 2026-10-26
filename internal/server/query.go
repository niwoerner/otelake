package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"time"

	"github.com/niwoerner/otelake/internal/query"
)

const maxQueryBodyBytes = 16 << 10

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if s.queries == nil {
		http.Error(w, "retrieval requires local storage", http.StatusServiceUnavailable)
		return
	}
	if !s.queryGate.TryAcquire(1) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "query concurrency limit reached", http.StatusTooManyRequests)
		return
	}
	defer s.queryGate.Release(1)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.Query.Timeout)
	defer cancel()
	// A context alone does not interrupt a blocked request-body read.
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	if err := controller.SetReadDeadline(deadline); err == nil {
		defer func() {
			// Close with the deadline still active: net/http may drain an
			// unread body on Close before it can flush the response.
			_ = r.Body.Close()
			_ = controller.SetReadDeadline(time.Time{})
		}()
	}
	r = r.WithContext(ctx)
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQueryBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		SQL string `json:"sql"`
	}
	err = decoder.Decode(&request)
	if err == nil {
		if trailing := decoder.Decode(new(any)); trailing != io.EOF {
			err = trailing
			if err == nil {
				err = errors.New("multiple JSON values")
			}
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			s.writeQueryResult(w, r, nil, context.DeadlineExceeded)
		} else if errors.As(err, &tooLarge) {
			http.Error(w, "query body exceeds 16 KiB", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "invalid query JSON", http.StatusBadRequest)
		}
		return
	}
	result, err := s.queries.Query(r.Context(), request.SQL)
	s.writeQueryResult(w, r, result, err)
}

func (s *Server) writeQueryResult(w http.ResponseWriter, r *http.Request, result any, err error) {
	err = errors.Join(err, r.Context().Err())
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			http.Error(w, "query timed out", http.StatusGatewayTimeout)
		case errors.Is(err, context.Canceled):
			http.Error(w, "query canceled", http.StatusRequestTimeout)
		case errors.Is(err, query.ErrResultLimit):
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	// Keep the deadline through net/http's final flush; it resets it for reuse.
	deadline, _ := r.Context().Deadline()
	_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	body, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "result cannot be encoded as JSON; cast non-finite numbers to VARCHAR", http.StatusBadRequest)
		return
	}
	if len(body) > query.MaxResponseBytes {
		http.Error(w, query.ErrResultLimit.Error(), http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}
