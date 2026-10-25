package server

import (
	"errors"
	"net/http"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/batcher"
	"github.com/niwoerner/otelake/internal/traces"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// handleTraces acks a request only once its spans are stored in a Parquet file.
func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}

	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Traces().SpanCount() > 0 {
		rec, err := traces.Flatten(memory.DefaultAllocator, req.Traces())
		if err != nil {
			s.logger.Error("flattening traces failed", "error", err)
			writeError(w, "failed to process traces", http.StatusInternalServerError)
			return
		}
		defer rec.Release()

		switch err := s.spans.Write(rec); {
		case err == nil:
		case errors.Is(err, batcher.ErrFull):
			w.Header().Set("Retry-After", "1")
			writeError(w, "server busy, retry later", http.StatusTooManyRequests)
			return
		default: // a failed flush (logged by the batcher) or shutting down
			writeError(w, "temporarily unable to store traces", http.StatusServiceUnavailable)
			return
		}
	}

	resp, err := ptraceotlp.NewExportResponse().MarshalProto()
	if err != nil {
		s.logger.Error("encoding export response failed", "error", err)
		writeError(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	writeResponse(w, resp)
}
