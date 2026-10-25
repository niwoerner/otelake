package server

import (
	"net/http"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/logs"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// handleLogs acks a request only once its log records are stored in a Parquet file.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, release, ok := s.readBody(w, r)
	if !ok {
		return
	}
	defer release()

	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Logs().LogRecordCount() > 0 {
		rec, err := logs.Flatten(memory.DefaultAllocator, req.Logs())
		if err != nil {
			s.logger.Error("flattening logs failed", "error", err)
			writeError(w, "failed to process logs", http.StatusInternalServerError)
			return
		}
		defer rec.Release()
		if !s.store(w, s.logs, rec, "logs") {
			return
		}
	}

	resp, err := plogotlp.NewExportResponse().MarshalProto()
	if err != nil {
		s.logger.Error("encoding export response failed", "error", err)
		writeError(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	writeResponse(w, resp)
}
