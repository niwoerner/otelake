package server

import (
	"net/http"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/metrics"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

// handleMetrics acks a request only once its data points are stored in a Parquet file.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	body, release, ok := s.readBody(w, r)
	if !ok {
		return
	}
	defer release()

	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Metrics().DataPointCount() > 0 {
		rec, err := metrics.Flatten(memory.DefaultAllocator, req.Metrics())
		if err != nil {
			s.logger.Error("flattening metrics failed", "error", err)
			writeError(w, "failed to process metrics", http.StatusInternalServerError)
			return
		}
		defer rec.Release()
		if !s.store(w, s.metrics, rec, "metrics") {
			return
		}
	}

	resp, err := pmetricotlp.NewExportResponse().MarshalProto()
	if err != nil {
		s.logger.Error("encoding export response failed", "error", err)
		writeError(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	writeResponse(w, resp)
}
