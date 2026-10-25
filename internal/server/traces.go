package server

import (
	"net/http"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}

	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.logger.Info("received traces", "spans", req.Traces().SpanCount())
}
