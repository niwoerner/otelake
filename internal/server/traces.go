package server

import "net/http"

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}
	s.logger.Info("received traces", "bytes", len(body))
}
