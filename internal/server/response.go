package server

import (
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

// writeError answers with an OTLP error response: a protobuf google.rpc.Status
// carrying a message that is safe to show to clients.
func writeError(w http.ResponseWriter, msg string, code int) {
	body, _ := proto.Marshal(&status.Status{Message: msg})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// writeResponse answers 200 with a protobuf-encoded OTLP export response.
func writeResponse(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(body)
}
