package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requireKey authenticates before the handler reads or decodes the body.
func requireKey(keys []string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		matched := 0
		if len(values) == 1 {
			scheme, token, ok := strings.Cut(values[0], " ")
			if ok && strings.EqualFold(scheme, "Bearer") && token != "" && !strings.ContainsAny(token, " \t\r\n,") {
				for _, key := range keys {
					matched |= subtle.ConstantTimeCompare([]byte(token), []byte(key))
				}
			}
		}
		if matched != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="otelake"`)
			writeError(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
