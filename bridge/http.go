package bridge

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const maxInternalBody = 16 * 1024

func HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func InternalHandler(manager *Manager, token string) http.Handler {
	mux := http.NewServeMux()
	protect := func(handler http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" || !validBearer(r.Header.Get("Authorization"), token) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			handler(w, r)
		})
	}
	mux.Handle("/internal/backends", protect(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"backends": manager.Backends()})
	}))
	mux.Handle("/internal/private/claim", protect(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			BackendID string `json:"backend_id"`
			RequestID string `json:"request_id"`
			Receipt   string `json:"receipt"`
		}
		if err := decodeBoundedJSON(w, r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
			return
		}
		delivery, err := manager.ClaimPrivate(body.BackendID, body.RequestID, body.Receipt)
		if err != nil {
			writeManagerError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, delivery)
	}))
	mux.Handle("/internal/private/ack", protect(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			DeliveryID string `json:"delivery_id"`
			Status     string `json:"status"`
		}
		if err := decodeBoundedJSON(w, r, &body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
			return
		}
		if err := manager.AckPrivate(body.DeliveryID, body.Status); err != nil {
			writeManagerError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}))
	return mux
}

func validBearer(header, token string) bool {
	want := "Bearer " + token
	return len(header) == len(want) && subtle.ConstantTimeCompare([]byte(header), []byte(want)) == 1
}

func decodeBoundedJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxInternalBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeManagerError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrAlreadyClaimed):
		status = http.StatusConflict
	case errors.Is(err, ErrInvalidRequest):
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": sanitizeStatus(err)})
}

func sanitizeStatus(err error) string {
	switch {
	case errors.Is(err, ErrAlreadyClaimed):
		return "already_claimed"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrInvalidRequest):
		return "invalid_request"
	default:
		return "request_failed"
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func MCPAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || !validBearer(r.Header.Get("Authorization"), token) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func InternalPath(path string) bool { return strings.HasPrefix(path, "/internal/") }
