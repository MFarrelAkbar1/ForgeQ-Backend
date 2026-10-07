// Package auth holds the shared-secret checks used by the API (bearer token)
// and the dashboard (HTTP basic auth). An empty key disables authentication,
// which is meant for local development only.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// Equal reports whether given matches want in constant time. Both values are
// hashed first so the comparison time does not reveal the key's length either.
func Equal(given, want string) bool {
	g := sha256.Sum256([]byte(given))
	w := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(g[:], w[:]) == 1
}

// BearerToken extracts the token from an "Authorization: Bearer <token>" header.
func BearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

// ValidBearer reports whether r carries key as its bearer token. It is always
// true when key is empty (authentication disabled).
func ValidBearer(r *http.Request, key string) bool {
	if key == "" {
		return true
	}
	token, ok := BearerToken(r)
	return ok && Equal(token, key)
}

// ValidBasic reports whether r carries key as its basic-auth password (any
// username). It is always true when key is empty.
func ValidBasic(r *http.Request, key string) bool {
	if key == "" {
		return true
	}
	_, password, ok := r.BasicAuth()
	return ok && Equal(password, key)
}
