package auth

import (
	"net/http/httptest"
	"testing"
)

func TestValidBearer(t *testing.T) {
	tests := []struct {
		name, key, header string
		want              bool
	}{
		{"disabled", "", "", true},
		{"missing header", "s3cret", "", false},
		{"correct", "s3cret", "Bearer s3cret", true},
		{"scheme is case-insensitive", "s3cret", "bearer s3cret", true},
		{"wrong key", "s3cret", "Bearer nope", false},
		{"prefix of key", "s3cret", "Bearer s3c", false},
		{"wrong scheme", "s3cret", "Basic s3cret", false},
		{"empty token", "s3cret", "Bearer ", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}
			if got := ValidBearer(r, tt.key); got != tt.want {
				t.Errorf("ValidBearer = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidBasic(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	if ValidBasic(r, "s3cret") {
		t.Error("no credentials should be rejected")
	}
	r.SetBasicAuth("anyone", "s3cret")
	if !ValidBasic(r, "s3cret") {
		t.Error("correct password should be accepted with any username")
	}
	r.SetBasicAuth("admin", "wrong")
	if ValidBasic(r, "s3cret") {
		t.Error("wrong password should be rejected")
	}
	if !ValidBasic(httptest.NewRequest("GET", "/", nil), "") {
		t.Error("empty key disables auth")
	}
}
