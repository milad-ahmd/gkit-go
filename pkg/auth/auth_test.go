package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/auth"
)

var secret = []byte("super-secret-test-key")

func token(t *testing.T, claims auth.Claims, ttl time.Duration) string {
	t.Helper()
	tok, err := auth.IssueToken(claims, secret, ttl)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	return tok
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFromContext(r.Context())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(claims.UserID))
}

func TestJWT_ValidToken(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u1", Roles: []string{"user"}}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	auth.JWT(secret)(http.HandlerFunc(okHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "u1" {
		t.Errorf("body = %q, want u1", rec.Body.String())
	}
}

func TestJWT_MissingToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	auth.JWT(secret)(http.HandlerFunc(okHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestJWT_ExpiredToken(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u1"}, -time.Second)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	auth.JWT(secret)(http.HandlerFunc(okHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestJWT_InvalidSignature(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u1"}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok+"tampered")
	rec := httptest.NewRecorder()

	auth.JWT(secret)(http.HandlerFunc(okHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestRequire_HasRole(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u1", Roles: []string{"admin"}}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	handler := auth.JWT(secret)(auth.Require("admin")(http.HandlerFunc(okHandler)))
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestRequire_MissingRole(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u1", Roles: []string{"user"}}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	handler := auth.JWT(secret)(auth.Require("admin")(http.HandlerFunc(okHandler)))
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestJWT_CookieFallback(t *testing.T) {
	tok := token(t, auth.Claims{UserID: "u2"}, time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "access_token", Value: tok})
	rec := httptest.NewRecorder()

	auth.JWT(secret)(http.HandlerFunc(okHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
