// Package auth provides JWT-based HTTP authentication middleware and RBAC helpers.
//
// # Usage
//
//	secret := []byte(os.Getenv("JWT_SECRET"))
//
//	// Middleware: parse + validate JWT, inject claims into context.
//	mux.Handle("/api/", auth.JWT(secret)(apiHandler))
//
//	// Role-based access control — chain after JWT.
//	mux.Handle("/admin/", auth.JWT(secret)(auth.Require("admin")(adminHandler)))
//
//	// Extract claims in a handler.
//	func myHandler(w http.ResponseWriter, r *http.Request) {
//	    claims, ok := auth.ClaimsFromContext(r.Context())
//	    if !ok { http.Error(w, "unauthorized", 401); return }
//	    fmt.Fprintf(w, "hello %s", claims.Subject)
//	}
//
// # Token generation (helper — use in tests or token issuance service)
//
//	token, err := auth.IssueToken(auth.Claims{
//	    UserID: "u1",
//	    Roles:  []string{"admin", "user"},
//	}, secret, 24*time.Hour)
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims are the JWT payload fields extracted from a valid token.
type Claims struct {
	UserID string   `json:"uid"`
	Roles  []string `json:"roles,omitempty"`
	jwt.RegisteredClaims
}

// HasRole reports whether the claims contain role.
func (c Claims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasAnyRole reports whether the claims contain at least one of the given roles.
func (c Claims) HasAnyRole(roles ...string) bool {
	for _, role := range roles {
		if c.HasRole(role) {
			return true
		}
	}
	return false
}

// --------------------------------------------------------------------------
// Context

type claimsKey struct{}

// ContextWithClaims returns a new context with the given claims attached.
func ContextWithClaims(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// ClaimsFromContext retrieves claims injected by the JWT middleware.
func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(Claims)
	return c, ok
}

// --------------------------------------------------------------------------
// Middleware

// JWT returns a middleware that validates the Bearer token in the
// Authorization header and injects the parsed Claims into the request context.
// It responds 401 if the token is missing or invalid, 403 if expired.
func JWT(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := bearerToken(r)
			if err != nil {
				http.Error(w, "unauthorized: "+err.Error(), http.StatusUnauthorized)
				return
			}

			claims, err := parseToken(raw, secret)
			if err != nil {
				if errors.Is(err, jwt.ErrTokenExpired) {
					http.Error(w, "token expired", http.StatusForbidden)
					return
				}
				http.Error(w, "unauthorized: invalid token", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r.WithContext(ContextWithClaims(r.Context(), claims)))
		})
	}
}

// Require returns a middleware that enforces at least one of the given roles
// is present in the request's Claims. Must be chained after JWT middleware.
func Require(roles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFromContext(r.Context())
			if !ok {
				http.Error(w, "forbidden: no claims", http.StatusForbidden)
				return
			}
			if len(roles) > 0 && !claims.HasAnyRole(roles...) {
				http.Error(w, "forbidden: insufficient role", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// --------------------------------------------------------------------------
// Token issuance

// IssueToken creates and signs a JWT with the given claims and TTL.
func IssueToken(c Claims, secret []byte, ttl time.Duration) (string, error) {
	now := time.Now()
	c.RegisteredClaims = jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		Subject:   c.UserID,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	signed, err := tok.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, nil
}

// --------------------------------------------------------------------------
// Helpers

func bearerToken(r *http.Request) (string, error) {
	hdr := r.Header.Get("Authorization")
	if hdr == "" {
		// Also check cookie for browser clients.
		if c, err := r.Cookie("access_token"); err == nil {
			return c.Value, nil
		}
		return "", errors.New("missing Authorization header")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(hdr, prefix) {
		return "", errors.New("Authorization header must start with 'Bearer '")
	}
	tok := strings.TrimSpace(strings.TrimPrefix(hdr, prefix))
	if tok == "" {
		return "", errors.New("empty token")
	}
	return tok, nil
}

func parseToken(raw string, secret []byte) (Claims, error) {
	var c Claims
	_, err := jwt.ParseWithClaims(raw, &c, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return secret, nil
	}, jwt.WithExpirationRequired())
	if err != nil {
		return Claims{}, err
	}
	return c, nil
}
