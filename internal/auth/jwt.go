// Package auth extracts identity from the request's bearer JWT. The token is
// decoded but NOT cryptographically verified: ownership of the integration is
// validated downstream by the Port API call that uses the same token.
package auth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// orgIDClaim is the JWT payload claim holding the organization id. Isolated
// here so it is a one-line change if the claim name differs.
const orgIDClaim = "orgId"

var (
	// ErrMissingToken is returned when no bearer token is present.
	ErrMissingToken = errors.New("missing bearer token")
	// ErrInvalidToken is returned when the token cannot be decoded.
	ErrInvalidToken = errors.New("invalid token")
	// ErrNoOrgID is returned when the token carries no usable org id claim.
	ErrNoOrgID = errors.New("token has no orgId claim")
)

// ExtractBearer pulls the raw token from an "Authorization: Bearer <token>"
// header.
func ExtractBearer(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", ErrMissingToken
	}
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", ErrMissingToken
	}
	token := strings.TrimSpace(h[len(prefix):])
	if token == "" {
		return "", ErrMissingToken
	}
	return token, nil
}

// OrgIDFromToken decodes the token (without verifying its signature) and
// returns the organization id claim.
func OrgIDFromToken(token string) (string, error) {
	claims := jwt.MapClaims{}
	parser := jwt.NewParser()
	if _, _, err := parser.ParseUnverified(token, claims); err != nil {
		return "", ErrInvalidToken
	}
	v, ok := claims[orgIDClaim].(string)
	if !ok || v == "" {
		return "", ErrNoOrgID
	}
	return v, nil
}
