// Package port is a minimal client for the Port API, used to resolve a
// logIngestId to its integration's liveEventsUuid.
package port

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// ErrIntegrationNotFound means the integration could not be resolved or is not
// eligible for live events. It maps to a 404 at the HTTP boundary. It covers
// both Port 404/401 responses and integrations with live events disabled or no
// liveEventsUuid.
var ErrIntegrationNotFound = errors.New("integration not found")

// Client calls the Port API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a Client targeting baseURL (e.g. https://api.getport.io) using
// the provided HTTP client (which should carry a timeout).
func New(baseURL string, httpClient *http.Client) *Client {
	return &Client{baseURL: baseURL, http: httpClient}
}

// integrationResponse mirrors the relevant subset of GET /v1/integration.
type integrationResponse struct {
	Integration struct {
		OrgID string `json:"_orgId"`
		Spec  struct {
			AppSpec struct {
				LiveEventsEnabled bool   `json:"liveEventsEnabled"`
				LiveEventsUUID    string `json:"liveEventsUuid"`
			} `json:"appSpec"`
		} `json:"spec"`
	} `json:"integration"`
}

// Integration is the resolved identity returned to callers.
type Integration struct {
	OrgID          string
	LiveEventsUUID string
}

// GetIntegrationByLogIngestID resolves logIngestID to an integration via
// GET /v1/integration/{logIngestID}?byField=logIngestId, authenticated with the
// caller's bearer token. The token both authenticates and validates ownership.
func (c *Client) GetIntegrationByLogIngestID(ctx context.Context, logIngestID, token string) (Integration, error) {
	u := fmt.Sprintf("%s/v1/integration/%s?byField=logIngestId", c.baseURL, url.PathEscape(logIngestID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Integration{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return Integration{}, fmt.Errorf("port request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// handled below
	case http.StatusNotFound, http.StatusUnauthorized:
		return Integration{}, ErrIntegrationNotFound
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return Integration{}, fmt.Errorf("port api status %d: %s", resp.StatusCode, body)
	}

	var ir integrationResponse
	if err := json.NewDecoder(resp.Body).Decode(&ir); err != nil {
		return Integration{}, fmt.Errorf("decode port response: %w", err)
	}

	app := ir.Integration.Spec.AppSpec
	if !app.LiveEventsEnabled || app.LiveEventsUUID == "" {
		return Integration{}, ErrIntegrationNotFound
	}
	return Integration{
		OrgID:          ir.Integration.OrgID,
		LiveEventsUUID: app.LiveEventsUUID,
	}, nil
}
