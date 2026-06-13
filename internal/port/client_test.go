package port

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const okBody = `{
  "ok": true,
  "integration": {
    "_orgId": "org_ukrSy0JXDGngBGUH",
    "_id": "integration_dHNtGSwx2QQlQ6a5",
    "identifier": "jira",
    "logAttributes": { "ingestId": "pkhQJcQcUYfnTCHn" },
    "spec": { "appSpec": { "liveEventsEnabled": true, "liveEventsUuid": "BTcNKoxlO9tedphi" } }
  }
}`

func newClient(t *testing.T, status int, body string) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Assert the request shape matches the plan.
		if got := r.URL.Path; got != "/v1/integration/log123" {
			t.Errorf("path = %q", got)
		}
		if got := r.URL.Query().Get("byField"); got != "logIngestId" {
			t.Errorf("byField = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("auth = %q", got)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	return New(srv.URL, srv.Client()), srv
}

func TestGetIntegrationOK(t *testing.T) {
	c, srv := newClient(t, http.StatusOK, okBody)
	defer srv.Close()
	got, err := c.GetIntegrationByLogIngestID(context.Background(), "log123", "tok")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.LiveEventsUUID != "BTcNKoxlO9tedphi" {
		t.Errorf("liveEventsUuid = %q", got.LiveEventsUUID)
	}
	if got.OrgID != "org_ukrSy0JXDGngBGUH" {
		t.Errorf("orgId = %q", got.OrgID)
	}
}

func TestGetIntegrationNotFound(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusUnauthorized} {
		c, srv := newClient(t, status, `{}`)
		_, err := c.GetIntegrationByLogIngestID(context.Background(), "log123", "tok")
		srv.Close()
		if err != ErrIntegrationNotFound {
			t.Errorf("status %d: got %v want ErrIntegrationNotFound", status, err)
		}
	}
}

func TestGetIntegrationLiveEventsDisabled(t *testing.T) {
	body := `{"ok":true,"integration":{"_orgId":"o","spec":{"appSpec":{"liveEventsEnabled":false,"liveEventsUuid":"x"}}}}`
	c, srv := newClient(t, http.StatusOK, body)
	defer srv.Close()
	if _, err := c.GetIntegrationByLogIngestID(context.Background(), "log123", "tok"); err != ErrIntegrationNotFound {
		t.Errorf("got %v want ErrIntegrationNotFound", err)
	}
}

func TestGetIntegrationEmptyUUID(t *testing.T) {
	body := `{"ok":true,"integration":{"_orgId":"o","spec":{"appSpec":{"liveEventsEnabled":true,"liveEventsUuid":""}}}}`
	c, srv := newClient(t, http.StatusOK, body)
	defer srv.Close()
	if _, err := c.GetIntegrationByLogIngestID(context.Background(), "log123", "tok"); err != ErrIntegrationNotFound {
		t.Errorf("got %v want ErrIntegrationNotFound", err)
	}
}

func TestGetIntegrationServerError(t *testing.T) {
	c, srv := newClient(t, http.StatusInternalServerError, `boom`)
	defer srv.Close()
	if _, err := c.GetIntegrationByLogIngestID(context.Background(), "log123", "tok"); err == nil || err == ErrIntegrationNotFound {
		t.Errorf("expected generic error, got %v", err)
	}
}
