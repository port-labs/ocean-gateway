package event

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestMarshalRequestHeadersFlattensToStringMap(t *testing.T) {
	h := http.Header{}
	h.Set("X-Event-Type", "issue_updated")
	h.Add("X-Event-Type", "ignored") // Get returns first value only

	raw, err := MarshalRequestHeaders(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var flat map[string]string
	if err := json.Unmarshal(raw, &flat); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := flat["X-Event-Type"]; got != "issue_updated" {
		t.Fatalf("X-Event-Type = %q want issue_updated", got)
	}
}
