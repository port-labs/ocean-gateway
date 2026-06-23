package event

import (
	"encoding/json"
	"net/http"
)

// MarshalRequestHeaders JSON-encodes HTTP headers as a flat map[string]string
// (one value per key via Header.Get). This is the on-the-wire headers shape
// stored in Redis stream entries.
func MarshalRequestHeaders(h http.Header) ([]byte, error) {
	flat := make(map[string]string, len(h))
	for k := range h {
		flat[k] = h.Get(k)
	}
	return json.Marshal(flat)
}
