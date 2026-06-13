package auth

import (
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func makeToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func TestOrgIDFromToken(t *testing.T) {
	tok := makeToken(t, jwt.MapClaims{"orgId": "org_abc", "sub": "user"})
	got, err := OrgIDFromToken(tok)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "org_abc" {
		t.Fatalf("got %q want org_abc", got)
	}
}

func TestOrgIDFromTokenMissingClaim(t *testing.T) {
	tok := makeToken(t, jwt.MapClaims{"sub": "user"})
	if _, err := OrgIDFromToken(tok); err != ErrNoOrgID {
		t.Fatalf("got %v want ErrNoOrgID", err)
	}
}

func TestOrgIDFromTokenMalformed(t *testing.T) {
	if _, err := OrgIDFromToken("not-a-jwt"); err != ErrInvalidToken {
		t.Fatalf("got %v want ErrInvalidToken", err)
	}
}

func TestExtractBearer(t *testing.T) {
	cases := []struct {
		hdr     string
		want    string
		wantErr bool
	}{
		{"Bearer abc.def.ghi", "abc.def.ghi", false},
		{"bearer lower", "lower", false},
		{"abc.def.ghi", "", true},
		{"", "", true},
		{"Bearer ", "", true},
	}
	for _, tc := range cases {
		r := httptest.NewRequest("POST", "/", nil)
		if tc.hdr != "" {
			r.Header.Set("Authorization", tc.hdr)
		}
		got, err := ExtractBearer(r)
		if tc.wantErr {
			if err == nil {
				t.Errorf("hdr %q: expected error", tc.hdr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("hdr %q: got %q, %v want %q", tc.hdr, got, err, tc.want)
		}
	}
}
