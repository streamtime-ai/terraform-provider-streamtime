package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/diag"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

func serverAt(t *testing.T, ver string) *client.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/setup/info/" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"version":"` + ver + `"}`))
	}))
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestVersionCheck_Configure proves the directional rule reaches the user as the right severity.
func TestVersionCheck_Configure(t *testing.T) {
	for name, tc := range map[string]struct {
		provider, server string
		errs, warns      int
	}{
		"equal is silent":         {"1.4.0", "1.4.0", 0, 0},
		"provider newer is error": {"1.5.0", "1.4.0", 1, 0},
		"provider older warns":    {"1.3.0", "1.4.0", 0, 1},
		"nightly server warns":    {"1.4.0", "0.0.0-nightly.20261027.1", 0, 1},
		"dev provider warns":      {"dev", "1.4.0", 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			var d diag.Diagnostics
			checkServerVersion(context.Background(), serverAt(t, tc.server), tc.provider, &d)
			if d.ErrorsCount() != tc.errs || d.WarningsCount() != tc.warns {
				t.Fatalf("want %d errors %d warnings, got %v", tc.errs, tc.warns, d)
			}
		})
	}
}
