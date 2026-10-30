package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

func newTest(t *testing.T, s config.Settings, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	s.APIURL = srv.URL
	c, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClient_PathsAlwaysOrgScoped(t *testing.T) {
	var paths []string
	c := newTest(t, config.Settings{Organization: "acme", APIKey: "k"}, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Write([]byte(`{}`))
	})
	for _, p := range []string{"secrets/", "/base-domains/x/", "../other-org/secrets/"} {
		_ = c.Do(context.Background(), "GET", p, nil, nil)
	}
	for _, p := range paths {
		if !strings.HasPrefix(p, "/organizations/acme/") {
			t.Fatalf("request escaped the org scope: %q", p)
		}
	}
}

func TestAuth_Headers(t *testing.T) {
	var got http.Header
	h := func(w http.ResponseWriter, r *http.Request) { got = r.Header; w.Write([]byte(`{}`)) }
	c := newTest(t, config.Settings{Organization: "acme", APIKey: "secret-key"}, h)
	_ = c.Do(context.Background(), "GET", "x/", nil, nil)
	if got.Get("X-Streamtime-Key") != "secret-key" || got.Get("X-Streamtime-Org") != "acme" || got.Get("Authorization") != "" {
		t.Fatalf("api key headers wrong: %v", got)
	}
	c = newTest(t, config.Settings{Organization: "acme", Token: "tok"}, h)
	_ = c.Do(context.Background(), "GET", "x/", nil, nil)
	if got.Get("Authorization") != "Bearer tok" || got.Get("X-Streamtime-Key") != "" {
		t.Fatalf("token headers wrong: %v", got)
	}
}

func TestAuth_ExpiredTokenMessage(t *testing.T) {
	c := newTest(t, config.Settings{Organization: "acme", Token: "super-secret-token"}, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
	})
	err := c.Do(context.Background(), "GET", "x/", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "streamtime login") || strings.Contains(err.Error(), "super-secret-token") {
		t.Fatalf("401 on a token must name 'streamtime login' and never echo the token: %v", err)
	}
}

func TestClient_PublicGetSendsNoCredentials(t *testing.T) {
	var got http.Header
	var path string
	c := newTest(t, config.Settings{Organization: "acme", APIKey: "k"}, func(w http.ResponseWriter, r *http.Request) {
		got, path = r.Header, r.URL.Path
		w.Write([]byte(`{"version":"1.0.0"}`))
	})
	var out struct{ Version string }
	if err := c.GetPublic(context.Background(), "setup/info/", &out); err != nil || out.Version != "" && path != "/setup/info/" {
		t.Fatal(err)
	}
	if got.Get("X-Streamtime-Key") != "" || got.Get("Authorization") != "" || path != "/setup/info/" {
		t.Fatalf("public call leaked credentials or hit the wrong path: %v %s", got, path)
	}
}

func TestClient_404IsErrNotFound(t *testing.T) {
	c := newTest(t, config.Settings{Organization: "a", APIKey: "k"}, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	if err := c.Do(context.Background(), "GET", "x/", nil, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestWaitForStatus(t *testing.T) {
	var n int32
	err := WaitForStatus(context.Background(), time.Millisecond, 2*time.Millisecond, func(context.Context) (bool, error) {
		return atomic.AddInt32(&n, 1) >= 3, nil
	})
	if err != nil || n != 3 {
		t.Fatalf("polls=%d err=%v", n, err)
	}
	boom := errors.New("failed")
	if err := WaitForStatus(context.Background(), time.Millisecond, time.Millisecond, func(context.Context) (bool, error) { return false, boom }); err != boom {
		t.Fatalf("terminal failure must surface: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := WaitForStatus(ctx, time.Millisecond, time.Millisecond, func(context.Context) (bool, error) { return false, nil }); err == nil {
		t.Fatal("must time out with the context")
	}
}
