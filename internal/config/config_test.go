package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func envOf(m map[string]string) Env { return func(k string) string { return m[k] } }

func writeCtx(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const ctxYAML = `current_context: default
contexts:
  default:
    org: ctxorg
    api_url: https://ctx.example
    auth_method: token
    token: ctx-token
    api_key: ctx-key
`

func TestConfig_Precedence(t *testing.T) {
	cfg := writeCtx(t, ctxYAML)
	env := envOf(map[string]string{"STREAMTIME_CONFIG": cfg, "STREAMTIME_API_URL": "https://env.example", "STREAMTIME_ORG": "envorg", "STREAMTIME_API_KEY": "k"})
	s, err := Resolve(Block{Organization: "blockorg"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if s.Organization != "blockorg" || s.APIURL != "https://env.example" || s.APIKey != "k" {
		t.Fatalf("block > env > context not honoured: %+v", s)
	}
	s, err = Resolve(Block{}, envOf(map[string]string{"STREAMTIME_CONFIG": cfg}))
	if err != nil || s.Organization != "ctxorg" || s.APIURL != "https://ctx.example" {
		t.Fatalf("context fallback failed: %+v %v", s, err)
	}
}

func TestAuth_Precedence(t *testing.T) {
	cfg := writeCtx(t, ctxYAML)
	base := map[string]string{"STREAMTIME_CONFIG": cfg}
	with := func(extra map[string]string) Env {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return envOf(m)
	}
	s, _ := Resolve(Block{Token: "block-token"}, with(map[string]string{"STREAMTIME_API_KEY": "env-key"}))
	if s.Token != "block-token" || s.APIKey != "" {
		t.Fatalf("block must win: %+v", s)
	}
	s, _ = Resolve(Block{}, with(map[string]string{"STREAMTIME_API_KEY": "env-key", "STREAMTIME_TOKEN": "env-token"}))
	if s.APIKey != "env-key" || s.Token != "" {
		t.Fatalf("env key must beat env token: %+v", s)
	}
	s, _ = Resolve(Block{}, with(map[string]string{"STREAMTIME_TOKEN": "env-token"}))
	if s.Token != "env-token" {
		t.Fatalf("env token must beat context: %+v", s)
	}
	s, _ = Resolve(Block{}, with(nil))
	if s.Token != "ctx-token" || s.APIKey != "" {
		t.Fatalf("context auth_method must pick the token: %+v", s)
	}
}

func TestAuth_BothInBlockFails(t *testing.T) {
	_, err := Resolve(Block{APIKey: "a", Token: "b", APIURL: "https://x", Organization: "o"}, envOf(nil))
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("expected both-set error, got %v", err)
	}
}

func TestConfig_NoURLFails(t *testing.T) {
	_, err := Resolve(Block{Organization: "o", APIKey: "k"}, envOf(map[string]string{"STREAMTIME_CONFIG": filepath.Join(t.TempDir(), "none.yaml")}))
	if err == nil || !strings.Contains(err.Error(), "STREAMTIME_API_URL") {
		t.Fatalf("a missing URL must name what to set, got %v", err)
	}
}

func TestConfig_MissingOrgAndCredentialsNameTheSetting(t *testing.T) {
	none := map[string]string{"STREAMTIME_CONFIG": filepath.Join(t.TempDir(), "none.yaml")}
	if _, err := Resolve(Block{APIURL: "https://x", APIKey: "k"}, envOf(none)); err == nil || !strings.Contains(err.Error(), "STREAMTIME_ORG") {
		t.Fatalf("got %v", err)
	}
	if _, err := Resolve(Block{APIURL: "https://x", Organization: "o"}, envOf(none)); err == nil || !strings.Contains(err.Error(), "streamtime login") {
		t.Fatalf("got %v", err)
	}
}

// TestConfig_ProjectLocalContext: Terraform and the CLI must pick the same login in one
// directory, so ./streamtime.yaml is read before the home file but after STREAMTIME_CONFIG.
func TestConfig_ProjectLocalContext(t *testing.T) {
	dir := t.TempDir()
	local := "current_context: default\ncontexts:\n  default:\n    org: localorg\n    api_url: https://local.example\n    auth_method: api_key\n    api_key: lk\n"
	if err := os.WriteFile(filepath.Join(dir, "streamtime.yaml"), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	t.Setenv("HOME", t.TempDir())

	s, err := Resolve(Block{}, envOf(nil))
	if err != nil || s.Organization != "localorg" || s.APIKey != "lk" {
		t.Fatalf("./streamtime.yaml not used: %+v %v", s, err)
	}
	s, err = Resolve(Block{}, envOf(map[string]string{"STREAMTIME_CONFIG": writeCtx(t, ctxYAML)}))
	if err != nil || s.Organization != "ctxorg" {
		t.Fatalf("STREAMTIME_CONFIG must win over ./streamtime.yaml: %+v %v", s, err)
	}
}
