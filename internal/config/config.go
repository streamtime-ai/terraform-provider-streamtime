// Package config resolves provider settings: provider block, then environment,
// then the Streamtime CLI context file. There is no default URL.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Block holds values set in the provider block. Empty means unset.
type Block struct {
	APIURL             string
	Organization       string
	APIKey             string
	Token              string
	CACertFile         string
	InsecureSkipVerify *bool
}

// Settings are the resolved values used to build the client.
type Settings struct {
	APIURL             string
	Organization       string
	APIKey             string
	Token              string
	CACertFile         string
	InsecureSkipVerify bool
}

type cliContext struct {
	Org        string `yaml:"org"`
	APIURL     string `yaml:"api_url"`
	AuthMethod string `yaml:"auth_method"`
	APIKey     string `yaml:"api_key"`
	Token      string `yaml:"token"`
	VerifySSL  *bool  `yaml:"verify_ssl"`
}

type cliFile struct {
	CurrentContext string                `yaml:"current_context"`
	Contexts       map[string]cliContext `yaml:"contexts"`
}

// Env reads environment variables; swapped in tests.
type Env func(string) string

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// configPath mirrors the CLI's search order (streamtime_cli/config.py): the first
// existing file among STREAMTIME_CONFIG, ./streamtime.yaml and ~/.streamtime/config.yaml.
func configPath(env Env) string {
	candidates := []string{env("STREAMTIME_CONFIG")}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, "streamtime.yaml"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".streamtime", "config.yaml"))
	}
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
			return p
		}
	}
	return ""
}

func loadContext(env Env) cliContext {
	p := configPath(env)
	if p == "" {
		return cliContext{}
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		return cliContext{}
	}
	var f cliFile
	if yaml.Unmarshal(raw, &f) != nil {
		return cliContext{}
	}
	name := f.CurrentContext
	if name == "" {
		name = "default"
	}
	return f.Contexts[name]
}

// Resolve applies block > env > CLI context and the auth precedence:
// block (api_key xor token), STREAMTIME_API_KEY, STREAMTIME_TOKEN, context auth_method.
func Resolve(b Block, env Env) (Settings, error) {
	if b.APIKey != "" && b.Token != "" {
		return Settings{}, fmt.Errorf("Set either api_key or token in the provider block, not both.")
	}
	ctx := loadContext(env)
	s := Settings{
		APIURL:       strings.TrimRight(firstNonEmpty(b.APIURL, env("STREAMTIME_API_URL"), env("STREAMTIME_URL"), ctx.APIURL), "/"),
		Organization: firstNonEmpty(b.Organization, env("STREAMTIME_ORG"), ctx.Org),
		CACertFile:   b.CACertFile,
	}
	if s.APIURL == "" {
		return Settings{}, fmt.Errorf("No Streamtime URL is configured. Set api_url in the provider block, or set STREAMTIME_API_URL.")
	}
	if s.Organization == "" {
		return Settings{}, fmt.Errorf("No organization is configured. Set organization in the provider block, or set STREAMTIME_ORG.")
	}

	switch {
	case b.APIKey != "":
		s.APIKey = b.APIKey
	case b.Token != "":
		s.Token = b.Token
	case strings.TrimSpace(env("STREAMTIME_API_KEY")) != "":
		s.APIKey = strings.TrimSpace(env("STREAMTIME_API_KEY"))
	case strings.TrimSpace(env("STREAMTIME_TOKEN")) != "":
		s.Token = strings.TrimSpace(env("STREAMTIME_TOKEN"))
	case ctx.AuthMethod == "api_key":
		s.APIKey = ctx.APIKey
	case ctx.AuthMethod == "token":
		s.Token = ctx.Token
	}
	if s.APIKey == "" && s.Token == "" {
		return Settings{}, fmt.Errorf("No credentials are configured. Set api_key or token in the provider block, " +
			"set STREAMTIME_API_KEY or STREAMTIME_TOKEN, or run 'streamtime login'.")
	}

	switch {
	case b.InsecureSkipVerify != nil:
		s.InsecureSkipVerify = *b.InsecureSkipVerify
	case env("STREAMTIME_VERIFY_SSL") != "":
		switch strings.ToLower(strings.TrimSpace(env("STREAMTIME_VERIFY_SSL"))) {
		case "0", "false", "no", "off":
			s.InsecureSkipVerify = true
		}
	case ctx.VerifySSL != nil:
		s.InsecureSkipVerify = !*ctx.VerifySSL
	}
	return s, nil
}
