// Package client is the org-scoped HTTP layer: auth headers, TLS trust,
// plain-language errors and status polling.
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

// ErrNotFound is returned for a 404, so callers can drop state or treat a delete as done.
var ErrNotFound = errors.New("not found")

// Client sends requests for one organization.
type Client struct {
	baseURL string
	org     string
	key     string
	token   string
	http    *http.Client
}

// New builds a client from resolved settings.
func New(s config.Settings) (*Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: s.InsecureSkipVerify} //nolint:gosec // explicit opt-in
	if s.CACertFile != "" {
		pem, err := os.ReadFile(s.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("Could not read the CA certificate file %q: %w", s.CACertFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("The CA certificate file %q holds no valid PEM certificate.", s.CACertFile)
		}
		tlsCfg.RootCAs = pool
	}
	return &Client{
		baseURL: s.APIURL, org: s.Organization, key: s.APIKey, token: s.Token,
		http: &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsCfg, Proxy: http.ProxyFromEnvironment}},
	}, nil
}

// Org returns the organization this client is scoped to.
func (c *Client) Org() string { return c.org }

// OrgPath builds an org-scoped path. It is the only way resources build paths.
func (c *Client) OrgPath(path string) string {
	return "/organizations/" + url.PathEscape(c.org) + "/" + strings.TrimLeft(path, "/")
}

func (c *Client) authenticate(req *http.Request) {
	if c.key != "" {
		req.Header.Set("X-Streamtime-Key", c.key)
		req.Header.Set("X-Streamtime-Org", c.org)
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
}

// Do sends an authenticated request to an org-scoped path and decodes a JSON
// response into out (which may be nil).
func (c *Client) Do(ctx context.Context, method, orgPath string, body, out any) error {
	return c.do(ctx, method, c.OrgPath(orgPath), body, out, true)
}

// GetPublic reads an unauthenticated global path. Only /setup/info/ uses it.
func (c *Client) GetPublic(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, "/"+strings.TrimLeft(path, "/"), nil, out, false)
}

func (c *Client) do(ctx context.Context, method, path string, body, out any, auth bool) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		c.authenticate(req)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Could not reach Streamtime at %s: %w", c.baseURL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 400 {
		return c.mapError(resp.StatusCode, raw, auth)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("Streamtime returned a response that could not be read: %w", err)
		}
	}
	return nil
}

func (c *Client) mapError(status int, raw []byte, auth bool) error {
	switch status {
	case http.StatusUnauthorized:
		if auth && c.token != "" {
			return errors.New("Your login has expired. Run 'streamtime login' again, or use an API key (api_key / STREAMTIME_API_KEY) for CI and long runs.")
		}
		return errors.New("Authentication failed. Check that the API key is valid and belongs to this organization.")
	case http.StatusForbidden:
		return fmt.Errorf("You do not have permission for this operation: %s", detail(raw))
	case http.StatusNotFound:
		return ErrNotFound
	}
	return fmt.Errorf("Streamtime rejected the request (%d): %s", status, detail(raw))
}

func detail(raw []byte) string {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return strings.TrimSpace(string(raw))
	}
	if d, ok := m["detail"].(string); ok {
		return d
	}
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s: %v", k, v))
	}
	return strings.Join(parts, "; ")
}

// WaitForStatus polls get until done reports true, with exponential backoff,
// until ctx (carrying the Terraform timeout) ends. done returns a non-nil error
// for a terminal failure.
func WaitForStatus(ctx context.Context, initial, max time.Duration, get func(context.Context) (done bool, err error)) error {
	delay := initial
	for {
		done, err := get(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Timed out waiting for the operation to finish: %w", ctx.Err())
		case <-time.After(delay):
		}
		if delay *= 2; delay > max {
			delay = max
		}
	}
}
