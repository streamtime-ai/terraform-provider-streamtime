package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"

	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// Settings the API takes as open JSON objects (a fleet's placement and advanced
// configuration, its workloads) are string attributes set with jsonencode().

// decodeJSON parses a JSON document, keeping numbers exact.
func decodeJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// jsonObject parses a configured JSON attribute. Null or unknown gives nil.
func jsonObject(s types.String, attr string) (map[string]any, error) {
	if s.IsNull() || s.IsUnknown() || s.ValueString() == "" {
		return nil, nil
	}
	v, err := decodeJSON([]byte(s.ValueString()))
	if err != nil {
		// The decoder's message can quote part of the value, which may be a credential.
		return nil, fmt.Errorf("%s is not valid JSON. Set it with jsonencode({...}).", attr)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a JSON object. Set it with jsonencode({...}).", attr)
	}
	return m, nil
}

// subsetOf reports whether every value the configuration sets is what the server
// holds. The server fills in defaults the configuration leaves out, so extra keys
// on the server side are not a difference. A configured key the server does not
// return is drift, unless hidden is set: then it is taken as a credential the
// server keeps to itself and cannot be compared. Lists must match element by element.
func subsetOf(configured, server any, hidden bool) bool {
	switch c := configured.(type) {
	case map[string]any:
		s, ok := server.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range c {
			sv, present := s[k]
			if !present {
				if hidden {
					continue
				}
				return false
			}
			if !subsetOf(v, sv, hidden) {
				return false
			}
		}
		return true
	case []any:
		s, ok := server.([]any)
		if !ok || len(s) != len(c) {
			return false
		}
		for i := range c {
			if !subsetOf(c[i], s[i], hidden) {
				return false
			}
		}
		return true
	case json.Number:
		s, ok := server.(json.Number)
		if !ok {
			return false
		}
		cf, err1 := c.Float64()
		sf, err2 := s.Float64()
		return err1 == nil && err2 == nil && cf == sf
	default:
		return reflect.DeepEqual(configured, server)
	}
}

// emptyJSON reports whether a server value means "not set".
func emptyJSON(v any) bool {
	if v == nil {
		return true
	}
	m, ok := v.(map[string]any)
	return ok && len(m) == 0
}

// keptOrServerJSON is the state value of a JSON attribute after a read. The
// configured text is kept while the server holds everything it sets, so a server
// default or a different key order is not a change. Anything else (drift, or an
// import with nothing configured) adopts the server's value, in canonical form.
// hidden is for settings that hold credentials the server does not return: a
// configured key missing from the server's value is then not drift.
func keptOrServerJSON(kept types.String, server json.RawMessage, hidden bool) types.String {
	sv, err := decodeJSON(server)
	if len(server) == 0 || err != nil || emptyJSON(sv) {
		if kept.IsUnknown() || kept.IsNull() {
			return types.StringNull()
		}
		if cv, err := decodeJSON([]byte(kept.ValueString())); err == nil && emptyJSON(cv) {
			return kept
		}
		return types.StringNull()
	}
	if !kept.IsNull() && !kept.IsUnknown() {
		if cv, err := decodeJSON([]byte(kept.ValueString())); err == nil && subsetOf(cv, sv, hidden) {
			return kept
		}
	}
	canonical, err := json.Marshal(sv)
	if err != nil {
		return types.StringNull()
	}
	return types.StringValue(string(canonical))
}

// listAll reads every page of an org-scoped list and returns its results.
func listAll(ctx context.Context, c *client.Client, collection string) ([]json.RawMessage, error) {
	var all []json.RawMessage
	for page := 1; ; page++ {
		var resp struct {
			Next    *string           `json:"next"`
			Results []json.RawMessage `json:"results"`
		}
		q := url.Values{"page": {fmt.Sprint(page)}, "page_size": {"100"}}
		if err := c.Do(ctx, "GET", collection+"?"+q.Encode(), nil, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Results...)
		if resp.Next == nil || *resp.Next == "" || len(resp.Results) == 0 {
			return all, nil
		}
	}
}
