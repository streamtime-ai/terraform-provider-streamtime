package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

// fakeServer stores JSON objects by path, like a tiny REST backend.
type fakeServer struct {
	mu     sync.Mutex
	items  map[string]map[string]any
	idKey  string // field used in the item URL
	prefix string // collection path after /organizations/acme/
	calls  []string
	bodies []string
}

func newFake(prefix, idKey string) *fakeServer {
	return &fakeServer{items: map[string]map[string]any{}, idKey: idKey, prefix: prefix}
}

func (f *fakeServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		base := "/organizations/acme/" + f.prefix + "/"
		raw, _ := io.ReadAll(r.Body)
		f.bodies = append(f.bodies, string(raw))
		if r.URL.Path == base && r.Method == http.MethodPost {
			var obj map[string]any
			_ = json.Unmarshal(raw, &obj)
			if bad := f.validate(obj); bad != "" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(bad))
				return
			}
			f.finish(obj)
			f.items[f.key(obj)] = obj
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(f.public(obj))
			return
		}
		key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, base), "/")
		obj, ok := f.items[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(f.public(obj))
		case http.MethodPut:
			var upd map[string]any
			_ = json.Unmarshal(raw, &upd)
			if bad := f.validate(upd); bad != "" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(bad))
				return
			}
			for k, v := range upd {
				obj[k] = v
			}
			_ = json.NewEncoder(w).Encode(f.public(obj))
		case http.MethodDelete:
			delete(f.items, key)
			w.WriteHeader(http.StatusNoContent)
		}
	})
}

func (f *fakeServer) key(o map[string]any) string { return o[f.idKey].(string) }

// finish adds server-generated fields.
func (f *fakeServer) finish(o map[string]any) {
	if f.idKey == "id" {
		o["id"] = "11111111-2222-3333-4444-555555555555"
		o["uri"] = "secret://" + o["path"].(string)
		if _, ok := o["provider"]; !ok {
			o["provider"] = "builtin"
		}
	} else {
		o["certificate_expiry"] = "2027-01-01T00:00:00Z"
		// The backend returns parsed certificate details as an object, though the spec
		// types certificate_info as a string.
		o["certificate_info"] = nil
		if c, _ := o["tls_cert"].(string); c != "" {
			o["certificate_info"] = map[string]any{"subject": "CN=example.com", "sans": []any{"example.com"}}
		}
		if _, ok := o["auto_create_certificates"]; !ok {
			o["auto_create_certificates"] = false
		}
		if dp := dnsProviderOf(o); dp != nil && dp["provider_type"] == "None" {
			dp["provider_type"] = "none" // the backend normalises this spelling
		}
	}
}

// dnsProviderOf returns dns_provider_config.dns_provider when it is an object.
func dnsProviderOf(o map[string]any) map[string]any {
	cfg, _ := o["dns_provider_config"].(map[string]any)
	dp, _ := cfg["dns_provider"].(map[string]any)
	return dp
}

// validate applies the backend's BaseDomainSerializer rule: dns_provider_config is
// required on every create and full update, as an object naming a provider_type.
func (f *fakeServer) validate(o map[string]any) string {
	if f.idKey != "domain" {
		return ""
	}
	dp := dnsProviderOf(o)
	if dp == nil {
		return `{"dns_provider_config":["This field is required."]}`
	}
	if pt, _ := dp["provider_type"].(string); pt == "" {
		return `{"dns_provider_config":["dns_provider is required."]}`
	}
	return ""
}

var redacted = map[string]bool{"aws_access_secret": true, "api_token": true, "api_key": true, "api_secret": true}

// public is what the server returns: secret content and DNS credentials are never echoed,
// the TLS key is masked and the certificate loses its trailing newline.
func (f *fakeServer) public(o map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range o {
		switch {
		case k == "content":
			continue
		case k == "tls_key":
			out[k] = "********" // masked, as a real server would
		case k == "tls_cert":
			if c, ok := v.(string); ok {
				v = strings.TrimSpace(c) // the backend trims the trailing newline
			}
			out[k] = v
		default:
			out[k] = v
		}
	}
	if dp := dnsProviderOf(o); dp != nil {
		safe := map[string]any{}
		for k, v := range dp {
			if !redacted[k] {
				safe[k] = v
			}
		}
		out["dns_provider_config"] = map[string]any{"dns_provider": safe}
	}
	return out
}

func testClient(t *testing.T, f *fakeServer) *client.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// obj builds a tftypes object value for a resource schema from attribute values.
func plan(t *testing.T, r resource.Resource, vals map[string]tftypes.Value) tfsdk.Plan {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	typ := sr.Schema.Type().TerraformType(context.Background()).(tftypes.Object)
	full := map[string]tftypes.Value{}
	for name, at := range typ.AttributeTypes {
		if v, ok := vals[name]; ok {
			full[name] = v
		} else {
			full[name] = tftypes.NewValue(at, tftypes.UnknownValue)
		}
	}
	return tfsdk.Plan{Schema: sr.Schema, Raw: tftypes.NewValue(typ, full)}
}

func emptyState(t *testing.T, r resource.Resource) tfsdk.State {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	typ := sr.Schema.Type().TerraformType(context.Background())
	return tfsdk.State{Schema: sr.Schema, Raw: tftypes.NewValue(typ, nil)}
}

func str(s string) tftypes.Value { return tftypes.NewValue(tftypes.String, s) }

// dnsCfg builds a dns_provider_config value for the base domain schema. Fields named in set
// are given; the server-assigned ones are unknown (as in a real plan), the rest null.
func dnsCfg(t *testing.T, set map[string]string) tftypes.Value {
	t.Helper()
	var sr resource.SchemaResponse
	NewBaseDomain().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	cfgT := sr.Schema.Type().TerraformType(context.Background()).(tftypes.Object).AttributeTypes["dns_provider_config"].(tftypes.Object)
	dpT := cfgT.AttributeTypes["dns_provider"].(tftypes.Object)
	vals := map[string]tftypes.Value{}
	for name, at := range dpT.AttributeTypes {
		switch v, ok := set[name]; {
		case ok:
			vals[name] = str(v)
		case name == "zone_apex" || name == "cert_id":
			vals[name] = tftypes.NewValue(at, tftypes.UnknownValue)
		default:
			vals[name] = tftypes.NewValue(at, nil)
		}
	}
	return tftypes.NewValue(cfgT, map[string]tftypes.Value{"dns_provider": tftypes.NewValue(dpT, vals)})
}

func getDNS(t *testing.T, s tfsdk.State, field string) *string {
	t.Helper()
	var out *string
	if d := s.GetAttribute(context.Background(), path.Root("dns_provider_config").AtName("dns_provider").AtName(field), &out); d.HasError() {
		t.Fatal(d)
	}
	return out
}

var cloudflare = map[string]string{"provider_type": "cloudflare", "zone_id": "z1", "api_token": "TOKEN"}

func configured(t *testing.T, r resource.Resource, c *client.Client) {
	t.Helper()
	cr, ok := r.(resource.ResourceWithConfigure)
	if !ok {
		t.Fatal("resource cannot be configured")
	}
	var resp resource.ConfigureResponse
	cr.Configure(context.Background(), resource.ConfigureRequest{ProviderData: c}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatal(resp.Diagnostics)
	}
}

func getStr(t *testing.T, s tfsdk.State, attr string) string {
	t.Helper()
	var out string
	diags := s.GetAttribute(context.Background(), path.Root(attr), &out)
	if diags.HasError() {
		t.Fatal(diags)
	}
	return out
}

// TestSecret_Lifecycle proves create, read, update, import and delete work, and
// that the secret value stays in state although the server never returns it.
func TestSecret_Lifecycle(t *testing.T) {
	f := newFake("secrets", "id")
	r := NewSecret()
	configured(t, r, testClient(t, f))
	ctx := context.Background()

	p := plan(t, r, map[string]tftypes.Value{"path": str("apps/db"), "content": str("hunter2"), "secret_provider": str("builtin")})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if got := getStr(t, cr.State, "id"); got == "" {
		t.Fatal("create did not record the secret id")
	}
	if got := getStr(t, cr.State, "content"); got != "hunter2" {
		t.Fatalf("content lost after create: %q", got)
	}

	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "content") != "hunter2" {
		t.Fatalf("read changed the state: %v", rr.Diagnostics)
	}

	p2 := plan(t, r, map[string]tftypes.Value{"id": str(getStr(t, cr.State, "id")), "path": str("apps/db"), "content": str("new"), "secret_provider": str("builtin"), "uri": str(getStr(t, cr.State, "uri"))})
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || getStr(t, ur.State, "content") != "new" {
		t.Fatalf("update failed: %v", ur.Diagnostics)
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: getStr(t, cr.State, "id")}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "path") != "apps/db" {
		t.Fatalf("import did not find the secret: %v", ir.Diagnostics)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.items) != 0 {
		t.Fatalf("delete failed: %v", dr.Diagnostics)
	}
}

// TestDelete_404IsSuccess: a secret removed outside Terraform must not fail destroy.
func TestDelete_404IsSuccess(t *testing.T) {
	f := newFake("secrets", "id")
	r := NewSecret()
	configured(t, r, testClient(t, f))
	st := plan(t, r, map[string]tftypes.Value{"id": str("gone"), "path": str("x"), "content": str("y"), "secret_provider": str("builtin"), "uri": str("u")})
	dr := resource.DeleteResponse{}
	r.Delete(context.Background(), resource.DeleteRequest{State: tfsdk.State{Schema: st.Schema, Raw: st.Raw}}, &dr)
	if dr.Diagnostics.HasError() {
		t.Fatalf("delete of a missing secret failed: %v", dr.Diagnostics)
	}
}

// TestRead_404DropsState: a secret deleted outside Terraform is planned for re-creation.
func TestRead_404DropsState(t *testing.T) {
	f := newFake("secrets", "id")
	r := NewSecret()
	configured(t, r, testClient(t, f))
	p := plan(t, r, map[string]tftypes.Value{"id": str("gone"), "path": str("x"), "content": str("y"), "secret_provider": str("builtin"), "uri": str("u")})
	rr := resource.ReadResponse{State: tfsdk.State{Schema: p.Schema, Raw: p.Raw}}
	r.Read(context.Background(), resource.ReadRequest{State: rr.State}, &rr)
	if rr.Diagnostics.HasError() || !rr.State.Raw.IsNull() {
		t.Fatalf("state not removed for a missing secret: %v", rr.Diagnostics)
	}
}

// TestSensitive_NotLogged: secret value, TLS key and DNS settings are marked sensitive,
// so Terraform never prints them in plans or logs.
func TestSensitive_NotLogged(t *testing.T) {
	for name, tc := range map[string]struct {
		r    resource.Resource
		attr []string
	}{
		"secret":      {NewSecret(), []string{"content"}},
		"base_domain": {NewBaseDomain(), []string{"tls_key", "dns_provider_config"}},
	} {
		var sr resource.SchemaResponse
		tc.r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
		for _, a := range tc.attr {
			at, ok := sr.Schema.Attributes[a]
			if !ok || !at.IsSensitive() {
				t.Errorf("%s.%s must be sensitive", name, a)
			}
		}
	}
}

// TestReplaceAttributes: changing an identity attribute must create a new object, not edit one.
func TestReplaceAttributes(t *testing.T) {
	for name, tc := range map[string]struct {
		r     resource.Resource
		attrs []string
	}{
		"secret":      {NewSecret(), []string{"path", "secret_provider"}},
		"base_domain": {NewBaseDomain(), []string{"domain"}},
	} {
		var sr resource.SchemaResponse
		tc.r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
		for _, a := range tc.attrs {
			sa := sr.Schema.Attributes[a].(rschema.StringAttribute)
			replaces := false
			live := tfsdk.State{Raw: tftypes.NewValue(tftypes.Object{AttributeTypes: map[string]tftypes.Type{}}, map[string]tftypes.Value{})}
			for _, m := range sa.PlanModifiers {
				resp := planmodifier.StringResponse{}
				m.PlanModifyString(context.Background(), planmodifier.StringRequest{
					State: live, Plan: tfsdk.Plan{Raw: live.Raw},
					StateValue: types.StringValue("old"), PlanValue: types.StringValue("new"), ConfigValue: types.StringValue("new"),
				}, &resp)
				replaces = replaces || resp.RequiresReplace
			}
			if !replaces {
				t.Errorf("%s.%s must force replacement", name, a)
			}
		}
	}
}

// TestBaseDomain_Lifecycle proves create, read, update, import and delete, and that
// the TLS key stays in state although the server never returns it.
func TestBaseDomain_Lifecycle(t *testing.T) {
	f := newFake("base-domains", "domain")
	r := NewBaseDomain()
	configured(t, r, testClient(t, f))
	ctx := context.Background()

	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "tls_key": str("KEY"), "dns_provider_config": dnsCfg(t, cloudflare)})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if getStr(t, cr.State, "id") != "example.com" || getStr(t, cr.State, "tls_key") != "KEY" {
		t.Fatal("create did not record id or keep the key")
	}

	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "tls_key") != "KEY" {
		t.Fatalf("read dropped the key: %v", rr.Diagnostics)
	}
	if tok := getDNS(t, rr.State, "api_token"); tok == nil || *tok != "TOKEN" {
		t.Fatalf("the DNS credential was lost although the server never returns it: %v", tok)
	}

	p2 := plan(t, r, map[string]tftypes.Value{"id": str("example.com"), "domain": str("example.com"), "tls_key": str("KEY2"),
		"auto_create_certificates": tftypes.NewValue(tftypes.Bool, true), "dns_provider_config": dnsCfg(t, cloudflare), "certificate_expiry": str("2027-01-01T00:00:00Z")})
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || getStr(t, ur.State, "tls_key") != "KEY2" {
		t.Fatalf("update failed: %v", ur.Diagnostics)
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "example.com"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "domain") != "example.com" {
		t.Fatalf("import did not find the domain: %v", ir.Diagnostics)
	}
	if pt := getDNS(t, ir.State, "provider_type"); pt == nil || *pt != "cloudflare" {
		t.Fatalf("import did not adopt the DNS provider: %v", pt)
	}
	if tok := getDNS(t, ir.State, "api_token"); tok != nil {
		t.Fatalf("import invented a credential the server never returned: %q", *tok)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.items) != 0 {
		t.Fatalf("delete failed: %v", dr.Diagnostics)
	}
}

// TestBaseDomain_AutomaticCertStaysOutOfState: when the configuration sets no tls_cert,
// a certificate the server reports must not enter state (Terraform would fail the apply).
func TestBaseDomain_AutomaticCertStaysOutOfState(t *testing.T) {
	f := newFake("base-domains", "domain")
	srv := f.handler()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, r)
			var obj map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &obj)
			obj["tls_cert"] = "AUTO-CERT"
			w.WriteHeader(rec.Code)
			_ = json.NewEncoder(w).Encode(obj)
			return
		}
		srv.ServeHTTP(w, r)
	})
	ts := httptest.NewServer(wrapped)
	t.Cleanup(ts.Close)
	c, _ := client.New(config.Settings{APIURL: ts.URL, Organization: "acme", APIKey: "k"})
	r := NewBaseDomain()
	configured(t, r, c)

	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "tls_cert": tftypes.NewValue(tftypes.String, nil), "tls_key": tftypes.NewValue(tftypes.String, nil), "dns_provider_config": dnsCfg(t, map[string]string{"provider_type": "none"})})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	var cert *string
	if d := cr.State.GetAttribute(context.Background(), path.Root("tls_cert"), &cert); d.HasError() || cert != nil {
		t.Fatalf("tls_cert entered state: %v %v", cert, d)
	}
}

// TestBaseDomain_SendsDNSConfigAsObject: the backend requires dns_provider_config as an
// object naming a provider_type, and rejects other fields; the request must match that
// shape without empty placeholders for unset fields.
func TestBaseDomain_SendsDNSConfigAsObject(t *testing.T) {
	f := newFake("base-domains", "domain")
	r := NewBaseDomain()
	configured(t, r, testClient(t, f))
	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "dns_provider_config": dnsCfg(t, cloudflare)})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	var sent struct {
		Cfg struct {
			DP map[string]any `json:"dns_provider"`
		} `json:"dns_provider_config"`
	}
	if err := json.Unmarshal([]byte(f.bodies[0]), &sent); err != nil {
		t.Fatalf("dns_provider_config was not sent as an object: %v: %s", err, f.bodies[0])
	}
	want := map[string]any{"provider_type": "cloudflare", "zone_id": "z1", "api_token": "TOKEN"}
	if len(sent.Cfg.DP) != len(want) {
		t.Fatalf("unset fields were sent: %v", sent.Cfg.DP)
	}
	for k, v := range want {
		if sent.Cfg.DP[k] != v {
			t.Fatalf("%s = %v, want %v", k, sent.Cfg.DP[k], v)
		}
	}
}

// TestBaseDomain_NoneSpellingIsNotDrift: the backend stores "None" as "none"; returning
// that must not make Terraform fail the apply with an inconsistent result.
func TestBaseDomain_NoneSpellingIsNotDrift(t *testing.T) {
	f := newFake("base-domains", "domain")
	r := NewBaseDomain()
	configured(t, r, testClient(t, f))
	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "dns_provider_config": dnsCfg(t, map[string]string{"provider_type": "None"})})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if pt := getDNS(t, cr.State, "provider_type"); pt == nil || *pt != "None" {
		t.Fatalf("configured spelling was replaced: %v", pt)
	}
}

// TestBaseDomain_ServerNormalisedCertIsNotDrift: the backend trims the certificate's trailing
// newline and masks the key. Returning those must not make Terraform fail the apply with an
// inconsistent result, which is what happened against a real backend.
func TestBaseDomain_ServerNormalisedCertIsNotDrift(t *testing.T) {
	f := newFake("base-domains", "domain")
	r := NewBaseDomain()
	configured(t, r, testClient(t, f))
	cert := "-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----\n"
	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "tls_cert": str(cert), "tls_key": str("PRIVATE"),
		"dns_provider_config": dnsCfg(t, map[string]string{"provider_type": "none"})})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if getStr(t, cr.State, "tls_cert") != cert || getStr(t, cr.State, "tls_key") != "PRIVATE" {
		t.Fatal("configured certificate or key was replaced by the server's normalised view")
	}
	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "tls_cert") != cert {
		t.Fatalf("read changed the certificate: %v", rr.Diagnostics)
	}
}

// TestRequests_LeaveUnsetNullableFieldsOut — must not (risk: data loss). The generated API
// types send null for an unset nullable field, and the API stores null: a base domain PUT
// would erase its automatic certificate, a secret PUT its vault metadata. Create and update
// build the same body, so the create request shows what an update sends.
func TestRequests_LeaveUnsetNullableFieldsOut(t *testing.T) {
	null := tftypes.NewValue(tftypes.String, nil)
	cases := []struct {
		name   string
		r      resource.Resource
		f      *fakeServer
		vals   map[string]tftypes.Value
		absent []string
	}{
		{"base domain", NewBaseDomain(), newFake("base-domains", "domain"),
			map[string]tftypes.Value{"domain": str("example.com"), "tls_cert": null, "tls_key": null,
				"dns_provider_config": dnsCfg(t, map[string]string{"provider_type": "none"})},
			[]string{"tls_cert", "tls_key"}},
		{"secret", NewSecret(), newFake("secrets", "id"),
			map[string]tftypes.Value{"path": str("apps/db"), "content": str("hunter2"), "secret_provider": str("vault")},
			[]string{"provider_data"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configured(t, tc.r, testClient(t, tc.f))
			cr := resource.CreateResponse{State: emptyState(t, tc.r)}
			tc.r.Create(context.Background(), resource.CreateRequest{Plan: plan(t, tc.r, tc.vals)}, &cr)
			if cr.Diagnostics.HasError() {
				t.Fatal(cr.Diagnostics)
			}
			var sent map[string]any
			if err := json.Unmarshal([]byte(tc.f.bodies[0]), &sent); err != nil {
				t.Fatal(err)
			}
			for _, k := range tc.absent {
				if _, ok := sent[k]; ok {
					t.Fatalf("%s was sent although it is not set: %s", k, tc.f.bodies[0])
				}
			}
		})
	}
}

// TestBaseDomain_SettingOfAnotherProviderIsAnError — must not (risk: a credential silently
// ignored). Each provider's request type has only its own settings, so a setting meant for
// another provider would be dropped; the API rejects it, and so does the provider.
func TestBaseDomain_SettingOfAnotherProviderIsAnError(t *testing.T) {
	f := newFake("base-domains", "domain")
	r := NewBaseDomain()
	configured(t, r, testClient(t, f))
	set := map[string]string{"provider_type": "route53", "aws_access_key": "AK", "aws_access_secret": "S",
		"aws_region": "eu-west-1", "zone_id": "z1"}
	p := plan(t, r, map[string]tftypes.Value{"domain": str("example.com"), "dns_provider_config": dnsCfg(t, set)})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if !cr.Diagnostics.HasError() || !strings.Contains(fmt.Sprint(cr.Diagnostics), "zone_id") {
		t.Fatalf("want an error naming zone_id, got %v", cr.Diagnostics)
	}
	if len(f.bodies) != 0 {
		t.Fatalf("the request was sent: %v", f.bodies)
	}
}
