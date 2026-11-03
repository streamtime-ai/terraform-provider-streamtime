package resources

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

// orgCollection is one collection of the fake backend. create adds the fields the
// server assigns; view is what the server returns, which differs from what it stores
// (a generated key is shown once, a secret is masked).
type orgCollection struct {
	keyField  string
	single    bool
	items     map[string]map[string]any
	create    func(in map[string]any) map[string]any
	view      func(o map[string]any, created bool) map[string]any
	getStatus int
}

type orgFake struct {
	mu     sync.Mutex
	routes map[string]*orgCollection
	calls  []string
	bodies []string
}

func newOrgFake(routes map[string]*orgCollection) *orgFake {
	for _, c := range routes {
		if c.items == nil {
			c.items = map[string]map[string]any{}
		}
		if c.view == nil {
			c.view = func(o map[string]any, _ bool) map[string]any { return o }
		}
		if c.create == nil {
			c.create = func(in map[string]any) map[string]any { return in }
		}
	}
	return &orgFake{routes: routes}
}

func (f *orgFake) client(t *testing.T) *client.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *orgFake) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	f.bodies = append(f.bodies, string(raw))
	rest := strings.TrimPrefix(r.URL.Path, "/organizations/acme/")
	if rest == "members/" && r.Method == http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if rest == "members/invite/" && r.Method == http.MethodPost {
		var invitation map[string]any
		_ = json.Unmarshal(raw, &invitation)
		members := f.routes["members/"]
		member := members.create(invitation)
		members.items[str0(member["email"])] = member
		writeJSON(w, http.StatusOK, map[string]any{"message": "Invitation sent successfully", "invite_text": "Invite", "note": "Provision identity separately"})
		return
	}
	for prefix, c := range f.routes {
		if !strings.HasPrefix(rest, prefix) {
			continue
		}
		key := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/")
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		switch {
		case c.single:
			key = "_"
			if r.Method == http.MethodGet || r.Method == http.MethodPut {
				if _, ok := c.items[key]; !ok {
					c.items[key] = c.create(map[string]any{})
				}
			}
		case key == "" && r.Method == http.MethodPost:
			obj := c.create(in)
			c.items[str0(obj[c.keyField])] = obj
			writeJSON(w, http.StatusCreated, c.view(obj, true))
			return
		}
		obj, ok := c.items[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if c.getStatus != 0 {
				w.WriteHeader(c.getStatus)
				return
			}
			writeJSON(w, http.StatusOK, c.view(obj, false))
		case http.MethodPut, http.MethodPatch:
			for k, v := range in {
				obj[k] = v
			}
			writeJSON(w, http.StatusOK, c.view(obj, false))
		case http.MethodDelete:
			delete(c.items, key)
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func str0(v any) string { s, _ := v.(string); return s }

func copyMap(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

func tfList(items ...string) tftypes.Value {
	vals := make([]tftypes.Value, len(items))
	for i, s := range items {
		vals[i] = str(s)
	}
	return tftypes.NewValue(tftypes.List{ElementType: tftypes.String}, vals)
}

func tfBool(b bool) tftypes.Value { return tftypes.NewValue(tftypes.Bool, b) }
func tfInt(n int) tftypes.Value   { return tftypes.NewValue(tftypes.Number, n) }

// planFromState is the plan Terraform builds for an update: the state, with the
// configured changes on top. Computed values keep what the state holds.
func planFromState(t *testing.T, r resource.Resource, st tfsdk.State, changes map[string]tftypes.Value) tfsdk.Plan {
	t.Helper()
	vals := map[string]tftypes.Value{}
	if err := st.Raw.As(&vals); err != nil {
		t.Fatal(err)
	}
	priorValues := vals
	vals = make(map[string]tftypes.Value, len(priorValues))
	for name, value := range priorValues {
		vals[name] = value
	}
	for k, v := range changes {
		vals[k] = v
	}
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	typ := sr.Schema.Type().TerraformType(context.Background()).(tftypes.Object)
	return tfsdk.Plan{Schema: sr.Schema, Raw: tftypes.NewValue(typ, vals)}
}

// orgCase is one resource run through its whole lifecycle.
type orgCase struct {
	name    string
	new     func() resource.Resource
	routes  func() map[string]*orgCollection
	in      map[string]tftypes.Value
	id      string
	change  map[string]tftypes.Value // an in-place change; nil when the resource can only be replaced
	keyAttr string                   // the attribute an import must fill in
}

func orgCases() []orgCase {
	return []orgCase{
		{
			name: "member", new: NewMember, id: "ana@example.com", keyAttr: "email",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"members/": {keyField: "email", create: func(in map[string]any) map[string]any {
					o := copyMap(in)
					o["id"], o["organization"] = "m-1", "acme"
					if _, ok := o["role"]; !ok {
						o["role"] = nil
					}
					return o
				}}}
			},
			in:     map[string]tftypes.Value{"email": str("ana@example.com")},
			change: map[string]tftypes.Value{"role": str("Administrator")},
		},
		{
			name: "api_key", new: NewAPIKey, id: "key-1", keyAttr: "name",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"api-keys/": {keyField: "id",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["id"], o["organization"], o["key"] = "key-1", "acme", "stk_generated"
						return o
					},
					// The key is shown only in the response that creates it.
					view: func(o map[string]any, created bool) map[string]any {
						out := copyMap(o)
						if !created {
							delete(out, "key")
						}
						return out
					}}}
			},
			in:     map[string]tftypes.Value{"name": str("ci"), "role": str("Agent")},
			change: map[string]tftypes.Value{"name": str("ci-renamed")},
		},
		{
			name: "oauth_provider", new: NewOAuthProvider, id: "okta", keyAttr: "issuer",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"oauth-providers/": {keyField: "identifier",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						// The server strips a trailing slash and fills in what discovery finds.
						o["issuer"] = strings.TrimRight(str0(o["issuer"]), "/")
						o["jwks_endpoint_uri"] = o["issuer"].(string) + "/keys"
						o["token_endpoint_uri"] = o["issuer"].(string) + "/token"
						o["authorize_endpoint_uri"] = o["issuer"].(string) + "/authorize"
						o["sub_claim_name"], o["email_claim_name"], o["groups_claim_name"] = "sub", "email", "groups"
						o["scope"], o["refresh_token"] = "openid email", false
						for _, attribute := range []string{"userinfo_endpoint_uri", "audience", "groups_claim_scope", "tls_trusted_cert"} {
							if _, present := o[attribute]; !present {
								o[attribute] = nil
							}
						}
						return o
					}}}
			},
			in: map[string]tftypes.Value{"identifier": str("okta"), "issuer": str("https://idp.example.com/"),
				"client_id": str("abc"), "client_secret": str("shh"),
				"jwks_endpoint_uri":      str("https://idp.example.com/keys"),
				"token_endpoint_uri":     str("https://idp.example.com/token"),
				"authorize_endpoint_uri": str("https://idp.example.com/authorize")},
			change: map[string]tftypes.Value{"client_id": str("def"), "refresh_token": tfBool(true)},
		},
		{
			name: "oauth_client", new: NewOAuthClient, id: "ingest", keyAttr: "name",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"oauth-clients/": {keyField: "identifier",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["client_id"], o["client_secret"], o["secret_last4"] = "cid", "very-secret", "cret"
						if _, ok := o["kafka_clusters"]; !ok {
							o["kafka_clusters"] = []any{}
						}
						return o
					},
					view: func(o map[string]any, created bool) map[string]any {
						out := copyMap(o)
						if !created {
							delete(out, "client_secret")
						}
						return out
					}}}
			},
			in: map[string]tftypes.Value{"identifier": str("ingest"), "name": str("Ingest")},
		},
		{
			name: "alert_channel", new: NewAlertChannel, id: "ops", keyAttr: "type",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"alert-channels/": {keyField: "identifier"}}
			},
			in: map[string]tftypes.Value{"identifier": str("ops"), "type": str("slack"),
				"receiver_config": str(`{"channel":"#operations","api_url":"https://hooks.example.com/x"}`)},
			change: map[string]tftypes.Value{"receiver_config": str(`{"channel":"#operations","api_url":"https://hooks.example.com/y"}`)},
		},
		{
			name: "observability_provider", new: NewObservabilityProvider, id: "dd", keyAttr: "vendor",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"observability-providers/": {keyField: "identifier",
					view: func(o map[string]any, _ bool) map[string]any {
						out := copyMap(o)
						cfg := copyMap(o["configuration"].(map[string]any))
						if _, ok := cfg["api_key"]; ok {
							cfg["api_key"] = "********"
						}
						out["configuration"] = cfg
						return out
					}}}
			},
			in: map[string]tftypes.Value{"identifier": str("dd"), "vendor": str("datadog"),
				"configuration": str(`{"api_key":"secret-key","site":"datadoghq.com"}`)},
			change: map[string]tftypes.Value{"configuration": str(`{"api_key":"other-key","site":"datadoghq.eu"}`)},
		},
		{
			name: "helm_repository", new: NewHelmRepository, id: "bitnami", keyAttr: "base_url",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"helm-repositories/": {keyField: "name",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["id"], o["organization_identifier"] = "h-1", "acme"
						return o
					}}}
			},
			in:     map[string]tftypes.Value{"name": str("bitnami"), "base_url": str("https://charts.example.com")},
			change: map[string]tftypes.Value{"base_url": str("https://charts2.example.com")},
		},
		{
			name: "tracked_repository", new: NewTrackedRepository, id: "streamtime/agent", keyAttr: "repo_type",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"tracked-repositories/": {keyField: "repository",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["id"], o["helm_repository"] = "t-1", nil
						if _, ok := o["template_values"]; !ok {
							o["template_values"] = map[string]any{}
						}
						return o
					}}}
			},
			in: map[string]tftypes.Value{"repository": str("streamtime/agent"), "repo_type": str("oci"),
				"registry_url": str("docker.io")},
			change: map[string]tftypes.Value{"template_values": str(`{"replicas":2}`)},
		},
		{
			name: "package", new: NewPackage, id: "agent-1.0.0", keyAttr: "tag",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"packages/": {keyField: "identifier",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["identifier"] = "agent-1.0.0"
						o["artifact_type"], o["support_status"], o["preferred_registry"] = "helm_chart", "active", ""
						o["template_values"], o["default_values"], o["values_schema"] = map[string]any{}, nil, nil
						return o
					}}}
			},
			in:     map[string]tftypes.Value{"registry_url": str("https://charts.example.com"), "repository": str("agent"), "tag": str("1.0.0")},
			change: map[string]tftypes.Value{"support_status": str("deprecated")},
		},
		{
			name: "package_rule", new: NewPackageRule, id: "kafka-default", keyAttr: "repository",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"package-rules/": {keyField: "name",
					create: func(in map[string]any) map[string]any {
						o := copyMap(in)
						o["description"], o["registry_url"], o["version_constraint"] = "", "", ""
						o["version_list"], o["max_age_days"], o["target_status"] = []any{}, 0, "supported"
						o["unmatch_status"], o["preferred_registry"], o["allowed_registries"] = "deprecated", "", []any{}
						o["default_version"], o["priority"], o["enabled"] = "", 100, true
						return o
					}}}
			},
			in:     map[string]tftypes.Value{"name": str("kafka-default"), "repository": str("kafka")},
			change: map[string]tftypes.Value{"priority": tfInt(5), "version_list": tfList("3.7.0", "3.8.0"), "enabled": tfBool(false)},
		},
		{
			name: "smtp_config", new: NewSMTPConfig, id: "smtp-config", keyAttr: "host",
			routes: func() map[string]*orgCollection {
				return map[string]*orgCollection{"smtp-config/": {single: true,
					create: func(map[string]any) map[string]any {
						return map[string]any{"enabled": false, "host": "", "port": 587, "encryption": "starttls", "username": "",
							"from_email": "", "from_name": "", "link_base_url": "", "timeout_seconds": 30,
							"status_emails_enabled": true, "alert_emails_enabled": true, "weekly_report_enabled": true}
					},
					view: func(o map[string]any, _ bool) map[string]any {
						out := copyMap(o)
						delete(out, "password") // never shown
						out["password_set"] = o["password"] != nil
						return out
					}}}
			},
			in:     map[string]tftypes.Value{"host": str("smtp.example.com"), "enabled": tfBool(true), "password": str("pw")},
			change: map[string]tftypes.Value{"port": tfInt(465)},
		},
	}
}

func getAttr(t *testing.T, st tfsdk.State, name string) string {
	t.Helper()
	return getStr(t, st, name)
}

// TestOrgObjects_Lifecycle proves, for every organization and catalog resource, that
// create fills every computed value, a read straight after changes nothing (so the next
// plan is empty), an in-place change works where the API has one, an import finds the
// object, and deleting an object that is already gone succeeds.
func TestOrgObjects_Lifecycle(t *testing.T) {
	for _, tc := range orgCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newOrgFake(tc.routes())
			r := tc.new()
			configured(t, r, f.client(t))

			cr := resource.CreateResponse{State: emptyState(t, r)}
			r.Create(ctx, resource.CreateRequest{Plan: plan(t, r, tc.in)}, &cr)
			if cr.Diagnostics.HasError() {
				t.Fatal(cr.Diagnostics)
			}
			if !cr.State.Raw.IsFullyKnown() {
				t.Fatalf("create left unknown values: %v", cr.State.Raw)
			}
			if got := getAttr(t, cr.State, "id"); got != tc.id {
				t.Fatalf("id = %q, want %q", got, tc.id)
			}

			rr := resource.ReadResponse{State: cr.State}
			r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
			if rr.Diagnostics.HasError() {
				t.Fatal(rr.Diagnostics)
			}
			if !rr.State.Raw.Equal(cr.State.Raw) {
				t.Fatalf("a read after create changed the state, so the next plan would not be empty:\n create %v\n read   %v", cr.State.Raw, rr.State.Raw)
			}

			st := cr.State
			if tc.change != nil {
				ur := resource.UpdateResponse{State: cr.State}
				r.Update(ctx, resource.UpdateRequest{Plan: planFromState(t, r, cr.State, tc.change), State: cr.State}, &ur)
				if ur.Diagnostics.HasError() {
					t.Fatal(ur.Diagnostics)
				}
				if !ur.State.Raw.IsFullyKnown() {
					t.Fatalf("update left unknown values: %v", ur.State.Raw)
				}
				rr2 := resource.ReadResponse{State: ur.State}
				r.Read(ctx, resource.ReadRequest{State: ur.State}, &rr2)
				if rr2.Diagnostics.HasError() || !rr2.State.Raw.Equal(ur.State.Raw) {
					t.Fatalf("a read after update changed the state: %v", rr2.Diagnostics)
				}
				st = ur.State
			}

			imp := resource.ImportStateResponse{State: emptyState(t, r)}
			r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: tc.id}, &imp)
			ir := resource.ReadResponse{State: imp.State}
			r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
			if ir.Diagnostics.HasError() || ir.State.Raw.IsNull() {
				t.Fatalf("import did not find the object: %v", ir.Diagnostics)
			}
			if getAttr(t, ir.State, tc.keyAttr) == "" {
				t.Fatalf("import left %s empty", tc.keyAttr)
			}

			dr := resource.DeleteResponse{State: st}
			r.Delete(ctx, resource.DeleteRequest{State: st}, &dr)
			if dr.Diagnostics.HasError() {
				t.Fatal(dr.Diagnostics)
			}
			if tc.name == "smtp_config" {
				return // the settings always exist
			}
			dr2 := resource.DeleteResponse{State: st}
			r.Delete(ctx, resource.DeleteRequest{State: st}, &dr2)
			if dr2.Diagnostics.HasError() {
				t.Fatalf("deleting an object that is already gone failed: %v", dr2.Diagnostics)
			}
			gone := resource.ReadResponse{State: st}
			r.Read(ctx, resource.ReadRequest{State: st}, &gone)
			if gone.Diagnostics.HasError() || !gone.State.Raw.IsNull() {
				t.Fatalf("a deleted object stayed in the state: %v", gone.Diagnostics)
			}
		})
	}
}

func orgCaseByName(t *testing.T, name string) orgCase {
	t.Helper()
	for _, c := range orgCases() {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no case %q", name)
	return orgCase{}
}

func createCase(t *testing.T, name string) (*orgFake, resource.Resource, resource.CreateResponse) {
	t.Helper()
	tc := orgCaseByName(t, name)
	f := newOrgFake(tc.routes())
	r := tc.new()
	configured(t, r, f.client(t))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: plan(t, r, tc.in)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	return f, r, cr
}

// TestAPIKey_KeyIsKeptAndNeverRotated: Streamtime shows a key once, so state keeps it
// through reads and renames, and a rename must not ask the server to rotate the key.
func TestAPIKey_KeyIsKeptAndNeverRotated(t *testing.T) {
	f, r, cr := createCase(t, "api_key")
	tc := orgCaseByName(t, "api_key")
	if got := getAttr(t, cr.State, "key"); got != "stk_generated" {
		t.Fatalf("key after create = %q", got)
	}
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(context.Background(), resource.UpdateRequest{Plan: planFromState(t, r, cr.State, tc.change), State: cr.State}, &ur)
	if ur.Diagnostics.HasError() {
		t.Fatal(ur.Diagnostics)
	}
	if got := getAttr(t, ur.State, "key"); got != "stk_generated" {
		t.Fatalf("a rename lost the key: %q", got)
	}
	for _, b := range f.bodies {
		if strings.Contains(b, "rotate") {
			t.Fatalf("a request asked the server to rotate the key: %s", b)
		}
	}
}

// TestOAuthClient_SecretIsKeptAndSensitive: the secret comes once, with create.
func TestOAuthClient_SecretIsKeptAndSensitive(t *testing.T) {
	_, r, cr := createCase(t, "oauth_client")
	if got := getAttr(t, cr.State, "client_secret"); got != "very-secret" {
		t.Fatalf("client_secret after create = %q", got)
	}
	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if got := getAttr(t, rr.State, "client_secret"); got != "very-secret" {
		t.Fatalf("a read lost the client secret: %q", got)
	}
	assertSensitive(t, r, "client_secret", "key")
}

func assertSensitive(t *testing.T, r resource.Resource, names ...string) {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	for _, n := range names {
		if a, ok := sr.Schema.Attributes[n]; ok && !a.IsSensitive() {
			t.Errorf("%s must be sensitive", n)
		}
	}
}

// TestCredentialAttributes_AreSensitive: every attribute that holds a credential is
// sensitive, so plans and logs never show it.
func TestCredentialAttributes_AreSensitive(t *testing.T) {
	assertSensitive(t, NewOAuthProvider(), "client_secret")
	assertSensitive(t, NewAlertChannel(), "receiver_config")
	assertSensitive(t, NewObservabilityProvider(), "configuration")
	assertSensitive(t, NewSMTPConfig(), "password")
	assertSensitive(t, NewAPIKey(), "key")
}

// TestAlertChannel_ReceiverTypeFollowsType: the API insists receiver_config names its
// type; the provider fills it in from type so the configuration need not repeat it.
func TestAlertChannel_ReceiverTypeFollowsType(t *testing.T) {
	f, _, _ := createCase(t, "alert_channel")
	if !strings.Contains(f.bodies[0], `"receiver_type":"slack"`) {
		t.Fatalf("create did not send receiver_type: %s", f.bodies[0])
	}
}

// TestObservabilityProvider_MaskedKeyIsNotDrift: the server shows a key as ********, and
// that must not read as a change from the key in the configuration.
func TestObservabilityProvider_MaskedKeyIsNotDrift(t *testing.T) {
	_, r, cr := createCase(t, "observability_provider")
	if cfg := getAttr(t, cr.State, "configuration"); !strings.Contains(cfg, "secret-key") {
		t.Fatalf("the configured key was replaced by the mask: %s", cfg)
	}
	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if !rr.State.Raw.Equal(cr.State.Raw) {
		t.Fatal("a masked key read as drift")
	}
}

// TestOAuthProvider_ServerNormalisingIsNotDrift: a trailing slash on the issuer, which
// the server strips, must not produce a change on every plan.
func TestOAuthProvider_ServerNormalisingIsNotDrift(t *testing.T) {
	_, _, cr := createCase(t, "oauth_provider")
	if got := getAttr(t, cr.State, "issuer"); got != "https://idp.example.com/" {
		t.Fatalf("issuer = %q, want the configured value", got)
	}
}

// TestSMTPConfig_PasswordIsSentAndKept: the server never returns the password, so the
// configured one stays in state and the other settings are written over the current ones.
func TestSMTPConfig_PasswordIsSentAndKept(t *testing.T) {
	f, _, cr := createCase(t, "smtp_config")
	if got := getAttr(t, cr.State, "password"); got != "pw" {
		t.Fatalf("password = %q", got)
	}
	var put map[string]any
	for i, c := range f.calls {
		if c == "PUT /organizations/acme/smtp-config/" {
			_ = json.Unmarshal([]byte(f.bodies[i]), &put)
		}
	}
	if put["password"] != "pw" {
		t.Fatal("the write did not carry the password")
	}
	if port := f.routes["smtp-config/"].items["_"]["port"]; port != 587 {
		t.Fatalf("an omitted setting changed: port = %v", port)
	}
	if _, present := put["password_set"]; present {
		t.Fatal("a read-only password flag was sent back to the server")
	}
}

// TestSMTPConfig_DestroyLeavesTheSettings: there is one set of mail settings, so destroy
// only stops managing them.
func TestSMTPConfig_DestroyLeavesTheSettings(t *testing.T) {
	f, r, cr := createCase(t, "smtp_config")
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if dr.Diagnostics.HasError() {
		t.Fatal(dr.Diagnostics)
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c, "DELETE") {
			t.Fatalf("destroy deleted the settings: %s", c)
		}
	}
	if len(dr.Diagnostics.Warnings()) == 0 {
		t.Fatal("destroy did not say the values stay")
	}
}

// TestOAuthClient_AnyChangeReplaces: the API has no way to change a client, so every
// attribute a person sets forces a new one rather than failing on apply.
func TestOAuthClient_AnyChangeReplaces(t *testing.T) {
	var sr resource.SchemaResponse
	NewOAuthClient().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	for _, n := range []string{"identifier", "name", "kafka_clusters"} {
		if !hasReplace(sr.Schema.Attributes[n]) {
			t.Errorf("changing %s must replace the client", n)
		}
	}
}

// TestTrackedRepository_RefusesAnotherRegistrysRepository: the URL names only the
// repository, so when another registry holds one of that name a read, change or delete
// must stop instead of acting on the wrong one.
func TestTrackedRepository_RefusesAnotherRegistrysRepository(t *testing.T) {
	f, r, cr := createCase(t, "tracked_repository")
	// A different registry's repository of the same name now answers the URL.
	f.routes["tracked-repositories/"].items["streamtime/agent"]["registry_url"] = "ghcr.io"

	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if !rr.Diagnostics.HasError() {
		t.Fatal("a read accepted another registry's repository")
	}
	before := len(f.calls)
	update := resource.UpdateResponse{State: cr.State}
	r.Update(context.Background(), resource.UpdateRequest{State: cr.State, Plan: planFromState(t, r, cr.State,
		map[string]tftypes.Value{"template_values": str(`{"replicas":2}`)})}, &update)
	if !update.Diagnostics.HasError() {
		t.Fatal("an update accepted another registry's repository")
	}
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if !dr.Diagnostics.HasError() {
		t.Fatal("a delete accepted another registry's repository")
	}
	for _, c := range f.calls[before:] {
		if strings.HasPrefix(c, "DELETE") || strings.HasPrefix(c, "PATCH") {
			t.Fatalf("the other registry's repository was modified: %s", c)
		}
	}
}

func TestMember_MustInviteWithoutResettingPasswords(t *testing.T) {
	memberCase := orgCaseByName(t, "member")
	fake := newOrgFake(memberCase.routes())
	member := NewMember()
	configured(t, member, fake.client(t))
	memberCase.in["initial_password"] = str("initial-secret")
	created := resource.CreateResponse{State: emptyState(t, member)}
	member.Create(context.Background(), resource.CreateRequest{Plan: plan(t, member, memberCase.in)}, &created)
	if created.Diagnostics.HasError() {
		t.Fatal(created.Diagnostics)
	}
	updated := resource.UpdateResponse{State: created.State}
	member.Update(context.Background(), resource.UpdateRequest{State: created.State,
		Plan: planFromState(t, member, created.State, memberCase.change)}, &updated)
	if updated.Diagnostics.HasError() {
		t.Fatal(updated.Diagnostics)
	}
	for index, call := range fake.calls {
		if call != "POST /organizations/acme/members/invite/" && strings.Contains(fake.bodies[index], "initial-secret") {
			t.Fatal("the initial password was sent outside the invitation")
		}
	}
	if getAttr(t, updated.State, "initial_password") != "initial-secret" {
		t.Fatal("updating the role lost the initial password from state")
	}
	assertSensitive(t, member, "initial_password")
}

func TestTrackedRepository_CannotUpdateWithoutVerifyingIdentity(t *testing.T) {
	fake, repository, created := createCase(t, "tracked_repository")
	fake.routes["tracked-repositories/"].getStatus = http.StatusServiceUnavailable
	updated := resource.UpdateResponse{State: created.State}
	repository.Update(context.Background(), resource.UpdateRequest{State: created.State,
		Plan: planFromState(t, repository, created.State, map[string]tftypes.Value{"template_values": str(`{"replicas":2}`)})}, &updated)
	if !updated.Diagnostics.HasError() {
		t.Fatal("an update continued without verifying repository identity")
	}
	for _, call := range fake.calls {
		if strings.HasPrefix(call, "PATCH") {
			t.Fatal("an unverified repository was changed")
		}
	}
}

func TestOAuthClient_MustIgnoreClusterOrdering(t *testing.T) {
	clientCase := orgCaseByName(t, "oauth_client")
	fake := newOrgFake(clientCase.routes())
	clientCase.in["kafka_clusters"] = tfList("orders", "events")
	clientResource := NewOAuthClient()
	configured(t, clientResource, fake.client(t))
	created := resource.CreateResponse{State: emptyState(t, clientResource)}
	clientResource.Create(context.Background(), resource.CreateRequest{Plan: plan(t, clientResource, clientCase.in)}, &created)
	if created.Diagnostics.HasError() {
		t.Fatal(created.Diagnostics)
	}
	fake.routes["oauth-clients/"].items["ingest"]["kafka_clusters"] = []string{"events", "orders"}
	read := resource.ReadResponse{State: created.State}
	clientResource.Read(context.Background(), resource.ReadRequest{State: created.State}, &read)
	if read.Diagnostics.HasError() || !read.State.Raw.Equal(created.State.Raw) {
		t.Fatal("the same cluster access in a different order caused drift")
	}
}

func TestOrgObjects_MustClearRemovedNullableSettings(t *testing.T) {
	for _, testCase := range []struct {
		resource  string
		attribute string
	}{
		{resource: "oauth_provider", attribute: "audience"},
		{resource: "oauth_provider", attribute: "groups_claim_scope"},
		{resource: "oauth_provider", attribute: "tls_trusted_cert"},
		{resource: "package", attribute: "support_notes"},
	} {
		t.Run(testCase.resource+"/"+testCase.attribute, func(t *testing.T) {
			objectCase := orgCaseByName(t, testCase.resource)
			fake := newOrgFake(objectCase.routes())
			objectCase.in[testCase.attribute] = str("configured")
			object := objectCase.new()
			configured(t, object, fake.client(t))
			created := resource.CreateResponse{State: emptyState(t, object)}
			object.Create(context.Background(), resource.CreateRequest{Plan: plan(t, object, objectCase.in)}, &created)
			if created.Diagnostics.HasError() {
				t.Fatal(created.Diagnostics)
			}
			updated := resource.UpdateResponse{State: created.State}
			object.Update(context.Background(), resource.UpdateRequest{State: created.State,
				Plan: planFromState(t, object, created.State, map[string]tftypes.Value{
					testCase.attribute: tftypes.NewValue(tftypes.String, nil)})}, &updated)
			if updated.Diagnostics.HasError() {
				t.Fatal(updated.Diagnostics)
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(fake.bodies[len(fake.bodies)-1]), &body); err != nil {
				t.Fatal(err)
			}
			if value, present := body[testCase.attribute]; !present || value != nil {
				t.Fatalf("removing %s must explicitly clear it", testCase.attribute)
			}
			read := resource.ReadResponse{State: updated.State}
			object.Read(context.Background(), resource.ReadRequest{State: updated.State}, &read)
			if read.Diagnostics.HasError() || !read.State.Raw.Equal(updated.State.Raw) {
				t.Fatal("a removed setting returned on refresh")
			}
		})
	}
}

// TestTrackedRepository_HelmRepositoryResolvedByName: people name the chart repository;
// the provider looks up its ID for the request and maps it back to the name on read.
func TestTrackedRepository_HelmRepositoryResolvedByName(t *testing.T) {
	routes := map[string]*orgCollection{
		"helm-repositories/": {keyField: "name", items: map[string]map[string]any{
			"bitnami": {"id": "h-1", "name": "bitnami", "base_url": "https://charts.example.com"}}},
		"tracked-repositories/": {keyField: "repository", create: func(in map[string]any) map[string]any {
			o := copyMap(in)
			o["id"], o["template_values"], o["registry_url"] = "t-2", map[string]any{}, nil
			if o["helm_repository_id"] == "h-1" {
				o["helm_repository"] = map[string]any{"id": "h-1", "name": "bitnami"}
			}
			delete(o, "helm_repository_id")
			return o
		}},
	}
	f := newOrgFake(routes)
	r := NewTrackedRepository()
	configured(t, r, f.client(t))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: plan(t, r, map[string]tftypes.Value{
		"repository": str("kafka"), "repo_type": str("helm"), "helm_repository": str("bitnami")})}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if !strings.Contains(f.bodies[len(f.bodies)-1], `"helm_repository_id":"h-1"`) {
		t.Fatalf("create did not send the chart repository's ID: %v", f.bodies)
	}
	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || !rr.State.Raw.Equal(cr.State.Raw) {
		t.Fatalf("reading a Helm tracked repository changed the state: %v", rr.Diagnostics)
	}
	if got := getAttr(t, rr.State, "helm_repository"); got != "bitnami" {
		t.Fatalf("helm_repository = %q", got)
	}
}

// hasReplace reports whether changing the attribute plans a replacement.
func hasReplace(a rschema.Attribute) bool {
	ctx := context.Background()
	switch v := a.(type) {
	case rschema.StringAttribute:
		for _, m := range v.PlanModifiers {
			if strings.Contains(m.Description(ctx), "destroy and recreate") || strings.Contains(m.Description(ctx), "replace") {
				return true
			}
		}
	case rschema.ListAttribute:
		for _, m := range v.PlanModifiers {
			if strings.Contains(m.Description(ctx), "destroy and recreate") || strings.Contains(m.Description(ctx), "replace") {
				return true
			}
		}
	}
	return false
}
