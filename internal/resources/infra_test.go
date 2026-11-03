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
	"time"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
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

func init() {
	// No real waiting in unit tests.
	fleetPoll.initial, fleetPoll.max = time.Millisecond, time.Millisecond
	networkPoll.initial, networkPoll.max = time.Millisecond, time.Millisecond
}

// infraFake answers the cloud account, network and fleet endpoints the way the
// backend does: a fleet is saved by POST, provisioned only after reconcile, and
// removed some time after DELETE. Anything outside /organizations/acme/ is a 404.
type infraFake struct {
	mu            sync.Mutex
	accounts      map[string]map[string]any
	networks      map[string]map[string]any
	fleets        map[string]map[string]any
	pollsToReady  int    // GETs before a reconciled fleet or a provisioned network is ready
	failReconcile bool   // the provisioning workflow reports Error
	reconcileKey  string // Idempotency-Key of the last reconcile
	reconcileGen  float64
	calls         []string
	gets          int
	webhookSecret string // the fleet's stored secret; PUT clears it on an explicit null
}

func newInfraFake() *infraFake {
	return &infraFake{accounts: map[string]map[string]any{}, networks: map[string]map[string]any{},
		fleets: map[string]map[string]any{}, pollsToReady: 2}
}

func (f *infraFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		rest, ok := strings.CutPrefix(r.URL.Path, "/organizations/acme/")
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		reply := func(code int, v any) {
			w.WriteHeader(code)
			if v != nil {
				_ = json.NewEncoder(w).Encode(v)
			}
		}
		switch {
		case parts[0] == "bootstrap-providers":
			f.crud(w, r, parts, in, f.accounts, nil)
		case parts[0] == "networks" && len(parts) == 1 && r.Method == http.MethodGet:
			var results []any
			for _, n := range f.networks {
				results = append(results, n)
			}
			reply(200, map[string]any{"count": len(results), "next": nil, "results": results})
		case parts[0] == "networks":
			f.crud(w, r, parts, in, f.networks, func(n map[string]any) {
				n["id"] = "22222222-2222-4222-8222-222222222222"
				if _, ok := n["network_id"]; !ok {
					n["network_id"] = nil
					n["_polls"] = 0
				}
			})
		case parts[0] == "fleet" && len(parts) == 1 && r.Method == http.MethodPost:
			in["generation"], in["observed_generation"] = 1, 0
			in["status"], in["reconciliation_state"], in["configuration_state"] = "Pending", "pending", "valid"
			in["desired_lifecycle"] = "present"
			addDefaults(in)
			if _, ok := in["tenancy_mode"]; !ok {
				in["tenancy_mode"] = "Shared"
			}
			cfg, _ := in["config"].(map[string]any)
			if cfg == nil {
				cfg = map[string]any{}
			}
			in["config"] = cfg
			delete(in, "alert_webhook_secret") // write-only
			f.fleets[in["identifier"].(string)] = in
			reply(201, in)
		case parts[0] == "fleet" && len(parts) == 3 && parts[2] == "reconcile":
			fl := f.fleets[parts[1]]
			f.reconcileKey = r.Header.Get("Idempotency-Key")
			f.reconcileGen, _ = in["generation"].(float64)
			fl["reconciliation_state"], fl["status"], fl["_polls"] = "queued", "Provisioning", 0
			reply(202, map[string]any{"status": "queued"})
		case parts[0] == "fleet" && len(parts) == 3 && parts[2] == "node-pools":
			if _, ok := f.fleets[parts[1]]; !ok {
				reply(404, nil)
				return
			}
			next := any(nil)
			page := r.URL.Query().Get("page")
			results := []any{map[string]any{"size": 3, "metadata": map[string]any{"name": "pool-a"}}}
			if page == "1" {
				next = "page2"
			}
			reply(200, map[string]any{"count": 2, "next": next, "results": results})
		case parts[0] == "fleet" && len(parts) == 2:
			fl, ok := f.fleets[parts[1]]
			if !ok {
				reply(404, nil)
				return
			}
			switch r.Method {
			case http.MethodGet:
				f.gets++
				f.advance(parts[1], fl)
				if _, still := f.fleets[parts[1]]; !still {
					reply(404, nil)
					return
				}
				reply(200, fl)
			case http.MethodPut:
				before, _ := json.Marshal([]any{fl["placement_config"], fl["advanced_config"]})
				addDefaults(in)
				if v, sent := in["alert_webhook_secret"]; sent {
					f.webhookSecret, _ = v.(string) // nil clears it, as the nullable field does
				}
				for k, v := range in {
					if k != "alert_webhook_secret" {
						fl[k] = v
					}
				}
				after, _ := json.Marshal([]any{fl["placement_config"], fl["advanced_config"]})
				if string(before) != string(after) {
					fl["generation"] = fl["generation"].(int) + 1
					fl["reconciliation_state"] = "pending"
				}
				reply(200, fl)
			case http.MethodDelete:
				fl["desired_lifecycle"], fl["status"], fl["reconciliation_state"], fl["_polls"] = "absent", "Deleting", "queued", 0
				reply(204, nil)
			}
		default:
			reply(404, nil)
		}
	})
}

// advance moves a fleet along after enough polls, as the workflows would.
func (f *infraFake) advance(id string, fl map[string]any) {
	polls, _ := fl["_polls"].(int)
	fl["_polls"] = polls + 1
	if polls+1 < f.pollsToReady {
		return
	}
	switch {
	case fl["desired_lifecycle"] == "absent":
		delete(f.fleets, id)
	case fl["reconciliation_state"] == "queued" && f.failReconcile:
		fl["reconciliation_state"], fl["status"] = "failed", "Error"
	case fl["reconciliation_state"] == "queued":
		fl["reconciliation_state"], fl["status"], fl["observed_generation"] = "succeeded", "Healthy", fl["generation"]
	}
}

// crud is a plain REST collection keyed by identifier.
func (f *infraFake) crud(w http.ResponseWriter, r *http.Request, parts []string, in map[string]any, items map[string]map[string]any, finish func(map[string]any)) {
	reply := func(code int, v any) {
		w.WriteHeader(code)
		if v != nil {
			_ = json.NewEncoder(w).Encode(v)
		}
	}
	if len(parts) == 1 && r.Method == http.MethodPost {
		if finish != nil {
			finish(in)
		}
		items[in["identifier"].(string)] = in
		reply(201, in)
		return
	}
	obj, ok := items[parts[len(parts)-1]]
	if !ok {
		reply(404, nil)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if p, tracked := obj["_polls"].(int); tracked {
			obj["_polls"] = p + 1
			if p+1 >= f.pollsToReady {
				obj["network_id"] = "vpc-123"
				delete(obj, "_polls")
			}
		}
		reply(200, obj)
	case http.MethodPut:
		for k, v := range in {
			obj[k] = v
		}
		reply(200, obj)
	case http.MethodDelete:
		delete(items, parts[len(parts)-1])
		reply(204, nil)
	}
}

func infraClient(t *testing.T, f *infraFake) *client.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// planNull is plan() with the named attributes null instead of unknown, as
// Terraform plans an optional attribute the configuration leaves out.
func planNull(t *testing.T, r resource.Resource, vals map[string]tftypes.Value, nulls ...string) tfsdk.Plan {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	typ := sr.Schema.Type().TerraformType(context.Background()).(tftypes.Object)
	for _, n := range append(nulls, "timeouts") {
		if _, set := vals[n]; !set {
			vals[n] = tftypes.NewValue(typ.AttributeTypes[n], nil)
		}
	}
	return plan(t, r, vals)
}

// netUUID is the ID of the seeded network net1.
const netUUID = "11111111-1111-4111-8111-111111111111"

func pathOf(attr string) path.Path { return path.Root(attr) }

// addDefaults fills in a placement default the configuration leaves out, as the
// backend's schema does on every write.
func addDefaults(fleet map[string]any) {
	if pc, ok := fleet["placement_config"].(map[string]any); ok {
		if _, set := pc["node_count"]; !set {
			pc["node_count"] = 3
		}
	}
}

// ---- cloud account ----------------------------------------------------------

// TestCloudAccount_Lifecycle: create, read, update, import and delete work, and the
// credentials stay as configured: they are never adopted from what the server returns.
func TestCloudAccount_Lifecycle(t *testing.T) {
	f := newInfraFake()
	r := NewCloudAccount()
	configured(t, r, infraClient(t, f))
	ctx := context.Background()
	creds := `{"aws_access_key":"AKIAEXAMPLEEXAMPLE","aws_access_secret":"s3cr3t-s3cr3t-s3cr3t-s3cr3t-s3cr3t"}`

	p := plan(t, r, map[string]tftypes.Value{"identifier": str("aws-prod"), "cloud_provider": str("aws"), "configuration": str(creds)})
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() || getStr(t, cr.State, "id") != "aws-prod" {
		t.Fatalf("create failed: %v", cr.Diagnostics)
	}

	// The server changes the stored credentials behind Terraform's back; the
	// state must still hold the configured value, never the server's.
	f.accounts["aws-prod"]["configuration"] = map[string]any{"aws_access_key": "OTHER"}
	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "configuration") != creds {
		t.Fatalf("read adopted credentials from the server: %v", rr.Diagnostics)
	}

	p2 := plan(t, r, map[string]tftypes.Value{"id": str("aws-prod"), "identifier": str("aws-prod"), "cloud_provider": str("aws"),
		"configuration": str(`{"aws_access_key":"AKIANEWNEWNEWNEWNEW","aws_access_secret":"n3w-n3w-n3w-n3w-n3w-n3w-n3w-n3w-n3w"}`)})
	ur := resource.UpdateResponse{State: rr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: rr.State}, &ur)
	if ur.Diagnostics.HasError() || f.accounts["aws-prod"]["configuration"].(map[string]any)["aws_access_key"] != "AKIANEWNEWNEWNEWNEW" {
		t.Fatalf("update did not send the new credentials: %v", ur.Diagnostics)
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "aws-prod"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	var cfg types.String
	ir.State.GetAttribute(ctx, pathOf("configuration"), &cfg)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "cloud_provider") != "aws" || !cfg.IsNull() {
		t.Fatalf("import must find the account without reading credentials into state: %v %v", ir.Diagnostics, cfg)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.accounts) != 0 {
		t.Fatalf("delete failed: %v", dr.Diagnostics)
	}
}

// ---- network ----------------------------------------------------------------

// TestNetwork_ProvisionedWaitsForCloudID: without network_id, create waits until
// Streamtime has created the cloud network and records its ID.
func TestNetwork_ProvisionedWaitsForCloudID(t *testing.T) {
	f := newInfraFake()
	f.pollsToReady = 3
	r := NewNetwork()
	configured(t, r, infraClient(t, f))
	p := planNull(t, r, map[string]tftypes.Value{"identifier": str("net1"), "name": str("net1"), "cloud_account": str("aws-prod"),
		"region": str("eu-west-1")}, "cidr")
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if got := getStr(t, cr.State, "network_id"); got != "vpc-123" {
		t.Fatalf("create returned before the cloud network existed: network_id=%q", got)
	}
	var prov types.Bool
	cr.State.GetAttribute(context.Background(), pathOf("provisioned_by_streamtime"), &prov)
	if !prov.ValueBool() {
		t.Fatal("a network Streamtime creates must be recorded as provisioned")
	}

	// Destroy removes the record but must say the cloud network is left in place.
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if dr.Diagnostics.HasError() || dr.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("destroy of a provisioned network must warn that the cloud network remains: %v", dr.Diagnostics)
	}
}

// TestNetwork_BringYourOwnDoesNotWait: with network_id set, create returns at once
// and destroy has nothing to warn about.
func TestNetwork_BringYourOwnDoesNotWait(t *testing.T) {
	f := newInfraFake()
	r := NewNetwork()
	configured(t, r, infraClient(t, f))
	p := planNull(t, r, map[string]tftypes.Value{"identifier": str("net2"), "name": str("net2"), "cloud_account": str("aws-prod"),
		"network_id": str("vpc-existing")}, "region", "cidr")
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if cr.Diagnostics.HasError() || getStr(t, cr.State, "network_id") != "vpc-existing" {
		t.Fatalf("create failed: %v", cr.Diagnostics)
	}
	for _, c := range f.calls {
		if c == "GET /organizations/acme/networks/net2/" {
			t.Fatal("a bring-your-own network must not wait for provisioning")
		}
	}
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if dr.Diagnostics.HasError() || dr.Diagnostics.WarningsCount() != 0 {
		t.Fatalf("destroy of a bring-your-own network: %v", dr.Diagnostics)
	}
}

// ---- fleet ------------------------------------------------------------------

func fleetPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{
		"identifier": str("prod"), "fleet_provider": str("aws"), "domain": str("example.com"), "network": str("net1"),
		"placement_config": str(`{"region":"eu-west-1"}`), "alert_webhook_secret": str("whsec"),
	}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "advanced_config", "workloads")
}

func seedNetwork(f *infraFake) {
	f.networks["net1"] = map[string]any{"id": netUUID, "identifier": "net1", "name": "net1", "provider": "aws-prod", "network_id": "vpc-1"}
}

// TestFleet_CreateReconcilesAndWaits: saving a fleet does not provision it, so create
// must start provisioning (reconcile, with an idempotency key and the saved generation)
// and wait until the fleet is ready. The network is sent by ID but kept by identifier.
func TestFleet_CreateReconcilesAndWaits(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if f.reconcileKey == "" || f.reconcileGen != 1 {
		t.Fatalf("create must reconcile generation 1 with an Idempotency-Key (key=%q gen=%v)", f.reconcileKey, f.reconcileGen)
	}
	if got := getStr(t, cr.State, "reconciliation_state"); got != "succeeded" || getStr(t, cr.State, "status") != "Healthy" {
		t.Fatalf("create returned before the fleet was ready: %s", got)
	}
	if f.fleets["prod"]["network"] != netUUID || getStr(t, cr.State, "network") != "net1" {
		t.Fatalf("network must be sent as its ID and kept as its identifier: sent %v, state %q", f.fleets["prod"]["network"], getStr(t, cr.State, "network"))
	}
	// The server added a placement default; the configured text is kept, so the next plan is empty.
	if got := getStr(t, cr.State, "placement_config"); got != `{"region":"eu-west-1"}` {
		t.Fatalf("a server default became a diff: %s", got)
	}
	if getStr(t, cr.State, "alert_webhook_secret") != "whsec" {
		t.Fatal("the webhook secret, which the server never returns, must stay in state")
	}
}

// TestFleet_FailedCreateIsAnErrorAndStaysInState: a provisioning failure fails the
// apply, but the fleet stays in state so Terraform replaces it rather than losing it.
func TestFleet_FailedCreateIsAnErrorAndStaysInState(t *testing.T) {
	f := newInfraFake()
	f.failReconcile = true
	seedNetwork(f)
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if !cr.Diagnostics.HasError() {
		t.Fatal("a failed provisioning must fail the apply")
	}
	if cr.State.Raw.IsNull() || getStr(t, cr.State, "id") != "prod" {
		t.Fatal("the fleet exists in Streamtime and must stay in state")
	}
}

// TestFleet_UpdateSavesAndWarnsWhenNotApplied: a placement change is an in-place update
// that is saved, with a warning that the running fleet does not change yet. A change
// that leaves the settings alone (domain) gives no warning.
func TestFleet_UpdateSavesAndWarnsWhenNotApplied(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	ctx := context.Background()
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	keep := map[string]tftypes.Value{"id": str("prod"), "generation": tftypes.NewValue(tftypes.Number, 1)}

	p := fleetPlan(t, r, merge(keep, map[string]tftypes.Value{"domain": str("other.example.com")}))
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || ur.Diagnostics.WarningsCount() != 0 || f.fleets["prod"]["domain"] != "other.example.com" {
		t.Fatalf("domain update: %v", ur.Diagnostics)
	}

	p2 := fleetPlan(t, r, merge(keep, map[string]tftypes.Value{"placement_config": str(`{"region":"eu-west-2"}`)}))
	ur2 := resource.UpdateResponse{State: ur.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: ur.State}, &ur2)
	if ur2.Diagnostics.HasError() || ur2.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("a placement change must be saved with a not-applied warning: %v", ur2.Diagnostics)
	}
	if f.fleets["prod"]["placement_config"].(map[string]any)["region"] != "eu-west-2" {
		t.Fatal("the placement change was not saved")
	}
}

// TestFleet_DeleteWaitsUntilGone: destroy returns only once Streamtime has removed
// the fleet, and a fleet already gone is success.
func TestFleet_DeleteWaitsUntilGone(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	f.pollsToReady = 3
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.fleets) != 0 {
		t.Fatalf("destroy returned before the fleet was removed: %v", dr.Diagnostics)
	}
	dr2 := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("destroying a fleet that is already gone must succeed: %v", dr2.Diagnostics)
	}
}

// TestFleet_ImportAdoptsServerSettings: an import has nothing configured, so the
// server's settings (network as its identifier) are adopted.
func TestFleet_ImportAdoptsServerSettings(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	f.fleets["legacy"] = map[string]any{"identifier": "legacy", "provider": "byok", "domain": "example.com", "network": netUUID,
		"placement_config": map[string]any{"region": "x"}, "workloads": map[string]any{"streaming": map[string]any{"enabled": true}},
		"generation": 2, "observed_generation": 2, "reconciliation_state": "succeeded", "status": "Healthy"}
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(context.Background(), resource.ImportStateRequest{ID: "legacy"}, &imp)
	rr := resource.ReadResponse{State: imp.State}
	r.Read(context.Background(), resource.ReadRequest{State: imp.State}, &rr)
	if rr.Diagnostics.HasError() {
		t.Fatal(rr.Diagnostics)
	}
	if getStr(t, rr.State, "network") != "net1" || getStr(t, rr.State, "placement_config") != `{"region":"x"}` ||
		getStr(t, rr.State, "workloads") != `{"streaming":{"enabled":true}}` {
		t.Fatalf("import did not adopt the server's settings: %s %s", getStr(t, rr.State, "network"), getStr(t, rr.State, "workloads"))
	}
}

// TestReplace_OnlyImmutableFields: a fleet is replaced (destroyed and created) only when
// its identifier, fleet provider or network changes. Any other change is in place.
func TestReplace_OnlyImmutableFields(t *testing.T) {
	var sr resource.SchemaResponse
	NewFleet().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	want := map[string]bool{"identifier": true, "fleet_provider": true, "network": true}
	for name, a := range sr.Schema.Attributes {
		var mods []planmodifier.String
		switch at := a.(type) {
		case rschema.StringAttribute:
			mods = at.PlanModifiers
		case rschema.Int64Attribute, rschema.BoolAttribute, rschema.SetAttribute, rschema.SingleNestedAttribute:
			if want[name] {
				t.Errorf("%s must force replacement", name)
			}
			continue
		}
		replaces := false
		live := tfsdk.State{Raw: tftypes.NewValue(tftypes.Object{AttributeTypes: map[string]tftypes.Type{}}, map[string]tftypes.Value{})}
		for _, m := range mods {
			resp := planmodifier.StringResponse{PlanValue: types.StringValue("new")}
			m.PlanModifyString(context.Background(), planmodifier.StringRequest{
				State: live, Plan: tfsdk.Plan{Raw: live.Raw},
				StateValue: types.StringValue("old"), PlanValue: types.StringValue("new"), ConfigValue: types.StringValue("new"),
			}, &resp)
			replaces = replaces || resp.RequiresReplace
		}
		if replaces != want[name] {
			t.Errorf("fleet.%s: replaces=%v, want %v", name, replaces, want[name])
		}
	}
}

// ---- JSON settings ----------------------------------------------------------

// TestKeptOrServerJSON: the configured text survives server defaults and key order.
// A changed value, or a configured key deleted outside Terraform, is drift and adopts
// the server's value; only a setting that holds credentials (hidden) may have keys the
// server keeps to itself.
func TestKeptOrServerJSON(t *testing.T) {
	for name, tc := range map[string]struct {
		kept   types.String
		server string
		hidden bool
		want   types.String
	}{
		"server adds a default":   {types.StringValue(`{"a":1}`), `{"a":1,"b":2}`, false, types.StringValue(`{"a":1}`)},
		"key order differs":       {types.StringValue(`{"b":{"y":1,"x":2},"a":1}`), `{"a":1,"b":{"x":2,"y":1}}`, false, types.StringValue(`{"b":{"y":1,"x":2},"a":1}`)},
		"number spelling":         {types.StringValue(`{"a":1.0}`), `{"a":1}`, false, types.StringValue(`{"a":1.0}`)},
		"value drifted":           {types.StringValue(`{"a":1}`), `{"a":2}`, false, types.StringValue(`{"a":2}`)},
		"list drifted":            {types.StringValue(`{"a":[1,2]}`), `{"a":[1]}`, false, types.StringValue(`{"a":[1]}`)},
		"configured key deleted":  {types.StringValue(`{"s":{"enabled":true,"units":3}}`), `{"s":{"enabled":true}}`, false, types.StringValue(`{"s":{"enabled":true}}`)},
		"credential kept hidden":  {types.StringValue(`{"a":1,"ssh_private_key":"k"}`), `{"a":1}`, true, types.StringValue(`{"a":1,"ssh_private_key":"k"}`)},
		"hidden still sees drift": {types.StringValue(`{"a":1,"ssh_private_key":"k"}`), `{"a":2}`, true, types.StringValue(`{"a":2}`)},
		"nothing configured":      {types.StringNull(), `{"a":1}`, false, types.StringValue(`{"a":1}`)},
		"unknown after create":    {types.StringUnknown(), `{"a":1}`, false, types.StringValue(`{"a":1}`)},
		"server empty":            {types.StringUnknown(), `{}`, false, types.StringNull()},
		"configured empty":        {types.StringValue(`{}`), `{}`, false, types.StringValue(`{}`)},
	} {
		if got := keptOrServerJSON(tc.kept, json.RawMessage(tc.server), tc.hidden); !got.Equal(tc.want) {
			t.Errorf("%s: got %s, want %s", name, got, tc.want)
		}
	}
}

// TestFleet_RemovingWebhookSecretClearsIt: the server never returns the webhook secret.
// Must not: removing it from the configuration leaves the old one active (risk: a
// signing credential the user meant to revoke still verifies webhooks).
func TestFleet_RemovingWebhookSecretClearsIt(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	ctx := context.Background()
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	f.webhookSecret = "whsec" // what the fake stored from the create
	p := fleetPlan(t, r, map[string]tftypes.Value{"id": str("prod"), "generation": tftypes.NewValue(tftypes.Number, 1),
		"alert_webhook_secret": tftypes.NewValue(tftypes.String, nil)})
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() {
		t.Fatal(ur.Diagnostics)
	}
	if f.webhookSecret != "" {
		t.Fatalf("the old webhook secret is still stored: %q", f.webhookSecret)
	}
}

// ---- data sources -----------------------------------------------------------

func readData(t *testing.T, d datasource.DataSource, c *client.Client, vals map[string]tftypes.Value) tfsdk.State {
	t.Helper()
	ctx := context.Background()
	var cr datasource.ConfigureResponse
	d.(datasource.DataSourceWithConfigure).Configure(ctx, datasource.ConfigureRequest{ProviderData: c}, &cr)
	var sr datasource.SchemaResponse
	d.Schema(ctx, datasource.SchemaRequest{}, &sr)
	typ := sr.Schema.Type().TerraformType(ctx).(tftypes.Object)
	full := map[string]tftypes.Value{}
	for name, at := range typ.AttributeTypes {
		if v, ok := vals[name]; ok {
			full[name] = v
		} else {
			full[name] = tftypes.NewValue(at, nil)
		}
	}
	cfg := tfsdk.Config{Schema: sr.Schema, Raw: tftypes.NewValue(typ, full)}
	resp := datasource.ReadResponse{State: tfsdk.State{Schema: sr.Schema, Raw: tftypes.NewValue(typ, nil)}}
	d.Read(ctx, datasource.ReadRequest{Config: cfg}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatal(resp.Diagnostics)
	}
	return resp.State
}

// TestFleetDataSource_ReadsByIdentifier: the fleet data source names the network by identifier.
func TestFleetDataSource_ReadsByIdentifier(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	f.fleets["prod"] = map[string]any{"identifier": "prod", "provider": "aws", "domain": "example.com", "network": netUUID,
		"generation": 1, "observed_generation": 1, "status": "Healthy"}
	st := readData(t, NewFleetDataSource(), infraClient(t, f), map[string]tftypes.Value{"identifier": str("prod")})
	if getStr(t, st, "network") != "net1" || getStr(t, st, "status") != "Healthy" {
		t.Fatalf("fleet data source: network=%q status=%q", getStr(t, st, "network"), getStr(t, st, "status"))
	}
}

// TestFleetNodePools_ReadsEveryPage: the node pool list follows the API's pages.
func TestFleetNodePools_ReadsEveryPage(t *testing.T) {
	f := newInfraFake()
	f.fleets["prod"] = map[string]any{"identifier": "prod"}
	st := readData(t, NewFleetNodePoolsDataSource(), infraClient(t, f), map[string]tftypes.Value{"fleet": str("prod")})
	var pools []nodePoolModel
	st.GetAttribute(context.Background(), pathOf("node_pools"), &pools)
	if len(pools) != 2 || pools[0].Size.ValueInt64() != 3 || pools[0].Metadata.ValueString() != `{"name":"pool-a"}` {
		t.Fatalf("node pools: %+v", pools)
	}
}

// TestFleetConfigurationProfiles_Read: the profiles list and one profile's schemas are read
// as the spec's FleetConfigurationProfileSummary and FleetConfigurationProfile.
func TestFleetConfigurationProfiles_Read(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/organizations/acme/fleet/configuration-profiles/":
			_, _ = w.Write([]byte(`[{"identifier":"aws","display_name":"AWS EKS","description":"d","version":"v0.1","priority":2,
				"details":{},"enabled":false,"disabled":true,"disabled_reasons":["Add a cloud account first."]}]`))
		case "/organizations/acme/fleet/configuration-profiles/aws/":
			_, _ = w.Write([]byte(`{"provider":"aws","placement":{"json_schema":{"type":"object"}},"advanced_config":{"detail":"needs placement"}}`))
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	c, _ := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})

	st := readData(t, NewFleetConfigurationProfilesDataSource(), c, nil)
	var profiles []fleetProfileSummary
	st.GetAttribute(context.Background(), pathOf("profiles"), &profiles)
	if len(profiles) != 1 || profiles[0].Identifier.ValueString() != "aws" || profiles[0].Enabled.ValueBool() ||
		len(profiles[0].DisabledReasons.Elements()) != 1 {
		t.Fatalf("profiles: %+v", profiles)
	}
	one := readData(t, NewFleetConfigurationProfileDataSource(), c, map[string]tftypes.Value{"fleet_provider": str("aws")})
	if getStr(t, one, "placement_schema") != `{"json_schema":{"type":"object"}}` {
		t.Fatalf("placement schema: %s", getStr(t, one, "placement_schema"))
	}
}

func merge(a, b map[string]tftypes.Value) map[string]tftypes.Value {
	out := map[string]tftypes.Value{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// replacesString runs an attribute's plan modifiers on a change from old to new.
func replacesString(t *testing.T, r resource.Resource, attr string, old, new types.String) bool {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	sa := sr.Schema.Attributes[attr].(rschema.StringAttribute)
	live := tfsdk.State{Raw: tftypes.NewValue(tftypes.Object{AttributeTypes: map[string]tftypes.Type{}}, map[string]tftypes.Value{})}
	replaces := false
	for _, m := range sa.PlanModifiers {
		resp := planmodifier.StringResponse{PlanValue: new}
		m.PlanModifyString(context.Background(), planmodifier.StringRequest{
			State: live, Plan: tfsdk.Plan{Raw: live.Raw}, StateValue: old, PlanValue: new, ConfigValue: new,
		}, &resp)
		replaces = replaces || resp.RequiresReplace
	}
	return replaces
}

// TestFleet_DroppedNetworkRelinksInPlace: deleting a network clears it on its fleets.
// Must not: that destroys the fleet on the next apply (risk: a running cluster replaced).
// The read reports no network, and putting it back is an in-place update that sends it again.
func TestFleet_DroppedNetworkRelinksInPlace(t *testing.T) {
	f := newInfraFake()
	seedNetwork(f)
	r := NewFleet()
	configured(t, r, infraClient(t, f))
	ctx := context.Background()
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: fleetPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}

	f.fleets["prod"]["network"] = nil // the backend's SET_NULL after the network was deleted
	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
	var n types.String
	rr.State.GetAttribute(ctx, pathOf("network"), &n)
	if rr.Diagnostics.HasError() || !n.IsNull() {
		t.Fatalf("read must report the dropped link: network=%v %v", n, rr.Diagnostics)
	}
	if replacesString(t, r, "network", types.StringNull(), types.StringValue("net1")) {
		t.Fatal("re-linking a dropped network must not replace the fleet")
	}
	if !replacesString(t, r, "network", types.StringValue("net1"), types.StringValue("net2")) {
		t.Fatal("moving a fleet to another network must replace it")
	}

	p := fleetPlan(t, r, map[string]tftypes.Value{"id": str("prod"), "generation": tftypes.NewValue(tftypes.Number, 1)})
	ur := resource.UpdateResponse{State: rr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: rr.State}, &ur)
	if ur.Diagnostics.HasError() || f.fleets["prod"]["network"] != netUUID || getStr(t, ur.State, "network") != "net1" {
		t.Fatalf("update did not link the network again: sent %v, %v", f.fleets["prod"]["network"], ur.Diagnostics)
	}
}

// TestCloudAccount_ProviderChangeIsInPlace: deleting a cloud account also deletes the
// networks and node pools under it. Must not: a provider change replaces the account
// (risk: records the plan does not show are deleted).
func TestCloudAccount_ProviderChangeIsInPlace(t *testing.T) {
	if replacesString(t, NewCloudAccount(), "cloud_provider", types.StringValue("aws"), types.StringValue("gcp")) {
		t.Fatal("cloud_provider must update in place")
	}
}

// TestJSONError_DoesNotEchoValue: a malformed JSON setting may hold credentials.
// Must not: the error quotes any of it (risk: a secret printed in the console).
func TestJSONError_DoesNotEchoValue(t *testing.T) {
	_, err := jsonObject(types.StringValue(`{"aws_access_secret": SECRETVALUE}`), "configuration")
	if err == nil {
		t.Fatal("malformed JSON must be an error")
	}
	// The decoder's own message ("invalid character 'S' ...") quotes the value.
	if strings.Contains(err.Error(), "SECRET") || strings.ContainsAny(err.Error(), "'\"") {
		t.Fatalf("error must not quote part of the value: %v", err)
	}
}
