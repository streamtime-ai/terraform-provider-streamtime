package resources

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

func init() {
	flinkPoll.initial, flinkPoll.max = time.Millisecond, time.Millisecond
	clickHousePoll.initial, clickHousePoll.max = time.Millisecond, time.Millisecond
}

type reconcileCall struct {
	key        string
	generation float64
}

// workloadFake answers a Flink or ClickHouse collection the way the backend does:
// POST saves, reconcile deploys the saved generation some polls later, a PUT that
// changes what is deployed raises the generation, and DELETE removes the object
// some polls later.
type workloadFake struct {
	mu            sync.Mutex
	collection    string   // "flink" or "clickhouses"
	deployable    []string // fields whose change raises the generation
	items         map[string]map[string]any
	pollsToReady  int
	failReconcile bool
	created       map[string]any
	puts          []map[string]any
	reconciles    []reconcileCall
	paths         []string
}

func newFlinkFake() *workloadFake {
	return &workloadFake{collection: "flink", items: map[string]map[string]any{}, pollsToReady: 2,
		deployable: []string{"flink_units", "max_flink_units", "advanced_configuration", "project_version"}}
}

func newClickHouseFake() *workloadFake {
	return &workloadFake{collection: "clickhouses", items: map[string]map[string]any{}, pollsToReady: 2,
		deployable: []string{"retention_days", "advanced_configuration"}}
}

// serverDefaults fills in what the backend's schemas add on every write.
func (f *workloadFake) serverDefaults(o map[string]any) {
	if adv, ok := o["advanced_configuration"].(map[string]any); ok {
		adv["parallelism"] = 1
	}
	if f.collection == "clickhouses" {
		for _, k := range []string{"cloud_provider", "tenancy_mode", "region"} {
			if _, set := o[k]; !set {
				o[k] = ""
			}
		}
		if _, set := o["retention_days"]; !set {
			o["retention_days"] = 30
		}
	} else if _, set := o["provider"]; !set {
		o["provider"] = "open-source"
	}
}

func (f *workloadFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.paths = append(f.paths, r.URL.Path)
		reply := func(code int, v any) {
			w.WriteHeader(code)
			if v != nil {
				_ = json.NewEncoder(w).Encode(v)
			}
		}
		rest, ok := strings.CutPrefix(r.URL.Path, "/organizations/acme/"+f.collection+"/")
		if !ok {
			reply(404, nil)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		switch {
		case rest == "" && r.Method == http.MethodPost:
			f.created = in
			f.serverDefaults(in)
			in["generation"], in["observed_generation"] = 1, 0
			in["status"], in["reconciliation_state"], in["desired_lifecycle"] = "Pending", "never_reconciled", "present"
			in["configuration_state"] = "valid"
			if f.collection == "flink" && emptyJSON(in["advanced_configuration"]) {
				in["configuration_state"] = "incomplete" // Flink deploys only with advanced settings saved
			}
			f.items[in["identifier"].(string)] = in
			reply(201, in)
		case len(parts) == 2 && parts[1] == "reconcile":
			o := f.items[parts[0]]
			if o["configuration_state"] != "valid" {
				reply(400, map[string]any{"detail": "Advanced configuration is required for reconciliation."})
				return
			}
			gen, _ := in["generation"].(float64)
			f.reconciles = append(f.reconciles, reconcileCall{r.Header.Get("Idempotency-Key"), gen})
			o["reconciliation_state"], o["_polls"] = "queued", 0
			reply(202, map[string]any{"status": "queued"})
		case len(parts) == 1:
			o, ok := f.items[parts[0]]
			if !ok {
				reply(404, nil)
				return
			}
			switch r.Method {
			case http.MethodGet:
				f.advance(parts[0], o)
				if _, still := f.items[parts[0]]; !still {
					reply(404, nil)
					return
				}
				reply(200, o)
			case http.MethodPut:
				f.puts = append(f.puts, in)
				before := f.deployed(o)
				for k, v := range in {
					if k != "identifier" && k != "mode" && k != "provider" { // read-only on update
						o[k] = v
					}
				}
				f.serverDefaults(o)
				if !jsonEqual(before, f.deployed(o)) {
					o["generation"] = o["generation"].(int) + 1
					o["reconciliation_state"] = "pending"
				}
				reply(200, o)
			case http.MethodDelete:
				o["desired_lifecycle"], o["status"], o["reconciliation_state"], o["_polls"] = "absent", "Deleting", "queued", 0
				reply(202, map[string]any{"id": "op-1", "kind": "delete", "status": "pending"})
			}
		default:
			reply(404, nil)
		}
	})
}

// deployed is what the saved object would deploy, as the backend compares it.
func (f *workloadFake) deployed(o map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range f.deployable {
		out[k] = o[k]
	}
	return out
}

// jsonEqual compares a request value with a stored one as JSON.
func jsonEqual(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	var va, vb any
	_ = json.Unmarshal(ja, &va)
	_ = json.Unmarshal(jb, &vb)
	return reflect.DeepEqual(va, vb)
}

// advance moves an object along after enough polls, as the workflows would.
func (f *workloadFake) advance(id string, o map[string]any) {
	polls, _ := o["_polls"].(int)
	o["_polls"] = polls + 1
	if polls+1 < f.pollsToReady {
		return
	}
	switch {
	case o["desired_lifecycle"] == "absent":
		delete(f.items, id)
	case o["reconciliation_state"] == "queued" && f.failReconcile:
		o["reconciliation_state"], o["status"] = "failed", "Error"
	case o["reconciliation_state"] == "queued":
		o["reconciliation_state"], o["status"], o["observed_generation"] = "succeeded", "Healthy", o["generation"]
		if o["fleet"] == nil {
			o["fleet"] = "auto-fleet" // Flink chooses a fleet when it is first deployed
		}
	}
}

func fakeClient(t *testing.T, h http.Handler) *client.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func flinkPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{"identifier": str("etl"), "mode": str("application"), "flink_units": num(2),
		"advanced_configuration": str(`{"application_jar":"local:///opt/etl.jar"}`)}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "description", "max_flink_units", "project_version")
}

func createFlink(t *testing.T, f *workloadFake) (resource.Resource, tfsdk.State) {
	t.Helper()
	r := NewFlink()
	configured(t, r, fakeClient(t, f.handler()))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: flinkPlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	return r, cr.State
}

// replacingAttributes lists the top-level attributes whose change forces replacement.
func replacingAttributes(t *testing.T, r resource.Resource) map[string]bool {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	out := map[string]bool{}
	for name, a := range sr.Schema.Attributes {
		switch at := a.(type) {
		case rschema.StringAttribute:
			if replacesString(t, r, name, types.StringValue("old"), types.StringValue("new")) {
				out[name] = true
			}
		case rschema.Int64Attribute:
			if int64Change(at.PlanModifiers, 1, 2).RequiresReplace {
				out[name] = true
			}
		}
	}
	return out
}

// ---- streamtime_flink -------------------------------------------------------

// TestFlink_CreateReconcilesAndWaits: saving an instance does not deploy it, so
// create starts the deployment with the saved generation and an idempotency key,
// returns only when it is ready, and reads back the fleet Streamtime chose.
func TestFlink_CreateReconcilesAndWaits(t *testing.T) {
	f := newFlinkFake()
	_, st := createFlink(t, f)
	if len(f.reconciles) != 1 || f.reconciles[0].generation != 1 || !strings.Contains(f.reconciles[0].key, "create") {
		t.Fatalf("create must reconcile generation 1 with an Idempotency-Key: %+v", f.reconciles)
	}
	if getStr(t, st, "status") != "Healthy" || getStr(t, st, "fleet") != "auto-fleet" {
		t.Fatal("create returned before the instance was ready, or lost the chosen fleet")
	}
	if getStr(t, st, "advanced_configuration") != `{"application_jar":"local:///opt/etl.jar"}` {
		t.Fatalf("a server default became a diff: %s", getStr(t, st, "advanced_configuration"))
	}
}

// TestFlink_CreateSendsAdvancedConfigurationWhenUnset: Streamtime deploys an instance
// only with its advanced settings saved, so create sends them even when unset.
func TestFlink_CreateSendsAdvancedConfigurationWhenUnset(t *testing.T) {
	f := newFlinkFake()
	r := NewFlink()
	configured(t, r, fakeClient(t, f.handler()))
	p := planNull(t, r, map[string]tftypes.Value{"identifier": str("ws"), "mode": str("session"), "max_flink_units": num(8)},
		"description", "flink_units", "project_version")
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: p}, &cr)
	if _, sent := f.created["advanced_configuration"]; !sent {
		t.Fatal("create must send advanced_configuration even when it is not configured")
	}
}

// TestFlink_ChangeIsDeployedOnlyWhenGenerationRises: a capacity change is saved in
// place, deployed and waited for; a change that does not touch what runs (the
// description) is saved without a deployment.
func TestFlink_ChangeIsDeployedOnlyWhenGenerationRises(t *testing.T) {
	f := newFlinkFake()
	r, st := createFlink(t, f)
	ctx := context.Background()

	p := flinkPlan(t, r, map[string]tftypes.Value{"id": str("etl"), "flink_units": num(4)})
	ur := resource.UpdateResponse{State: st}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: st}, &ur)
	if ur.Diagnostics.HasError() {
		t.Fatal(ur.Diagnostics)
	}
	if len(f.reconciles) != 2 || f.reconciles[1].generation != 2 || !strings.Contains(f.reconciles[1].key, "update") {
		t.Fatalf("a capacity change must deploy generation 2: %+v", f.reconciles)
	}
	var observed int64
	ur.State.GetAttribute(ctx, pathOf("observed_generation"), &observed)
	if observed != 2 || getStr(t, ur.State, "status") != "Healthy" {
		t.Fatalf("update returned before the change was running (observed_generation %d)", observed)
	}

	p2 := flinkPlan(t, r, map[string]tftypes.Value{"id": str("etl"), "flink_units": num(4), "description": str("nightly ETL")})
	ur2 := resource.UpdateResponse{State: ur.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: ur.State}, &ur2)
	if ur2.Diagnostics.HasError() || len(f.reconciles) != 2 {
		t.Fatalf("a description change must not redeploy: %v %+v", ur2.Diagnostics, f.reconciles)
	}
}

// modifyPlan runs a resource's ModifyPlan for a plan that matches the state, as
// Terraform does when the configuration has not changed.
func modifyPlan(t *testing.T, r resource.Resource, st tfsdk.State) resource.ModifyPlanResponse {
	t.Helper()
	p := tfsdk.Plan{Schema: st.Schema, Raw: st.Raw.Copy()}
	resp := resource.ModifyPlanResponse{Plan: p}
	r.(resource.ResourceWithModifyPlan).ModifyPlan(context.Background(), resource.ModifyPlanRequest{State: st, Plan: p}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatal(resp.Diagnostics)
	}
	return resp
}

// TestFlink_FailedChangeIsRetriedByTheNextApply: a change that failed to deploy is
// saved and matches the configuration, so without help the next plan would be
// empty. The plan shows an update with a warning, and that apply deploys it.
func TestFlink_FailedChangeIsRetriedByTheNextApply(t *testing.T) {
	f := newFlinkFake()
	r, st := createFlink(t, f)
	ctx := context.Background()
	f.failReconcile = true
	p := flinkPlan(t, r, map[string]tftypes.Value{"id": str("etl"), "flink_units": num(4)})
	ur := resource.UpdateResponse{State: st}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: st}, &ur)
	if !ur.Diagnostics.HasError() {
		t.Fatal("a failed deployment of a change must fail the apply")
	}

	f.failReconcile = false
	mp := modifyPlan(t, r, ur.State)
	var observed types.Int64
	mp.Plan.GetAttribute(ctx, pathOf("observed_generation"), &observed)
	if !observed.IsUnknown() || mp.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("the next plan must show the undeployed change as an update, with a warning: %v %v", observed, mp.Diagnostics)
	}
	ur2 := resource.UpdateResponse{State: ur.State}
	r.Update(ctx, resource.UpdateRequest{Plan: mp.Plan, State: ur.State}, &ur2)
	if ur2.Diagnostics.HasError() || len(f.reconciles) != 3 || f.reconciles[2].generation != 2 {
		t.Fatalf("the next apply must deploy generation 2 again: %v %+v", ur2.Diagnostics, f.reconciles)
	}
	if getStr(t, ur2.State, "status") != "Healthy" {
		t.Fatal("the retried change must be running when apply returns")
	}
	if mp2 := modifyPlan(t, r, ur2.State); mp2.Diagnostics.WarningsCount() != 0 {
		t.Fatalf("once the change runs, the plan must be empty again: %v", mp2.Diagnostics)
	}
}

// TestFlink_RunningDeploymentIsWaitedForNotRestarted: Must not: an apply while a
// deployment of the saved settings is under way starts a second one (risk: two
// rollouts of one change). It waits for the one that is running.
func TestFlink_RunningDeploymentIsWaitedForNotRestarted(t *testing.T) {
	f := newFlinkFake()
	r, st := createFlink(t, f)
	o := f.items["etl"]
	o["generation"], o["reconciliation_state"], o["_polls"] = 2, "queued", -5 // still under way after the refresh
	rr := resource.ReadResponse{State: st}
	r.Read(context.Background(), resource.ReadRequest{State: st}, &rr)
	st = rr.State
	var gen types.Int64
	st.GetAttribute(context.Background(), pathOf("generation"), &gen)
	if gen.ValueInt64() != 2 {
		t.Fatalf("the refresh must see the deployment under way (generation %v)", gen)
	}
	if mp := modifyPlan(t, r, st); mp.Diagnostics.WarningsCount() != 0 {
		t.Fatal("a deployment under way is not a change to plan")
	}
	p := flinkPlan(t, r, map[string]tftypes.Value{"id": str("etl"), "description": str("x")})
	ur := resource.UpdateResponse{State: st}
	r.Update(context.Background(), resource.UpdateRequest{Plan: p, State: st}, &ur)
	if ur.Diagnostics.HasError() || len(f.reconciles) != 1 || getStr(t, ur.State, "status") != "Healthy" {
		t.Fatalf("update must wait for the running deployment without starting another: %v %+v", ur.Diagnostics, f.reconciles)
	}
}

// TestFlink_FailedDeploymentIsAnErrorAndStaysInState: a failed deployment fails the
// apply, but the instance stays in state so Terraform replaces it rather than losing it.
func TestFlink_FailedDeploymentIsAnErrorAndStaysInState(t *testing.T) {
	f := newFlinkFake()
	f.failReconcile = true
	r := NewFlink()
	configured(t, r, fakeClient(t, f.handler()))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: flinkPlan(t, r, nil)}, &cr)
	if !cr.Diagnostics.HasError() || !strings.Contains(cr.Diagnostics[0].Detail(), "could not deploy the Flink instance") {
		t.Fatalf("a failed deployment must fail the apply, naming the instance: %v", cr.Diagnostics)
	}
	if cr.State.Raw.IsNull() || getStr(t, cr.State, "id") != "etl" {
		t.Fatal("the instance exists in Streamtime and must stay in state")
	}
}

// TestFlink_ReplaceOnlyImmutableFields: Must not: a change Streamtime can deploy in
// place destroys the instance and its job state. Only the fields the API refuses to
// change force replacement.
func TestFlink_ReplaceOnlyImmutableFields(t *testing.T) {
	want := map[string]bool{"identifier": true, "mode": true, "flink_provider": true, "fleet": true}
	if got := replacingAttributes(t, NewFlink()); !reflect.DeepEqual(got, want) {
		t.Fatalf("replacing attributes = %v, want %v", got, want)
	}
}

// TestFlink_DeleteWaitsUntilGone: destroy returns only once Streamtime has removed the
// instance, and an instance that is already gone is success.
func TestFlink_DeleteWaitsUntilGone(t *testing.T) {
	f := newFlinkFake()
	r, st := createFlink(t, f)
	f.pollsToReady = 3
	dr := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr)
	if dr.Diagnostics.HasError() || len(f.items) != 0 {
		t.Fatalf("destroy returned before the instance was removed: %v", dr.Diagnostics)
	}
	dr2 := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("destroying an instance that is already gone must succeed: %v", dr2.Diagnostics)
	}
}

// TestFlink_ImportAdoptsServerSettings: an import has nothing configured, so the
// server's settings are adopted, including the project version it runs.
func TestFlink_ImportAdoptsServerSettings(t *testing.T) {
	f := newFlinkFake()
	f.items["legacy"] = map[string]any{"identifier": "legacy", "provider": "cmf", "mode": "application", "fleet": "prod",
		"flink_units": 3, "advanced_configuration": map[string]any{"parallelism": 2},
		"project_version": map[string]any{"project": "etl", "version": "1.2.0"},
		"tags":            []any{map[string]any{"key": "team", "value": "data"}}, "generation": 4, "observed_generation": 4,
		"reconciliation_state": "succeeded", "status": "Healthy"}
	r := NewFlink()
	configured(t, r, fakeClient(t, f.handler()))
	ctx := context.Background()
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "legacy"}, &imp)
	rr := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "flink_provider") != "cmf" || getStr(t, rr.State, "advanced_configuration") != `{"parallelism":2}` {
		t.Fatalf("import did not adopt the server's settings: %v", rr.Diagnostics)
	}
	var pv projectVersionModel
	rr.State.GetAttribute(ctx, pathOf("project_version"), &pv)
	if pv.Version.ValueString() != "1.2.0" {
		t.Fatalf("project_version: %+v", pv)
	}
}

// ---- streamtime_clickhouse --------------------------------------------------

func clickHousePlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{"identifier": str("events"), "fleet": str("prod"),
		"advanced_configuration": str(`{"shards":2}`)}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "description")
}

// TestClickHouse_Lifecycle: create deploys on the chosen fleet and waits; a retention
// change is deployed in place; the identifier is never sent on update (Streamtime
// cannot rename); an import reads the database back; destroy waits until it is gone.
func TestClickHouse_Lifecycle(t *testing.T) {
	f := newClickHouseFake()
	r := NewClickHouse()
	configured(t, r, fakeClient(t, f.handler()))
	ctx := context.Background()

	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: clickHousePlan(t, r, nil)}, &cr)
	if cr.Diagnostics.HasError() || f.created["fleet"] != "prod" || len(f.reconciles) != 1 {
		t.Fatalf("create: %v %v %+v", cr.Diagnostics, f.created, f.reconciles)
	}
	var days int64
	cr.State.GetAttribute(ctx, pathOf("retention_days"), &days)
	if days != 30 || getStr(t, cr.State, "status") != "Healthy" {
		t.Fatalf("the default retention must be read back and create must wait (days %d)", days)
	}

	p := clickHousePlan(t, r, map[string]tftypes.Value{"id": str("events"), "retention_days": num(7)})
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || len(f.reconciles) != 2 || f.reconciles[1].generation != 2 {
		t.Fatalf("a retention change must be deployed: %v %+v", ur.Diagnostics, f.reconciles)
	}
	if _, sent := f.puts[0]["identifier"]; sent {
		t.Fatal("update must not send the identifier")
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "events"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "fleet") != "prod" {
		t.Fatalf("import: %v", ir.Diagnostics)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.items) != 0 {
		t.Fatalf("destroy returned before the database was removed: %v", dr.Diagnostics)
	}
	rr := resource.ReadResponse{State: ur.State}
	r.Read(ctx, resource.ReadRequest{State: ur.State}, &rr)
	if !rr.State.Raw.IsNull() {
		t.Fatal("a database deleted outside Terraform must drop from state")
	}
}

// TestClickHouse_FailedChangeIsPlannedAgain: the next plan after a failed deployment
// of a ClickHouse change is an update that deploys it.
func TestClickHouse_FailedChangeIsPlannedAgain(t *testing.T) {
	f := newClickHouseFake()
	r := NewClickHouse()
	configured(t, r, fakeClient(t, f.handler()))
	ctx := context.Background()
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: clickHousePlan(t, r, nil)}, &cr)
	f.failReconcile = true
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: clickHousePlan(t, r, map[string]tftypes.Value{"id": str("events"), "retention_days": num(7)}), State: cr.State}, &ur)
	if !ur.Diagnostics.HasError() {
		t.Fatal("a failed deployment of a change must fail the apply")
	}
	mp := modifyPlan(t, r, ur.State)
	var observed types.Int64
	mp.Plan.GetAttribute(ctx, pathOf("observed_generation"), &observed)
	if !observed.IsUnknown() || mp.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("the next plan must be an update with a warning: %v %v", observed, mp.Diagnostics)
	}
}

// TestClickHouse_ReplaceOnlyImmutableFields: Must not: a change Streamtime can deploy
// in place destroys the database and its data. Only identifier and fleet replace it.
func TestClickHouse_ReplaceOnlyImmutableFields(t *testing.T) {
	want := map[string]bool{"identifier": true, "fleet": true}
	if got := replacingAttributes(t, NewClickHouse()); !reflect.DeepEqual(got, want) {
		t.Fatalf("replacing attributes = %v, want %v", got, want)
	}
}

// ---- streamtime_flink_project -----------------------------------------------

// flinkProjectFake answers the Flink project and store endpoints: the CI key is
// returned only by create, and the store never returns its credential.
type flinkProjectFake struct {
	mu       sync.Mutex
	projects map[string]map[string]any
	stores   map[string]map[string]any
	storePUT []map[string]any
	inUse    bool
	paths    []string
}

func (f *flinkProjectFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.paths = append(f.paths, r.URL.Path)
		reply := func(code int, v any) {
			w.WriteHeader(code)
			if v != nil {
				_ = json.NewEncoder(w).Encode(v)
			}
		}
		rest, ok := strings.CutPrefix(r.URL.Path, "/organizations/acme/flink-projects/")
		if !ok {
			reply(404, nil)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		switch {
		case rest == "" && r.Method == http.MethodPost:
			for k, v := range map[string]any{"runtime": "java", "build_tool": "maven"} {
				if _, set := in[k]; !set {
					in[k] = v
				}
			}
			if meta, ok := in["metadata"].(map[string]any); ok { // the schema's defaults
				meta["default_args"], meta["entry_class"], meta["documentation_url"] = []any{}, nil, nil
			}
			in["artifact_kind"], in["latest_version"], in["versions_count"] = "jar", nil, 0
			f.projects[in["identifier"].(string)] = in
			out := map[string]any{"has_ci_key": false}
			for k, v := range in {
				out[k] = v
			}
			reply(201, out)
		case len(parts) == 1:
			p, ok := f.projects[parts[0]]
			if !ok {
				reply(404, nil)
				return
			}
			switch r.Method {
			case http.MethodGet:
				reply(200, p)
			case http.MethodPut:
				for k, v := range in {
					if k != "identifier" && k != "runtime" {
						p[k] = v
					}
				}
				reply(200, p)
			case http.MethodDelete:
				if f.inUse {
					reply(409, map[string]any{"error": "This project is still deployed. Redeploy or delete these first: flink/etl"})
					return
				}
				delete(f.projects, parts[0])
				delete(f.stores, parts[0])
				reply(204, nil)
			}
		case len(parts) == 2 && parts[1] == "store":
			s, ok := f.stores[parts[0]]
			switch r.Method {
			case http.MethodGet:
				if !ok {
					reply(404, nil)
					return
				}
				reply(200, s)
			case http.MethodPut:
				f.storePUT = append(f.storePUT, in)
				auth, _ := in["authentication"].(string)
				if auth == "" {
					auth = "none"
				}
				ca, _ := in["ca_certificate"].(string)
				hasCred := in["password"] != nil || in["token"] != nil || (ok && s["has_credential"] == true)
				s = map[string]any{"store_type": in["store_type"], "url": in["url"], "ca_certificate": ca,
					"authentication": auth, "has_credential": hasCred, "updated_at": "2026-10-31T00:00:00Z"}
				f.stores[parts[0]] = s
				reply(200, s)
			case http.MethodDelete:
				if !ok {
					reply(404, nil)
					return
				}
				delete(f.stores, parts[0])
				reply(204, nil)
			}
		default:
			reply(404, nil)
		}
	})
}

func storeValue(t *testing.T, r resource.Resource, set map[string]string) tftypes.Value {
	t.Helper()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	st := sr.Schema.Type().TerraformType(context.Background()).(tftypes.Object).AttributeTypes["store"].(tftypes.Object)
	if set == nil {
		return tftypes.NewValue(st, nil)
	}
	vals := map[string]tftypes.Value{}
	for name, at := range st.AttributeTypes {
		switch v, ok := set[name]; {
		case ok:
			vals[name] = str(v)
		case name == "has_credential" || name == "ca_certificate" || name == "authentication":
			vals[name] = tftypes.NewValue(at, tftypes.UnknownValue)
		default:
			vals[name] = tftypes.NewValue(at, nil)
		}
	}
	return tftypes.NewValue(st, vals)
}

func projectPlan(t *testing.T, r resource.Resource, store map[string]string, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{"identifier": str("etl"), "name": str("ETL jobs"), "store": storeValue(t, r, store),
		"cluster": str("shared"), "flink_unit_budget": tftypes.NewValue(tftypes.Number, 2)}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "description")
}

var ghcr = map[string]string{"store_type": "oras", "url": "https://ghcr.io", "authentication": "token", "token": "ghp_x"}

// TestFlinkProject_Lifecycle: create keeps the one-time CI key, sends metadata as an
// object (its server defaults are not a diff) and connects the store, keeping its
// token (never returned); removing the store disconnects it; an import
// reads the project and store back, without the key; delete removes the project.
func TestFlinkProject_Lifecycle(t *testing.T) {
	f := &flinkProjectFake{projects: map[string]map[string]any{}, stores: map[string]map[string]any{}}
	r := NewFlinkProject()
	configured(t, r, fakeClient(t, f.handler()))
	ctx := context.Background()

	cr := resource.CreateResponse{State: emptyState(t, r)}
	meta := str(`{"labels":{"team":"data"}}`)
	r.Create(ctx, resource.CreateRequest{Plan: projectPlan(t, r, ghcr, map[string]tftypes.Value{"metadata": meta})}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if getStr(t, cr.State, "metadata") != `{"labels":{"team":"data"}}` {
		t.Fatalf("metadata defaults Streamtime fills in became a diff: %s", getStr(t, cr.State, "metadata"))
	}
	if getStr(t, cr.State, "runtime") != "java" || getStr(t, cr.State, "build_tool") != "maven" {
		t.Fatal("the default runtime and the build tool derived from it must be in state")
	}
	var token types.String
	cr.State.GetAttribute(ctx, pathOf("store").AtName("token"), &token)
	if token.ValueString() != "ghp_x" || f.storePUT[0]["token"] != "ghp_x" {
		t.Fatal("the store token must be sent, and kept in state although Streamtime never returns it")
	}

	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)

	p := projectPlan(t, r, nil, map[string]tftypes.Value{"id": str("etl"), "description": str("nightly")})
	ur := resource.UpdateResponse{State: rr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: rr.State}, &ur)
	if ur.Diagnostics.HasError() || len(f.stores) != 0 || f.projects["etl"]["description"] != "nightly" {
		t.Fatalf("removing the store must disconnect it: %v %v", ur.Diagnostics, f.stores)
	}
	var store *flinkProjectStore
	ur.State.GetAttribute(ctx, pathOf("store"), &store)
	if store != nil {
		t.Fatal("the store must be gone from state")
	}

	f.stores["etl"] = map[string]any{"store_type": "jfrog", "url": "https://acme.jfrog.io", "ca_certificate": "",
		"authentication": "basic", "has_credential": true}
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "etl"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "name") != "ETL jobs" {
		t.Fatalf("import: %v", ir.Diagnostics)
	}
	var url types.String
	ir.State.GetAttribute(ctx, pathOf("store").AtName("url"), &url)
	if url.ValueString() != "https://acme.jfrog.io" {
		t.Fatalf("an import reads the store back: url=%v", url)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.projects) != 0 {
		t.Fatalf("delete: %v", dr.Diagnostics)
	}
	dr2 := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("deleting a project that is already gone must succeed: %v", dr2.Diagnostics)
	}
}

// TestFlinkProject_DeleteInUseNamesWhatRunsIt: Streamtime refuses to delete a
// project something still runs; destroy fails with its message, naming what to change.
func TestFlinkProject_DeleteInUseNamesWhatRunsIt(t *testing.T) {
	f := &flinkProjectFake{projects: map[string]map[string]any{}, stores: map[string]map[string]any{}}
	r := NewFlinkProject()
	configured(t, r, fakeClient(t, f.handler()))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: projectPlan(t, r, nil, nil)}, &cr)
	f.inUse = true
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if !dr.Diagnostics.HasError() || !strings.Contains(dr.Diagnostics[0].Detail(), "flink/etl") {
		t.Fatalf("delete of a project in use must fail naming what runs it: %v", dr.Diagnostics)
	}
}

// TestFlinkProject_ReplaceOnlyImmutableFields: Must not: renaming the project or
// editing its settings deletes it with its versions. Only identifier and runtime
// (which the API cannot change) replace it.
func TestFlinkProject_ReplaceOnlyImmutableFields(t *testing.T) {
	want := map[string]bool{"identifier": true, "runtime": true}
	if got := replacingAttributes(t, NewFlinkProject()); !reflect.DeepEqual(got, want) {
		t.Fatalf("replacing attributes = %v, want %v", got, want)
	}
}

// TestFlinkProject_CredentialsSensitive: the store credentials are sensitive, so Terraform never
// prints them in plans or logs. The resource has no CI key attribute at all: the key is created on demand.
func TestFlinkProject_CredentialsSensitive(t *testing.T) {
	var sr resource.SchemaResponse
	NewFlinkProject().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	if _, has := sr.Schema.Attributes["ci_api_key"]; has {
		t.Error("the resource must not return a CI key: it is created on demand, never at creation")
	}
	store := sr.Schema.Attributes["store"].(rschema.SingleNestedAttribute)
	for _, a := range []string{"username", "password", "token"} {
		if !store.Attributes[a].IsSensitive() {
			t.Errorf("store.%s must be sensitive", a)
		}
	}
}

// TestProcessing_PathsAlwaysOrgScoped: Must not: a Flink, Flink project or ClickHouse
// request leaves the configured organization.
func TestProcessing_PathsAlwaysOrgScoped(t *testing.T) {
	ff := newFlinkFake()
	r, st := createFlink(t, ff)
	dr := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr)
	pf := &flinkProjectFake{projects: map[string]map[string]any{}, stores: map[string]map[string]any{}}
	pr := NewFlinkProject()
	configured(t, pr, fakeClient(t, pf.handler()))
	cr := resource.CreateResponse{State: emptyState(t, pr)}
	pr.Create(context.Background(), resource.CreateRequest{Plan: projectPlan(t, pr, ghcr, nil)}, &cr)
	for _, p := range append(ff.paths, pf.paths...) {
		if !strings.HasPrefix(p, "/organizations/acme/") {
			t.Errorf("request outside the organization: %s", p)
		}
	}
}
