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

	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

func init() {
	for _, p := range []*pollBounds{&kafkaConnectPoll, &kafkaReplicatorPoll, &virtualKafkaPoll} {
		p.initial, p.max = time.Millisecond, time.Millisecond
	}
}

// streamingFake answers the Kafka Connect, replicator and virtual Kafka endpoints the
// way the backend does: a POST only saves the object, a reconcile deploys it, and a
// DELETE removes it some polls later. Virtual Kafka is reconciled on its
// fleet-scoped route only.
type streamingFake struct {
	mu           sync.Mutex
	objects      map[string]map[string]any // "<collection>/<identifier>"
	pollsToReady int
	created      map[string]map[string]any // last POST body per collection
	puts         map[string]map[string]any // last PUT body per collection
	reconciled   []string                  // reconcile paths called
	reconcileKey string
	// updateBumps is the keys whose change raises the generation on PUT.
	updateBumps map[string][]string
}

func newStreamingFake() *streamingFake {
	return &streamingFake{objects: map[string]map[string]any{}, created: map[string]map[string]any{}, puts: map[string]map[string]any{}, pollsToReady: 2,
		updateBumps: map[string][]string{"kafka-connect": {"advanced_config"}, "kafka-replicator": {"replication_mode", "source_kafka_id"},
			"virtual-kafka": {"kafka_units", "advanced_config"}}}
}

func (f *streamingFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		reply := func(code int, v any) {
			w.WriteHeader(code)
			if v != nil {
				_ = json.NewEncoder(w).Encode(v)
			}
		}
		rest, ok := strings.CutPrefix(r.URL.Path, "/organizations/acme/")
		if !ok {
			reply(404, nil)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(raw, &in)
		parts := strings.Split(strings.Trim(rest, "/"), "/")
		coll := parts[0]
		// fleet/<fleet>/kafka/kvc/<id>/reconcile/
		if len(parts) == 6 && parts[0] == "fleet" && parts[2] == "kafka" && parts[3] == "kvc" && parts[5] == "reconcile" {
			f.reconciled = append(f.reconciled, rest)
			f.reconcileKey = r.Header.Get("Idempotency-Key")
			o := f.objects["virtual-kafka/"+parts[4]]
			if o == nil || o["fleet"] != parts[1] {
				reply(404, nil)
				return
			}
			o["reconciliation_state"], o["status"], o["_polls"] = "queued", "Provisioning", 0
			reply(202, map[string]any{"status": "queued"})
			return
		}
		switch {
		case len(parts) == 1 && r.Method == http.MethodPost:
			if k := missingRequired(coll, "POST", in); k != "" {
				reply(400, map[string]any{"detail": k + ": This field is required."})
				return
			}
			f.created[coll] = in
			o := f.newObject(coll, in)
			f.objects[coll+"/"+o["identifier"].(string)] = o
			reply(201, o)
		case len(parts) == 3 && parts[2] == "reconcile" && coll != "virtual-kafka":
			f.reconciled = append(f.reconciled, rest)
			f.reconcileKey = r.Header.Get("Idempotency-Key")
			o := f.objects[coll+"/"+parts[1]]
			o["reconciliation_state"], o["status"], o["_polls"] = "queued", "Provisioning", 0
			reply(202, map[string]any{"status": "queued"})
		case len(parts) == 2:
			o, ok := f.objects[coll+"/"+parts[1]]
			if !ok {
				reply(404, nil)
				return
			}
			switch r.Method {
			case http.MethodGet:
				f.advance(coll+"/"+parts[1], o)
				if _, still := f.objects[coll+"/"+parts[1]]; !still {
					reply(404, nil)
					return
				}
				reply(200, o)
			case http.MethodPut:
				if k := missingRequired(coll, "PUT", in); k != "" {
					reply(400, map[string]any{"detail": k + ": This field is required."})
					return
				}
				f.puts[coll] = in
				bump := false
				for k, v := range in {
					if b, _ := json.Marshal(v); string(b) != mustJSON(o[k]) {
						for _, key := range f.updateBumps[coll] {
							bump = bump || key == k
						}
					}
					o[k] = v
				}
				if bump {
					o["generation"] = o["generation"].(int) + 1
					o["reconciliation_state"] = "pending"
				}
				reply(200, o)
			case http.MethodDelete:
				o["desired_lifecycle"], o["status"], o["reconciliation_state"], o["_polls"] = "absent", "Deleting", "queued", 0
				reply(204, nil)
			}
		default:
			reply(404, nil)
		}
	})
}

// planHolder keeps the test plans short: .p is the plan.
type planHolder struct{ p tfsdk.Plan }

// getInt reads an integer attribute from state.
func getInt(t *testing.T, st tfsdk.State, name string) int64 {
	t.Helper()
	var v int64
	if d := st.GetAttribute(context.Background(), pathOf(name), &v); d.HasError() {
		t.Fatal(d)
	}
	return v
}

// required is what the backend's serializers require per method (checked against
// KafkaConnectSerializer, KafkaReplicatorSerializer and VirtualKafkaClusterSerializer).
// A fake that accepted anything let sparse update bodies and the wrong replicator field
// names through.
var requiredFields = map[string]map[string][]string{
	"kafka-connect": {"POST": {"identifier", "provider", "advanced_config", "kafka_id"}, "PUT": {"identifier"}},
	"kafka-replicator": {"POST": {"identifier", "provider", "replication_mode", "lag_alert_threshold", "lag_alert_threshold_unit", "source_secret_path", "destination_secret_path"},
		"PUT": {"identifier", "replication_mode", "lag_alert_threshold", "lag_alert_threshold_unit", "source_secret_path", "destination_secret_path"}},
	"virtual-kafka": {"POST": {"identifier", "kafka_units", "kafka_id"}, "PUT": {"identifier", "kafka_units"}},
}

// missingRequired names a required field the request lacks. A replicator also needs a
// Streamtime cluster or connection details on each side, under the serializer's own names.
func missingRequired(coll, method string, in map[string]any) string {
	for _, k := range requiredFields[coll][method] {
		if _, ok := in[k]; !ok {
			return k
		}
	}
	if coll == "kafka-replicator" {
		for _, side := range []string{"source", "destination"} {
			_, k := in[side+"_kafka"]
			_, c := in[side+"_connection_details"]
			if !k && !c {
				return side + "_kafka"
			}
		}
	}
	if coll == "kafka-connect" && method == "PUT" {
		_, a := in["kafka_id"]
		_, b := in["provider"]
		_, c := in["advanced_config"]
		if !a && !b && !c {
			return "kafka_id"
		}
	}
	return ""
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// newObject is what the backend returns for a saved object, in the response's field names.
func (f *streamingFake) newObject(coll string, in map[string]any) map[string]any {
	o := map[string]any{"generation": 1, "observed_generation": 0, "configuration_state": "valid",
		"desired_lifecycle": "present", "reconciliation_state": "pending", "status": "Pending"}
	for k, v := range in {
		o[k] = v
	}
	switch coll {
	case "kafka-connect":
		o["kafka"], o["fleet"] = in["kafka_id"], "auto-fleet"
		delete(o, "kafka_id")
		if adv, ok := o["advanced_config"].(map[string]any); ok {
			adv["connector_plugin_zips"] = []any{} // a server default
		}
	case "kafka-replicator":
		o["fleet"] = "auto-fleet"
		o["source_initiated_link"] = false
	case "virtual-kafka":
		o["kafka"], o["fleet"] = in["kafka_id"], "prod"
		delete(o, "kafka_id")
		o["produce_bytes_per_second"], o["fetch_bytes_per_second"] = 100, 300
	}
	return o
}

// advance moves an object along after enough polls, as the workflows would.
func (f *streamingFake) advance(key string, o map[string]any) {
	polls, _ := o["_polls"].(int)
	o["_polls"] = polls + 1
	if polls+1 < f.pollsToReady {
		return
	}
	switch {
	case o["desired_lifecycle"] == "absent":
		delete(f.objects, key)
	case o["reconciliation_state"] == "queued":
		o["reconciliation_state"], o["status"], o["observed_generation"] = "succeeded", "Healthy", o["generation"]
		if strings.HasPrefix(key, "virtual-kafka/") {
			o["resources"] = map[string]any{"endpoints": []any{map[string]any{"type": "bootstrap", "address": "vk.example.com:9092"}}}
		}
	}
}

func streamingClient(t *testing.T, f *streamingFake) *client.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// ---- streamtime_kafka_connect -----------------------------------------------

func connectPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) planHolder {
	vals := map[string]tftypes.Value{"identifier": str("conn"), "kafka_id": str("orders"), "connect_provider": str("skc")}
	for k, v := range extra {
		vals[k] = v
	}
	return planHolder{planNull(t, r, vals, "advanced_config")}
}

// TestKafkaConnect_CreateReconcilesAndWaits: saving Kafka Connect does not deploy it, so
// create sends advanced_config even when it is not configured, deploys the saved
// generation with an idempotency key, and returns only when it is ready.
func TestKafkaConnect_CreateReconcilesAndWaits(t *testing.T) {
	f := newStreamingFake()
	r := NewKafkaConnect()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: connectPlan(t, r, nil).p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if _, sent := f.created["kafka-connect"]["advanced_config"]; !sent || f.created["kafka-connect"]["kafka_id"] != "orders" ||
		f.created["kafka-connect"]["provider"] != "skc" {
		t.Fatalf("create sent %v", f.created["kafka-connect"])
	}
	if f.reconcileKey == "" || len(f.reconciled) != 1 || f.reconciled[0] != "kafka-connect/conn/reconcile/" {
		t.Fatalf("create must reconcile once with an Idempotency-Key: %v %q", f.reconciled, f.reconcileKey)
	}
	if getStr(t, cr.State, "status") != "Healthy" || getStr(t, cr.State, "fleet") != "auto-fleet" || getStr(t, cr.State, "kafka_id") != "orders" {
		t.Fatal("create returned before Kafka Connect was ready, or did not read the server's view back")
	}
}

// TestKafkaConnect_ServerDefaultsAreNotDrift: settings Streamtime fills in do not show
// as a change, so the plan after apply is empty.
func TestKafkaConnect_ServerDefaultsAreNotDrift(t *testing.T) {
	f := newStreamingFake()
	r := NewKafkaConnect()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	cfg := `{"chart_config":{"main":{"version":"1.0.0"}}}`
	r.Create(context.Background(), resource.CreateRequest{Plan: connectPlan(t, r, map[string]tftypes.Value{"advanced_config": str(cfg)}).p}, &cr)
	rr := resource.ReadResponse{State: cr.State}
	r.Read(context.Background(), resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "advanced_config") != cfg {
		t.Fatalf("a server default became a diff: %v", rr.Diagnostics)
	}
}

// TestKafkaConnect_ChangeIsSavedWithWarning: Must not replace. A settings change is an
// in-place update, saved with a warning that running Kafka Connect does not change yet.
func TestKafkaConnect_ChangeIsSavedWithWarning(t *testing.T) {
	f := newStreamingFake()
	r := NewKafkaConnect()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: connectPlan(t, r, map[string]tftypes.Value{"advanced_config": str(`{"chart_config":{}}`)}).p}, &cr)
	p := connectPlan(t, r, map[string]tftypes.Value{"id": str("conn"), "advanced_config": str(`{"connector_plugin_zips":[]}`)}).p
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(context.Background(), resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || ur.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("a change must be saved with a not-applied warning: %v", ur.Diagnostics)
	}
	if len(f.reconciled) != 1 {
		t.Fatalf("update must not reconcile (Streamtime cannot apply it): %v", f.reconciled)
	}
}

// ---- streamtime_kafka_replicator --------------------------------------------

func replicatorPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) planHolder {
	vals := map[string]tftypes.Value{"identifier": str("mirror"), "replicator_provider": str("MirrorMaker2"),
		"source_kafka_id": str("east"), "destination_kafka_id": str("west"), "replication_mode": str("active-passive"),
		"source_secret_path": str("builtin:///replicators/mirror/source-secret"), "destination_secret_path": str("builtin:///replicators/mirror/destination-secret"),
		"lag_alert_threshold": tftypes.NewValue(tftypes.Number, 1000), "lag_alert_threshold_unit": str("messages")}
	for k, v := range extra {
		vals[k] = v
	}
	return planHolder{planNull(t, r, vals, "source_connection_details", "destination_connection_details", "topic_whitelist", "topic_blacklist", "groups_whitelist", "groups_blacklist", "link_mode",
		"source_initiated_link", "mirrormaker2_advanced_settings", "cluster_link_advanced_settings")}
}

// TestKafkaReplicator_CreateUsesSerializerFieldNames: the API reads a Streamtime cluster
// from source_kafka / destination_kafka (not the *_id its create schema names) and needs
// both secret paths; the state must hold the configured ids after create.
func TestKafkaReplicator_CreateUsesSerializerFieldNames(t *testing.T) {
	f := newStreamingFake()
	r := NewKafkaReplicator()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: replicatorPlan(t, r, nil).p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	in := f.created["kafka-replicator"]
	if in["source_kafka"] != "east" || in["destination_kafka"] != "west" || in["provider"] != "MirrorMaker2" || in["lag_alert_threshold"] != float64(1000) {
		t.Fatalf("create sent %v", in)
	}
	if _, wrong := in["source_kafka_id"]; wrong {
		t.Fatal("the API does not read source_kafka_id: send source_kafka")
	}
	if _, leaked := in["topic_whitelist"]; leaked {
		t.Fatal("settings that are not configured must be left out, not sent as empty")
	}
	if len(f.reconciled) != 1 || getStr(t, cr.State, "status") != "Healthy" ||
		getStr(t, cr.State, "source_kafka_id") != "east" || getStr(t, cr.State, "destination_kafka_id") != "west" {
		t.Fatalf("create must deploy, wait, and read the clusters back: %v", f.reconciled)
	}
}

// TestKafkaReplicator_ChangeIsSavedWithWarning: a mode change is an in-place update with
// a not-applied warning; it never replaces the replicator.
func TestKafkaReplicator_ChangeIsSavedWithWarning(t *testing.T) {
	f := newStreamingFake()
	r := NewKafkaReplicator()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: replicatorPlan(t, r, nil).p}, &cr)
	p := replicatorPlan(t, r, map[string]tftypes.Value{"id": str("mirror"), "replication_mode": str("active-active")}).p
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(context.Background(), resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || ur.Diagnostics.WarningsCount() != 1 || f.objects["kafka-replicator/mirror"]["replication_mode"] != "active-active" {
		t.Fatalf("a change must be saved with a not-applied warning: %v", ur.Diagnostics)
	}
}

// ---- streamtime_virtual_kafka -----------------------------------------------

func virtualPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) planHolder {
	vals := map[string]tftypes.Value{"identifier": str("vk"), "kafka_id": str("orders"), "kafka_units": num(2),
		"advanced_config": str(`{"oauth":{"client_id":"c","client_secret":"s"}}`)}
	for k, v := range extra {
		vals[k] = v
	}
	return planHolder{planNull(t, r, vals)}
}

// TestVirtualKafka_CreateReconcilesOnTheFleetRoute: only the fleet-scoped route has the
// reconcile call, so create deploys through the fleet the server placed the cluster on,
// and returns when it is ready.
func TestVirtualKafka_CreateReconcilesOnTheFleetRoute(t *testing.T) {
	f := newStreamingFake()
	r := NewVirtualKafka()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: virtualPlan(t, r, nil).p}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	if len(f.reconciled) != 1 || f.reconciled[0] != "fleet/prod/kafka/kvc/vk/reconcile/" || f.reconcileKey == "" {
		t.Fatalf("create must reconcile on the fleet route with an Idempotency-Key: %v", f.reconciled)
	}
	if getStr(t, cr.State, "status") != "Healthy" || !strings.Contains(getStr(t, cr.State, "resources"), "vk.example.com:9092") {
		t.Fatal("create returned before the cluster was ready, or its endpoints were not read back")
	}
}

// TestVirtualKafka_CapacityChangeDeploysInPlace: unlike Kafka, a change is deployed.
// apply raises the capacity in place, reconciles the new generation and waits for it.
func TestVirtualKafka_CapacityChangeDeploysInPlace(t *testing.T) {
	f := newStreamingFake()
	r := NewVirtualKafka()
	configured(t, r, streamingClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: virtualPlan(t, r, nil).p}, &cr)
	p := virtualPlan(t, r, map[string]tftypes.Value{"id": str("vk"), "kafka_units": num(5)}).p
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(context.Background(), resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || ur.Diagnostics.WarningsCount() != 0 {
		t.Fatalf("a capacity change must be deployed without a warning: %v", ur.Diagnostics)
	}
	if len(f.reconciled) != 2 || f.objects["virtual-kafka/vk"]["kafka_units"] != float64(5) ||
		getInt(t, ur.State, "observed_generation") != 2 {
		t.Fatalf("update must deploy generation 2: %v", f.reconciled)
	}
}

// ---- all three --------------------------------------------------------------

// TestKafkaWorkloads_ReplaceOnlyImmutableFields: Must not. A change Streamtime can save in
// place must never delete the object. Only these attributes force replacement.
func TestKafkaWorkloads_ReplaceOnlyImmutableFields(t *testing.T) {
	cases := []struct {
		name string
		r    func() resource.Resource
		want map[string]bool
	}{
		{"kafka_connect", NewKafkaConnect, map[string]bool{"identifier": true, "kafka_id": true, "connect_provider": true}},
		{"kafka_replicator", NewKafkaReplicator, map[string]bool{"identifier": true, "replicator_provider": true}},
		{"virtual_kafka", NewVirtualKafka, map[string]bool{"identifier": true, "kafka_id": true}},
	}
	for _, c := range cases {
		var sr resource.SchemaResponse
		c.r().Schema(context.Background(), resource.SchemaRequest{}, &sr)
		for name, a := range sr.Schema.Attributes {
			replaces := false
			switch at := a.(type) {
			case rschema.StringAttribute:
				replaces = replacesString(t, c.r(), name, types.StringValue("old"), types.StringValue("new"))
			case rschema.Int64Attribute:
				replaces = int64Change(at.PlanModifiers, 1, 2).RequiresReplace
			}
			if replaces != c.want[name] {
				t.Errorf("%s.%s: replaces=%v, want %v", c.name, name, replaces, c.want[name])
			}
		}
	}
}

// TestKafkaWorkloads_DeleteWaitsUntilGone: destroy returns only once Streamtime has
// removed the object, and an object that is already gone is success.
func TestKafkaWorkloads_DeleteWaitsUntilGone(t *testing.T) {
	for name, mk := range map[string]func() resource.Resource{
		"kafka-connect": NewKafkaConnect, "kafka-replicator": NewKafkaReplicator, "virtual-kafka": NewVirtualKafka} {
		f := newStreamingFake()
		f.pollsToReady = 3
		f.objects[name+"/x"] = map[string]any{"identifier": "x", "generation": 1, "observed_generation": 1,
			"reconciliation_state": "succeeded", "status": "Healthy", "desired_lifecycle": "present"}
		r := mk()
		configured(t, r, streamingClient(t, f))
		st := emptyState(t, r)
		if err := st.SetAttribute(context.Background(), pathOf("id"), "x"); err != nil {
			t.Fatal(err)
		}
		dr := resource.DeleteResponse{State: st}
		r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr)
		if dr.Diagnostics.HasError() || len(f.objects) != 0 {
			t.Fatalf("%s: destroy returned before the object was removed: %v", name, dr.Diagnostics)
		}
		dr2 := resource.DeleteResponse{State: st}
		r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr2)
		if dr2.Diagnostics.HasError() {
			t.Fatalf("%s: destroying an object that is already gone must succeed: %v", name, dr2.Diagnostics)
		}
	}
}

// TestKafkaWorkloads_ImportAndGone: an import adopts the server's settings, and an
// object deleted outside Terraform drops from state.
func TestKafkaWorkloads_ImportAndGone(t *testing.T) {
	f := newStreamingFake()
	f.objects["virtual-kafka/old"] = map[string]any{"identifier": "old", "kafka": "orders", "fleet": "prod", "kafka_units": 4,
		"advanced_config": map[string]any{"oauth": map[string]any{"client_id": "c"}}, "generation": 3, "observed_generation": 3,
		"reconciliation_state": "succeeded", "status": "Healthy", "produce_bytes_per_second": 1, "fetch_bytes_per_second": 2}
	r := NewVirtualKafka()
	configured(t, r, streamingClient(t, f))
	ctx := context.Background()
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "old"}, &imp)
	rr := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "kafka_id") != "orders" || getInt(t, rr.State, "kafka_units") != 4 {
		t.Fatalf("import did not adopt the server's settings: %v", rr.Diagnostics)
	}
	delete(f.objects, "virtual-kafka/old")
	rr2 := resource.ReadResponse{State: rr.State}
	r.Read(ctx, resource.ReadRequest{State: rr.State}, &rr2)
	if rr2.Diagnostics.HasError() || !rr2.State.Raw.IsNull() {
		t.Fatal("an object deleted outside Terraform must drop from state")
	}
}

// TestKafkaWorkloads_CredentialAttributesAreSensitive: Must not (risk: secrets in plan output,
// state diffs and logs). Settings that hold an OAuth client secret or client properties
// are sensitive.
func TestKafkaWorkloads_CredentialAttributesAreSensitive(t *testing.T) {
	for name, c := range map[string]struct {
		r     func() resource.Resource
		attrs []string
	}{
		"kafka_connect": {NewKafkaConnect, []string{"advanced_config"}},
		"virtual_kafka": {NewVirtualKafka, []string{"advanced_config", "resources"}},
	} {
		var sr resource.SchemaResponse
		c.r().Schema(context.Background(), resource.SchemaRequest{}, &sr)
		for _, a := range c.attrs {
			if !sr.Schema.Attributes[a].IsSensitive() {
				t.Errorf("%s.%s must be sensitive", name, a)
			}
		}
	}
}

// TestKafkaWorkloads_UpdatesSendTheRequiredFields: Must work. The API validates the full
// representation on update, so each update body carries the identifier (and a replicator its
// secret paths); the fake refuses a body without them, as the real serializers do. A change
// is accepted for all three.
func TestKafkaWorkloads_UpdatesSendTheRequiredFields(t *testing.T) {
	f := newStreamingFake()
	ctx := context.Background()
	for _, c := range []struct {
		coll   string
		r      func() resource.Resource
		create func(*testing.T, resource.Resource) planHolder
		change map[string]tftypes.Value
		id     string
	}{
		{"kafka-connect", NewKafkaConnect, func(t *testing.T, r resource.Resource) planHolder {
			return connectPlan(t, r, map[string]tftypes.Value{"advanced_config": str(`{"chart_config":{}}`)})
		}, map[string]tftypes.Value{"advanced_config": str(`{"connector_plugin_zips":[]}`)}, "conn"},
		{"kafka-replicator", NewKafkaReplicator, func(t *testing.T, r resource.Resource) planHolder { return replicatorPlan(t, r, nil) },
			map[string]tftypes.Value{"replication_mode": str("active-active")}, "mirror"},
		{"virtual-kafka", NewVirtualKafka, func(t *testing.T, r resource.Resource) planHolder { return virtualPlan(t, r, nil) },
			map[string]tftypes.Value{"kafka_units": num(5)}, "vk"},
	} {
		r := c.r()
		configured(t, r, streamingClient(t, f))
		cr := resource.CreateResponse{State: emptyState(t, r)}
		r.Create(ctx, resource.CreateRequest{Plan: c.create(t, r).p}, &cr)
		if cr.Diagnostics.HasError() {
			t.Fatalf("%s create: %v", c.coll, cr.Diagnostics)
		}
		change := map[string]tftypes.Value{"id": str(c.id)}
		for k, v := range c.change {
			change[k] = v
		}
		// rebuild the plan with the change merged over the create plan's values
		var plan planHolder
		switch c.coll {
		case "kafka-connect":
			plan = connectPlan(t, r, change)
		case "kafka-replicator":
			plan = replicatorPlan(t, r, change)
		default:
			plan = virtualPlan(t, r, change)
		}
		ur := resource.UpdateResponse{State: cr.State}
		r.Update(ctx, resource.UpdateRequest{Plan: plan.p, State: cr.State}, &ur)
		if ur.Diagnostics.HasError() {
			t.Errorf("%s: an update the API would accept was refused: %v", c.coll, ur.Diagnostics)
		}
		if f.puts[c.coll]["identifier"] != c.id {
			t.Errorf("%s: the update must send the identifier: %v", c.coll, f.puts[c.coll])
		}
	}
}

// TestKafkaReplicator_SecretPathsAreRequired: Must not (risk: a plan that can never apply).
// Streamtime refuses a replicator without both secret paths, so the schema requires them
// instead of letting the failure surface at apply time.
func TestKafkaReplicator_SecretPathsAreRequired(t *testing.T) {
	var sr resource.SchemaResponse
	NewKafkaReplicator().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	for _, a := range []string{"source_secret_path", "destination_secret_path"} {
		if !sr.Schema.Attributes[a].IsRequired() {
			t.Errorf("%s must be required", a)
		}
	}
}
