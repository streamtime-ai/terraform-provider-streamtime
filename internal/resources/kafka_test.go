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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

func init() { kafkaPoll.initial, kafkaPoll.max = time.Millisecond, time.Millisecond }

// kafkaFake answers the Kafka cluster and topic endpoints the way the backend does:
// a cluster is saved by POST, deployed only after reconcile and removed some time
// after DELETE; topics are synchronous, and deleting one needs its name in the body.
type kafkaFake struct {
	mu            sync.Mutex
	clusters      map[string]map[string]any
	topics        map[string]map[string]any // "<kafka>/<topic>"
	pollsToReady  int
	failReconcile bool
	created       map[string]any // last POST /kafka/ body
	reconcileKey  string
	reconcileGen  float64
	deleteBody    map[string]any // last topic DELETE body
	patches       []map[string]any
	paths         []string
}

func newKafkaFake() *kafkaFake {
	return &kafkaFake{clusters: map[string]map[string]any{}, topics: map[string]map[string]any{}, pollsToReady: 2}
}

// topicDefaults are the settings the cluster reports for every topic.
var topicDefaults = map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"}

func topicBody(t map[string]any) map[string]any {
	set, _ := t["_configs"].(map[string]string)
	var configs []any
	for k, v := range topicDefaults {
		val, overridden := set[k]
		if !overridden {
			val = v
		}
		configs = append(configs, map[string]any{"name": k, "value": val, "source": "x", "is_default": !overridden, "is_sensitive": false})
	}
	for k, v := range set {
		if _, known := topicDefaults[k]; !known {
			configs = append(configs, map[string]any{"name": k, "value": v, "source": "x", "is_default": false, "is_sensitive": false})
		}
	}
	return map[string]any{"name": t["name"], "partitions": t["partitions"], "replication_factor": t["replication_factor"],
		"internal": false, "configs": configs}
}

func (f *kafkaFake) handler() http.Handler {
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
		rest, ok := strings.CutPrefix(r.URL.Path, "/organizations/acme/kafka/")
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
			if _, sent := in["advanced_config"]; !sent {
				in["configuration_state"] = "incomplete" // a draft: never deployed
			} else {
				in["configuration_state"] = "valid"
			}
			if _, set := in["fleet"]; !set {
				in["fleet"] = "auto-fleet"
			}
			if _, set := in["tenancy_mode"]; !set {
				in["tenancy_mode"] = "Shared"
			}
			if adv, ok := in["advanced_config"].(map[string]any); ok {
				adv["chart_config"] = map[string]any{"main": map[string]any{"version": "1.0.0"}} // a server default
			}
			delete(in, "retention_mode")       // write-only
			delete(in, "alert_webhook_secret") // write-only
			in["generation"], in["observed_generation"] = 1, 0
			in["status"], in["reconciliation_state"], in["desired_lifecycle"] = "Pending", "pending", "present"
			in["spec"] = nil
			f.clusters[in["identifier"].(string)] = in
			reply(201, in)
		case len(parts) == 2 && parts[1] == "reconcile":
			k := f.clusters[parts[0]]
			if k["configuration_state"] != "valid" {
				reply(409, map[string]any{"detail": "The cluster's configuration is incomplete."})
				return
			}
			f.reconcileKey = r.Header.Get("Idempotency-Key")
			f.reconcileGen, _ = in["generation"].(float64)
			k["reconciliation_state"], k["status"], k["_polls"] = "queued", "Provisioning", 0
			reply(202, map[string]any{"status": "queued"})
		case len(parts) == 1:
			k, ok := f.clusters[parts[0]]
			if !ok {
				reply(404, nil)
				return
			}
			switch r.Method {
			case http.MethodGet:
				f.advance(parts[0], k)
				if _, still := f.clusters[parts[0]]; !still {
					reply(404, nil)
					return
				}
				reply(200, k)
			case http.MethodPut:
				before := k["kafka_units"]
				for key, v := range in {
					if key != "alert_webhook_secret" && key != "retention_mode" {
						k[key] = v
					}
				}
				if adv, ok := k["advanced_config"].(map[string]any); ok {
					adv["chart_config"] = map[string]any{"main": map[string]any{"version": "1.0.0"}}
				}
				if before != k["kafka_units"] {
					k["generation"] = k["generation"].(int) + 1
					k["reconciliation_state"] = "pending"
				}
				reply(200, k)
			case http.MethodDelete:
				k["desired_lifecycle"], k["status"], k["reconciliation_state"], k["_polls"] = "absent", "Deleting", "queued", 0
				reply(202, map[string]any{"status": "queued"})
			}
		case len(parts) >= 2 && parts[1] == "topics":
			if _, ok := f.clusters[parts[0]]; !ok {
				reply(404, map[string]any{"detail": "Kafka cluster not found.", "code": "not_found"})
				return
			}
			if len(parts) == 2 && r.Method == http.MethodPost {
				t := map[string]any{"name": in["name"], "partitions": in["partitions"], "replication_factor": in["replication_factor"],
					"_configs": map[string]string{}}
				if c, ok := in["configs"].(map[string]any); ok {
					for k, v := range c {
						t["_configs"].(map[string]string)[k] = v.(string)
					}
				}
				f.topics[parts[0]+"/"+in["name"].(string)] = t
				reply(201, topicBody(t))
				return
			}
			key := parts[0] + "/" + parts[2]
			t, ok := f.topics[key]
			switch {
			case r.Method == http.MethodDelete:
				f.deleteBody = in
				if in["name"] != parts[2] {
					reply(400, map[string]any{"detail": "The request body is not valid."})
					return
				}
				delete(f.topics, key)
				reply(200, map[string]any{"name": parts[2], "deleted": ok})
			case !ok:
				reply(404, map[string]any{"detail": "Topic not found.", "code": "topic_not_found"})
			case r.Method == http.MethodGet:
				reply(200, topicBody(t))
			case r.Method == http.MethodPatch:
				f.patches = append(f.patches, in)
				if p, ok := in["partitions"]; ok {
					t["partitions"] = p
				}
				if c, ok := in["configs"].(map[string]any); ok {
					set, _ := c["set"].(map[string]any)
					for k, v := range set {
						t["_configs"].(map[string]string)[k] = v.(string)
					}
					del, _ := c["delete"].([]any)
					for _, k := range del {
						delete(t["_configs"].(map[string]string), k.(string))
					}
				}
				reply(200, topicBody(t))
			}
		default:
			reply(404, nil)
		}
	})
}

// advance moves a cluster along after enough polls, as the workflows would.
func (f *kafkaFake) advance(id string, k map[string]any) {
	polls, _ := k["_polls"].(int)
	k["_polls"] = polls + 1
	if polls+1 < f.pollsToReady {
		return
	}
	switch {
	case k["desired_lifecycle"] == "absent":
		delete(f.clusters, id)
	case k["reconciliation_state"] == "queued" && f.failReconcile:
		k["reconciliation_state"], k["status"] = "failed", "Error"
	case k["reconciliation_state"] == "queued":
		k["reconciliation_state"], k["status"], k["observed_generation"] = "succeeded", "Healthy", k["generation"]
		k["spec"] = map[string]any{"bootstrapServers": "orders.kafka.example.com:9092"}
	}
}

func kafkaClient(t *testing.T, f *kafkaFake) *client.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func num(n int64) tftypes.Value { return tftypes.NewValue(tftypes.Number, n) }

func kafkaPlan(t *testing.T, r resource.Resource, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{
		"identifier": str("orders"), "workload": str("strimzi"), "kafka_units": num(3), "retention_mode": str("Finite"),
		"alert_webhook_secret": str("whsec"),
	}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "advanced_config", "metadata")
}

func createKafka(t *testing.T, f *kafkaFake, extra map[string]tftypes.Value) (resource.Resource, tfsdk.State) {
	t.Helper()
	r := NewKafka()
	configured(t, r, kafkaClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: kafkaPlan(t, r, extra)}, &cr)
	if cr.Diagnostics.HasError() {
		t.Fatal(cr.Diagnostics)
	}
	return r, cr.State
}

// int64Change runs Int64 plan modifiers on a change from old to new for an object
// that already exists (RequiresReplace ignores a create, which has no state).
func int64Change(mods []planmodifier.Int64, old, new int64) planmodifier.Int64Response {
	live := tfsdk.State{Raw: tftypes.NewValue(tftypes.Object{AttributeTypes: map[string]tftypes.Type{}}, map[string]tftypes.Value{})}
	resp := planmodifier.Int64Response{PlanValue: types.Int64Value(new)}
	for _, m := range mods {
		m.PlanModifyInt64(context.Background(), planmodifier.Int64Request{State: live, Plan: tfsdk.Plan{Raw: live.Raw},
			StateValue: types.Int64Value(old), PlanValue: types.Int64Value(new), ConfigValue: types.Int64Value(new)}, &resp)
	}
	return resp
}

// ---- streamtime_kafka -------------------------------------------------------

// TestKafka_CreateReconcilesAndWaits: saving a cluster does not deploy it, so create
// sends a complete request (advanced_config always present, or Streamtime keeps a
// draft), starts the deployment with the saved generation and an idempotency key,
// and returns only when the cluster is ready.
func TestKafka_CreateReconcilesAndWaits(t *testing.T) {
	f := newKafkaFake()
	_, st := createKafka(t, f, nil)
	if _, sent := f.created["advanced_config"]; !sent {
		t.Fatal("create must send advanced_config even when it is not configured")
	}
	if f.reconcileKey == "" || f.reconcileGen != 1 {
		t.Fatalf("create must reconcile generation 1 with an Idempotency-Key (key=%q gen=%v)", f.reconcileKey, f.reconcileGen)
	}
	if getStr(t, st, "status") != "Healthy" || getStr(t, st, "reconciliation_state") != "succeeded" {
		t.Fatal("create returned before the cluster was ready")
	}
	if getStr(t, st, "fleet") != "auto-fleet" || getStr(t, st, "spec") != `{"bootstrapServers":"orders.kafka.example.com:9092"}` {
		t.Fatalf("the chosen fleet and the running cluster's spec must be read back: %s %s", getStr(t, st, "fleet"), getStr(t, st, "spec"))
	}
	if getStr(t, st, "retention_mode") != "Finite" || getStr(t, st, "alert_webhook_secret") != "whsec" {
		t.Fatal("settings Streamtime never returns must stay as configured")
	}
}

// TestKafka_ServerDefaultsAreNotDrift: the advanced settings Streamtime fills in do not
// show as a change, so the plan after apply is empty.
func TestKafka_ServerDefaultsAreNotDrift(t *testing.T) {
	f := newKafkaFake()
	r, st := createKafka(t, f, map[string]tftypes.Value{"advanced_config": str(`{"listeners":{"external":true}}`)})
	rr := resource.ReadResponse{State: st}
	r.Read(context.Background(), resource.ReadRequest{State: st}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "advanced_config") != `{"listeners":{"external":true}}` {
		t.Fatalf("a server default became a diff: %v", rr.Diagnostics)
	}
}

// TestKafka_FailedCreateIsAnErrorAndStaysInState: a failed deployment fails the apply,
// but the cluster stays in state so Terraform replaces it rather than losing it.
func TestKafka_FailedCreateIsAnErrorAndStaysInState(t *testing.T) {
	f := newKafkaFake()
	f.failReconcile = true
	r := NewKafka()
	configured(t, r, kafkaClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: kafkaPlan(t, r, nil)}, &cr)
	if !cr.Diagnostics.HasError() {
		t.Fatal("a failed deployment must fail the apply")
	}
	if cr.State.Raw.IsNull() || getStr(t, cr.State, "id") != "orders" {
		t.Fatal("the cluster exists in Streamtime and must stay in state")
	}
}

// TestKafka_CapacityChangeIsInPlaceAndWarns: a capacity change is an in-place update
// (never a replace, which would delete the cluster's data). It is saved, with a
// warning that the running cluster does not change yet.
func TestKafka_CapacityChangeIsInPlaceAndWarns(t *testing.T) {
	f := newKafkaFake()
	r, st := createKafka(t, f, nil)
	p := kafkaPlan(t, r, map[string]tftypes.Value{"id": str("orders"), "kafka_units": num(6), "generation": num(1)})
	ur := resource.UpdateResponse{State: st}
	r.Update(context.Background(), resource.UpdateRequest{Plan: p, State: st}, &ur)
	if ur.Diagnostics.HasError() || ur.Diagnostics.WarningsCount() != 1 {
		t.Fatalf("a capacity change must be saved with a not-applied warning: %v", ur.Diagnostics)
	}
	if f.clusters["orders"]["kafka_units"] != float64(6) {
		t.Fatalf("capacity not saved: %v", f.clusters["orders"]["kafka_units"])
	}
}

// TestKafka_ReplaceOnlyImmutableFields: Must not: a change that Streamtime can save in
// place destroys the cluster. Only identifier, workload and fleet force replacement.
func TestKafka_ReplaceOnlyImmutableFields(t *testing.T) {
	var sr resource.SchemaResponse
	NewKafka().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	want := map[string]bool{"identifier": true, "workload": true, "fleet": true}
	for name, a := range sr.Schema.Attributes {
		replaces := false
		switch at := a.(type) {
		case rschema.StringAttribute:
			replaces = replacesString(t, NewKafka(), name, types.StringValue("old"), types.StringValue("new"))
		case rschema.Int64Attribute:
			replaces = int64Change(at.PlanModifiers, 1, 2).RequiresReplace
		}
		if replaces != want[name] {
			t.Errorf("kafka.%s: replaces=%v, want %v", name, replaces, want[name])
		}
	}
}

// TestKafka_DeleteWaitsUntilGone: destroy returns only once Streamtime has removed the
// cluster, and a cluster that is already gone is success.
func TestKafka_DeleteWaitsUntilGone(t *testing.T) {
	f := newKafkaFake()
	f.pollsToReady = 3
	r, st := createKafka(t, f, nil)
	dr := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr)
	if dr.Diagnostics.HasError() || len(f.clusters) != 0 {
		t.Fatalf("destroy returned before the cluster was removed: %v", dr.Diagnostics)
	}
	dr2 := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("destroying a cluster that is already gone must succeed: %v", dr2.Diagnostics)
	}
}

// TestKafka_ImportAdoptsServerSettings: an import has nothing configured, so the
// server's settings are adopted, and a cluster deleted outside Terraform drops from state.
func TestKafka_ImportAdoptsServerSettings(t *testing.T) {
	f := newKafkaFake()
	f.clusters["legacy"] = map[string]any{"identifier": "legacy", "workload": "cfk", "fleet": "prod", "kafka_units": 4,
		"tags": []any{map[string]any{"key": "team", "value": "payments"}}, "advanced_config": map[string]any{"x": 1},
		"generation": 2, "observed_generation": 2, "reconciliation_state": "succeeded", "status": "Healthy"}
	r := NewKafka()
	configured(t, r, kafkaClient(t, f))
	ctx := context.Background()
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "legacy"}, &imp)
	rr := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "workload") != "cfk" || getStr(t, rr.State, "advanced_config") != `{"x":1}` {
		t.Fatalf("import did not adopt the server's settings: %v", rr.Diagnostics)
	}
	var tags map[string]string
	rr.State.GetAttribute(ctx, pathOf("tags"), &tags)
	if tags["team"] != "payments" {
		t.Fatalf("tags: %v", tags)
	}

	delete(f.clusters, "legacy")
	rr2 := resource.ReadResponse{State: rr.State}
	r.Read(ctx, resource.ReadRequest{State: rr.State}, &rr2)
	if rr2.Diagnostics.HasError() || !rr2.State.Raw.IsNull() {
		t.Fatal("a cluster deleted outside Terraform must drop from state")
	}
}

// ---- streamtime_kafka_topic -------------------------------------------------

func topicPlan(t *testing.T, r resource.Resource, partitions int64, cfg map[string]string) tfsdk.Plan {
	vals := map[string]tftypes.Value{"kafka": str("orders"), "name": str("payments"), "partitions": num(partitions),
		"replication_factor": num(3)}
	if cfg != nil {
		m := map[string]tftypes.Value{}
		for k, v := range cfg {
			m[k] = str(v)
		}
		vals["config"] = tftypes.NewValue(tftypes.Map{ElementType: tftypes.String}, m)
	}
	return planNull(t, r, vals, "config")
}

func topicConfig(t *testing.T, st tfsdk.State) map[string]string {
	t.Helper()
	var out map[string]string
	st.GetAttribute(context.Background(), pathOf("config"), &out)
	return out
}

// TestKafkaTopic_Lifecycle: create, read, update (more partitions, a changed and a
// removed setting, in place), import and delete work. Only configured settings are
// tracked, so the cluster's defaults are not a diff.
func TestKafkaTopic_Lifecycle(t *testing.T) {
	f := newKafkaFake()
	f.clusters["orders"] = map[string]any{"identifier": "orders"}
	r := NewKafkaTopic()
	configured(t, r, kafkaClient(t, f))
	ctx := context.Background()

	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: topicPlan(t, r, 6, map[string]string{"retention.ms": "86400000", "min.insync.replicas": "2"})}, &cr)
	if cr.Diagnostics.HasError() || getStr(t, cr.State, "id") != "orders/payments" {
		t.Fatalf("create: %v", cr.Diagnostics)
	}
	if got := topicConfig(t, cr.State); len(got) != 2 || got["retention.ms"] != "86400000" {
		t.Fatalf("only configured settings may be in state (cluster defaults would be a diff): %v", got)
	}

	p := topicPlan(t, r, 8, map[string]string{"retention.ms": "3600000"})
	p.SetAttribute(ctx, pathOf("id"), "orders/payments")
	ur := resource.UpdateResponse{State: cr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: cr.State}, &ur)
	if ur.Diagnostics.HasError() || len(f.patches) != 1 {
		t.Fatalf("update must be one in-place change: %v %v", ur.Diagnostics, f.patches)
	}
	patch, _ := json.Marshal(f.patches[0])
	if string(patch) != `{"configs":{"delete":["min.insync.replicas"],"set":{"retention.ms":"3600000"}},"partitions":8}` {
		t.Fatalf("update sent %s", patch)
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "orders/payments"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "kafka") != "orders" {
		t.Fatalf("import: %v", ir.Diagnostics)
	}
	if got := topicConfig(t, ir.State); len(got) != 1 || got["retention.ms"] != "3600000" {
		t.Fatalf("import must adopt the settings that differ from the cluster default, and only those: %v", got)
	}

	dr := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr)
	if dr.Diagnostics.HasError() || len(f.topics) != 0 || f.deleteBody["name"] != "payments" {
		t.Fatalf("delete must confirm with the topic name: %v %v", dr.Diagnostics, f.deleteBody)
	}
	dr2 := resource.DeleteResponse{State: ur.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur.State}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("deleting a topic that is already gone must succeed: %v", dr2.Diagnostics)
	}
	rr := resource.ReadResponse{State: ur.State}
	r.Read(ctx, resource.ReadRequest{State: ur.State}, &rr)
	if !rr.State.Raw.IsNull() {
		t.Fatal("a topic deleted outside Terraform must drop from state")
	}
}

// TestKafkaTopic_PartitionsCannotShrink: Must not: lowering partitions plans a replace,
// which would delete the topic's messages. It is an error instead; raising is in place.
func TestKafkaTopic_PartitionsCannotShrink(t *testing.T) {
	var sr resource.SchemaResponse
	NewKafkaTopic().Schema(context.Background(), resource.SchemaRequest{}, &sr)
	mods := sr.Schema.Attributes["partitions"].(rschema.Int64Attribute).PlanModifiers
	if down := int64Change(mods, 6, 3); !down.Diagnostics.HasError() || down.RequiresReplace {
		t.Fatalf("lowering partitions must be an error, never a replace: %+v", down)
	}
	if up := int64Change(mods, 6, 12); up.Diagnostics.HasError() || up.RequiresReplace {
		t.Fatalf("raising partitions must be in place: %+v", up)
	}
	rf := sr.Schema.Attributes["replication_factor"].(rschema.Int64Attribute).PlanModifiers
	if !int64Change(rf, 3, 1).RequiresReplace {
		t.Fatal("the API cannot change the replication factor, so a change must replace the topic")
	}
}

// TestKafkaTopic_ImportIDNamesTheFormat: a malformed import ID says what to pass.
func TestKafkaTopic_ImportIDNamesTheFormat(t *testing.T) {
	r := NewKafkaTopic()
	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(context.Background(), resource.ImportStateRequest{ID: "payments"}, &imp)
	if !imp.Diagnostics.HasError() || !strings.Contains(imp.Diagnostics[0].Detail(), "<kafka>/<topic name>") {
		t.Fatalf("import of a bare name: %v", imp.Diagnostics)
	}
}

// TestKafka_PathsAlwaysOrgScoped: Must not: a Kafka or topic request leaves the
// configured organization.
func TestKafka_PathsAlwaysOrgScoped(t *testing.T) {
	f := newKafkaFake()
	r, st := createKafka(t, f, nil)
	dr := resource.DeleteResponse{State: st}
	r.Delete(context.Background(), resource.DeleteRequest{State: st}, &dr)
	for _, p := range f.paths {
		if !strings.HasPrefix(p, "/organizations/acme/") {
			t.Errorf("request outside the organization: %s", p)
		}
	}
}

// ---- streamtime_kafka_connection_configuration ------------------------------

// TestKafkaConnectionConfiguration_ReadsAndHidesCredentials: the data source reads
// the connection settings, and the attributes holding credentials are sensitive.
func TestKafkaConnectionConfiguration_ReadsAndHidesCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/organizations/acme/kafka/vm-cluster/connection-configuration/" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(`{"client_properties":"bootstrap.servers=a:9092\nsasl.jaas.config=secret","ca_certificate":"PEM",
			"ca_filename":"ca.pem","control_center_username":"admin","control_center_password":"pw"}`))
	}))
	t.Cleanup(srv.Close)
	c, _ := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	d := NewKafkaConnectionConfigurationDataSource()
	st := readData(t, d, c, map[string]tftypes.Value{"kafka": str("vm-cluster")})
	if getStr(t, st, "ca_filename") != "ca.pem" || getStr(t, st, "control_center_password") != "pw" {
		t.Fatal("connection configuration not read")
	}
	for _, name := range []string{"client_properties", "control_center_password"} {
		if !st.Schema.GetAttributes()[name].IsSensitive() {
			t.Errorf("%s holds credentials and must be sensitive", name)
		}
	}
}
