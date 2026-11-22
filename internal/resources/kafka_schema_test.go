package resources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
)

type fakeVersion struct {
	id, version int
	schema      string
	deleted     bool
}

type fakeSubject struct {
	versions    []*fakeVersion
	level       string // "" inherits the registry default
	softDeleted bool
}

// schemaFake answers the schema endpoints the way the backend does. Like a real
// registry it stores schemas in its own formatting (compact JSON), gives an
// identical schema the version it already has, and removes permanently only what
// is soft-deleted.
type schemaFake struct {
	mu       sync.Mutex
	subjects map[string]*fakeSubject
	nextID   int
	calls    []string // "<METHOD> <subject>/<rest>?<query>"
	// strict refuses a new schema for a subject with live versions unless the
	// subject's own level is NONE, as a registry does for an incompatible schema.
	strict bool
}

func newSchemaFake() *schemaFake {
	return &schemaFake{subjects: map[string]*fakeSubject{}, nextID: 100}
}

func compact(schema string) string {
	var v any
	if json.Unmarshal([]byte(schema), &v) != nil {
		return schema
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func (f *schemaFake) handler() http.Handler {
	const prefix = "/organizations/acme/kafka/orders/schemas/subjects/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		reply := func(code int, v any) {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(v)
		}
		notFound := func() { reply(404, map[string]any{"detail": "Subject not found.", "code": "subject_not_found"}) }
		escaped, ok := strings.CutPrefix(r.URL.EscapedPath(), prefix)
		if !ok {
			notFound()
			return
		}
		seg, rest, _ := strings.Cut(escaped, "/")
		subject, _ := url.PathUnescape(seg)
		f.calls = append(f.calls, r.Method+" "+subject+"/"+rest+"?"+r.URL.RawQuery)
		s := f.subjects[subject]
		live := func() []*fakeVersion {
			var out []*fakeVersion
			if s != nil && !s.softDeleted {
				for _, v := range s.versions {
					if !v.deleted {
						out = append(out, v)
					}
				}
			}
			return out
		}
		switch {
		case r.Method == "POST" && rest == "versions/":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			text := compact(body["schema"])
			if s == nil || s.softDeleted {
				s = &fakeSubject{}
				f.subjects[subject] = s
			}
			for _, v := range live() {
				if v.schema == text {
					reply(200, map[string]any{"subject": subject, "id": v.id, "version": v.version})
					return
				}
			}
			if f.strict && len(live()) > 0 && s.level != "NONE" {
				reply(409, map[string]any{"detail": "The schema is not compatible with the subject's level.", "code": "schema_incompatible"})
				return
			}
			f.nextID++
			v := &fakeVersion{id: f.nextID, version: len(s.versions) + 1, schema: text}
			s.versions = append(s.versions, v)
			reply(200, map[string]any{"subject": subject, "id": v.id, "version": v.version})
		case r.Method == "GET" && rest == "versions/latest/":
			l := live()
			if len(l) == 0 {
				notFound()
				return
			}
			v := l[len(l)-1]
			reply(200, map[string]any{"subject": subject, "version": v.version, "id": v.id, "schema_type": "avro",
				"schema": v.schema, "references": []any{}})
		case rest == "config/":
			if s == nil {
				s = &fakeSubject{}
				f.subjects[subject] = s
			}
			switch r.Method {
			case "PUT":
				var body map[string]string
				_ = json.NewDecoder(r.Body).Decode(&body)
				s.level = body["compatibility"]
			case "DELETE":
				s.level = ""
			}
			if s.level == "" {
				reply(200, map[string]any{"subject": subject, "compatibility": "BACKWARD", "inherited": true})
				return
			}
			reply(200, map[string]any{"subject": subject, "compatibility": s.level, "inherited": false})
		case r.Method == "DELETE" && rest == "":
			if r.URL.Query().Get("confirm") != "true" {
				reply(400, map[string]any{"detail": "Pass confirm=true.", "code": "invalid_request"})
				return
			}
			permanent := r.URL.Query().Get("permanent") == "true"
			switch {
			case s == nil:
				reply(200, map[string]any{"subject": subject, "versions": []int{}, "permanent": permanent, "deleted": false})
			case permanent && !s.softDeleted:
				reply(409, map[string]any{"detail": "Soft-delete the subject first.", "code": "subject_not_soft_deleted"})
			case permanent:
				delete(f.subjects, subject)
				reply(200, map[string]any{"subject": subject, "versions": []int{}, "permanent": true, "deleted": true})
			default:
				deleted := !s.softDeleted
				s.softDeleted = true
				reply(200, map[string]any{"subject": subject, "versions": []int{}, "permanent": false, "deleted": deleted})
			}
		default:
			notFound()
		}
	})
}

func schemaClient(t *testing.T, f *schemaFake) *client.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := client.New(config.Settings{APIURL: srv.URL, Organization: "acme", APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The configured schema, formatted the way a person writes it; the registry stores it compact.
const paymentV1 = `{
  "type": "record", "name": "Payment",
  "fields": [{"name": "id", "type": "string"}]
}`
const paymentV2 = `{
  "type": "record", "name": "Payment",
  "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double", "default": 0}]
}`

// schemaPlan plans the resource; compatibility "" leaves it out. extra overrides
// any attribute, for example the computed ones of an update.
func schemaPlan(t *testing.T, r resource.Resource, subject, schemaText, compatibility string, hardDelete bool, extra map[string]tftypes.Value) tfsdk.Plan {
	vals := map[string]tftypes.Value{"kafka": str("orders"), "subject": str(subject), "schema_type": str("avro"),
		"schema": str(schemaText), "hard_delete": tftypes.NewValue(tftypes.Bool, hardDelete)}
	if compatibility != "" {
		vals["compatibility"] = str(compatibility)
	}
	for k, v := range extra {
		vals[k] = v
	}
	return planNull(t, r, vals, "compatibility")
}

// stateOf copies a state's attribute values. As returns the object's own map, so
// changing it without the copy would change the state too.
func stateOf(st tfsdk.State) map[string]tftypes.Value {
	var raw map[string]tftypes.Value
	_ = st.Raw.As(&raw)
	vals := make(map[string]tftypes.Value, len(raw))
	for k, v := range raw {
		vals[k] = v
	}
	return vals
}

func (f *schemaFake) callsSince(n int) []string { return append([]string(nil), f.calls[n:]...) }

// TestKafkaSchema_Lifecycle: create registers and sets the level; a read keeps the
// configured text although the registry reformats it (no plan diff); a schema change
// sets the level first and then registers version 2; a level-only change registers
// nothing; import adopts the server's schema and level; destroy with hard_delete
// soft-deletes, then removes permanently, and is idempotent.
func TestKafkaSchema_Lifecycle(t *testing.T) {
	f := newSchemaFake()
	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, f))
	ctx := context.Background()

	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: schemaPlan(t, r, "payments-value", paymentV1, "FULL", true, nil)}, &cr)
	if cr.Diagnostics.HasError() || getStr(t, cr.State, "id") != "orders/payments-value" || getInt(t, cr.State, "version") != 1 {
		t.Fatalf("create: %v", cr.Diagnostics)
	}
	if got := f.callsSince(0); strings.Join(got, ",") != "GET payments-value/versions/latest/?,POST payments-value/versions/?,GET payments-value/versions/latest/?,PUT payments-value/config/?" {
		t.Fatalf("a new subject is registered, then given its level: %v", got)
	}

	rr := resource.ReadResponse{State: cr.State}
	r.Read(ctx, resource.ReadRequest{State: cr.State}, &rr)
	if rr.Diagnostics.HasError() || getStr(t, rr.State, "schema") != paymentV1 || getStr(t, rr.State, "compatibility") != "FULL" {
		t.Fatalf("read must keep the configured text while the latest schema is the one registered from it: %v %q",
			rr.Diagnostics, getStr(t, rr.State, "schema"))
	}

	// A new schema with a relaxed level: the level must change first, or the
	// registry would judge the schema under the old one.
	n := len(f.calls)
	unknown := tftypes.NewValue(tftypes.Number, tftypes.UnknownValue)
	p := schemaPlan(t, r, "payments-value", paymentV2, "NONE", true,
		map[string]tftypes.Value{"id": str("orders/payments-value"), "schema_id": unknown, "version": unknown})
	ur := resource.UpdateResponse{State: rr.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p, State: rr.State}, &ur)
	if ur.Diagnostics.HasError() || getInt(t, ur.State, "version") != 2 {
		t.Fatalf("update: %v", ur.Diagnostics)
	}
	if got := f.callsSince(n); !strings.HasPrefix(got[0], "PUT payments-value/config/") || !strings.HasPrefix(got[1], "POST payments-value/versions/") {
		t.Fatalf("the level must change before the schema registers: %v", got)
	}

	// Only the level changes: nothing is registered, ID and version stay.
	n = len(f.calls)
	vals := stateOf(ur.State)
	vals["compatibility"] = tftypes.NewValue(tftypes.String, nil)
	p2 := tfsdk.Plan{Schema: ur.State.Schema, Raw: tftypes.NewValue(ur.State.Raw.Type(), vals)}
	ur2 := resource.UpdateResponse{State: ur.State}
	r.Update(ctx, resource.UpdateRequest{Plan: p2, State: ur.State}, &ur2)
	if got := f.callsSince(n); ur2.Diagnostics.HasError() || len(got) != 1 || !strings.HasPrefix(got[0], "DELETE payments-value/config/") {
		t.Fatalf("removing the level must clear it and register nothing: %v %v", ur2.Diagnostics, got)
	}
	if getInt(t, ur2.State, "version") != 2 || f.subjects["payments-value"].level != "" {
		t.Fatal("a level-only change must keep the version and clear the subject's level")
	}

	imp := resource.ImportStateResponse{State: emptyState(t, r)}
	r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: "orders/payments-value"}, &imp)
	ir := resource.ReadResponse{State: imp.State}
	r.Read(ctx, resource.ReadRequest{State: imp.State}, &ir)
	if ir.Diagnostics.HasError() || getStr(t, ir.State, "schema") != compact(paymentV2) || getStr(t, ir.State, "schema_type") != "avro" {
		t.Fatalf("import must adopt the registry's latest schema: %v", ir.Diagnostics)
	}
	if !stateOf(ir.State)["compatibility"].IsNull() {
		t.Fatal("an inherited level must read as unset, or every plan would set it")
	}

	n = len(f.calls)
	dr := resource.DeleteResponse{State: ur2.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur2.State}, &dr)
	if got := f.callsSince(n); dr.Diagnostics.HasError() || len(got) != 2 ||
		got[0] != "DELETE payments-value/?confirm=true" || got[1] != "DELETE payments-value/?confirm=true&permanent=true" {
		t.Fatalf("hard_delete must soft-delete, then remove permanently, confirming both: %v %v", dr.Diagnostics, got)
	}
	if _, left := f.subjects["payments-value"]; left {
		t.Fatal("the subject must be gone")
	}
	dr2 := resource.DeleteResponse{State: ur2.State}
	r.Delete(ctx, resource.DeleteRequest{State: ur2.State}, &dr2)
	if dr2.Diagnostics.HasError() {
		t.Fatalf("deleting a subject that is already gone must succeed: %v", dr2.Diagnostics)
	}
	gone := resource.ReadResponse{State: ur2.State}
	r.Read(ctx, resource.ReadRequest{State: ur2.State}, &gone)
	if !gone.State.Raw.IsNull() {
		t.Fatal("a subject deleted outside Terraform must drop from state")
	}
}

// TestKafkaSchema_DestroyKeepsSchemasByDefault: Must not: without hard_delete, destroy
// never removes the subject permanently; messages written with it must still decode.
func TestKafkaSchema_DestroyKeepsSchemasByDefault(t *testing.T) {
	f := newSchemaFake()
	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: schemaPlan(t, r, "payments-value", paymentV1, "", false, nil)}, &cr)
	n := len(f.calls)
	dr := resource.DeleteResponse{State: cr.State}
	r.Delete(context.Background(), resource.DeleteRequest{State: cr.State}, &dr)
	if got := f.callsSince(n); dr.Diagnostics.HasError() || len(got) != 1 || strings.Contains(got[0], "permanent") {
		t.Fatalf("destroy without hard_delete must only soft-delete: %v %v", dr.Diagnostics, got)
	}
	if s := f.subjects["payments-value"]; s == nil || !s.softDeleted {
		t.Fatal("the subject must be soft-deleted, its schemas kept")
	}
}

// TestKafkaSchema_CreateClearsOnlyAnExistingLevel: with no level configured, create
// clears a level an existing subject set, and sends nothing for a new subject (every
// change is an audit log entry).
func TestKafkaSchema_CreateClearsOnlyAnExistingLevel(t *testing.T) {
	f := newSchemaFake()
	f.subjects["legacy-value"] = &fakeSubject{level: "NONE"}
	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, f))
	for _, subject := range []string{"payments-value", "legacy-value"} {
		cr := resource.CreateResponse{State: emptyState(t, r)}
		r.Create(context.Background(), resource.CreateRequest{Plan: schemaPlan(t, r, subject, paymentV1, "", false, nil)}, &cr)
		if cr.Diagnostics.HasError() {
			t.Fatal(cr.Diagnostics)
		}
	}
	var clears []string
	for _, c := range f.calls {
		if strings.HasPrefix(c, "DELETE ") {
			clears = append(clears, c)
		}
	}
	if len(clears) != 1 || !strings.HasPrefix(clears[0], "DELETE legacy-value/config/") || f.subjects["legacy-value"].level != "" {
		t.Fatalf("only the existing subject's level may be cleared: %v", clears)
	}
}

// TestKafkaSchema_OlderIdenticalSchemaIsAnError: Must not: registering a schema equal
// to an older version registers nothing, and the latest would differ from the
// configuration on every plan. Apply says so instead of looping.
func TestKafkaSchema_OlderIdenticalSchemaIsAnError(t *testing.T) {
	f := newSchemaFake()
	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, f))
	ctx := context.Background()
	for _, s := range []string{paymentV1, paymentV2} {
		cr := resource.CreateResponse{State: emptyState(t, r)}
		r.Create(ctx, resource.CreateRequest{Plan: schemaPlan(t, r, "payments-value", s, "", false, nil)}, &cr)
	}
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(ctx, resource.CreateRequest{Plan: schemaPlan(t, r, "payments-value", paymentV1, "", false, nil)}, &cr)
	if !cr.Diagnostics.HasError() || !strings.Contains(cr.Diagnostics[0].Detail(), "same as version 1") {
		t.Fatalf("an older identical schema must be refused with the version it matches: %v", cr.Diagnostics)
	}
}

// stringChange runs String plan modifiers on a change for an object that exists.
func stringChange(t *testing.T, r resource.Resource, mods []planmodifier.String, attr, schemaType, old, new string) planmodifier.StringResponse {
	t.Helper()
	p := schemaPlan(t, r, "s", new, "", false, map[string]tftypes.Value{"schema_type": str(schemaType)})
	resp := planmodifier.StringResponse{PlanValue: types.StringValue(new)}
	for _, m := range mods {
		m.PlanModifyString(context.Background(), planmodifier.StringRequest{Path: pathOf(attr), Plan: p,
			State: tfsdk.State{Schema: p.Schema, Raw: p.Raw}, StateValue: types.StringValue(old),
			PlanValue: types.StringValue(new), ConfigValue: types.StringValue(new)}, &resp)
	}
	return resp
}

// TestKafkaSchema_ReformattedSchemaIsNotAChange: an Avro or JSON Schema text that
// differs only in formatting plans no change (no new version on every apply); a real
// change, and any Protobuf text change, does. ID and version stay known when the
// schema does not change.
func TestKafkaSchema_ReformattedSchemaIsNotAChange(t *testing.T) {
	r := NewKafkaSchema()
	var sr resource.SchemaResponse
	r.Schema(context.Background(), resource.SchemaRequest{}, &sr)
	mods := sr.Schema.Attributes["schema"].(rschema.StringAttribute).PlanModifiers

	if got := stringChange(t, r, mods, "schema", "avro", compact(paymentV1), paymentV1); got.PlanValue.ValueString() != compact(paymentV1) {
		t.Fatal("a reformatted Avro schema must plan no change")
	}
	if got := stringChange(t, r, mods, "schema", "avro", compact(paymentV1), paymentV2); got.PlanValue.ValueString() != paymentV2 {
		t.Fatal("a changed Avro schema must plan the new text")
	}
	proto := "syntax = \"proto3\";\nmessage P { string id = 1; }"
	if got := stringChange(t, r, mods, "schema", "protobuf", proto, proto+"\n"); got.PlanValue.ValueString() != proto {
		t.Fatal("trailing whitespace alone must not be a change")
	}
	if got := stringChange(t, r, mods, "schema", "protobuf", proto, strings.Replace(proto, " id", "  id", 1)); got.PlanValue.ValueString() == proto {
		t.Fatal("Protobuf cannot be compared parsed, so a text change is a change")
	}

	keep := sr.Schema.Attributes["version"].(rschema.Int64Attribute).PlanModifiers[0]
	for _, tc := range []struct {
		planned string
		known   bool
	}{{paymentV1, true}, {paymentV2, false}} {
		p := schemaPlan(t, r, "s", tc.planned, "", false, nil)
		st := tfsdk.State{Schema: p.Schema, Raw: schemaPlan(t, r, "s", paymentV1, "", false, nil).Raw}
		resp := planmodifier.Int64Response{PlanValue: types.Int64Unknown()}
		keep.PlanModifyInt64(context.Background(), planmodifier.Int64Request{Plan: p, State: st,
			StateValue: types.Int64Value(3), PlanValue: types.Int64Unknown()}, &resp)
		if resp.PlanValue.IsUnknown() == tc.known {
			t.Errorf("schema %s: version known=%v, want %v", tc.planned[:10], !resp.PlanValue.IsUnknown(), tc.known)
		}
	}
}

// TestKafkaSchema_SubjectWithASlashIsRefused: Must not: the API addresses a subject
// as one path segment and refuses "/" and "\", so plan and import refuse such a
// subject instead of sending a request that addresses something else. A malformed
// import ID names the format.
func TestKafkaSchema_SubjectWithASlashIsRefused(t *testing.T) {
	ctx := context.Background()
	for _, subject := range []string{"team/payments", `team\payments`} {
		vr := validator.StringResponse{}
		subjectValidator{}.ValidateString(ctx, validator.StringRequest{Path: path.Root("subject"), ConfigValue: types.StringValue(subject)}, &vr)
		if !vr.Diagnostics.HasError() {
			t.Errorf("plan must refuse subject %q", subject)
		}
	}
	vr := validator.StringResponse{}
	subjectValidator{}.ValidateString(ctx, validator.StringRequest{Path: path.Root("subject"), ConfigValue: types.StringValue("payments-value")}, &vr)
	if vr.Diagnostics.HasError() {
		t.Fatalf("plan must accept an ordinary subject: %v", vr.Diagnostics)
	}

	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, newSchemaFake()))
	for id, want := range map[string]string{"orders/team/payments": "cannot be managed", "payments-value": "<kafka>/<subject>"} {
		imp := resource.ImportStateResponse{State: emptyState(t, r)}
		r.(resource.ResourceWithImportState).ImportState(ctx, resource.ImportStateRequest{ID: id}, &imp)
		if !imp.Diagnostics.HasError() || !strings.Contains(imp.Diagnostics[0].Detail(), want) {
			t.Fatalf("import of %q: %v", id, imp.Diagnostics)
		}
	}
}

// TestKafkaSchema_CreateAdoptsAnExistingSubjectUnderTheConfiguredLevel: adopting an
// existing subject applies the configured level before registering, so a schema only
// that level allows registers in one apply instead of failing on every apply.
func TestKafkaSchema_CreateAdoptsAnExistingSubjectUnderTheConfiguredLevel(t *testing.T) {
	f := newSchemaFake()
	f.strict = true
	f.subjects["payments-value"] = &fakeSubject{level: "FULL",
		versions: []*fakeVersion{{id: 7, version: 1, schema: compact(`{"type":"string"}`)}}}
	r := NewKafkaSchema()
	configured(t, r, schemaClient(t, f))
	cr := resource.CreateResponse{State: emptyState(t, r)}
	r.Create(context.Background(), resource.CreateRequest{Plan: schemaPlan(t, r, "payments-value", paymentV1, "NONE", false, nil)}, &cr)
	if cr.Diagnostics.HasError() || getInt(t, cr.State, "version") != 2 || f.subjects["payments-value"].level != "NONE" {
		t.Fatalf("adopting under NONE must register the schema as version 2: %v", cr.Diagnostics)
	}
}
