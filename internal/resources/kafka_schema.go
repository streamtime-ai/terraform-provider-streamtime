package resources

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewKafkaSchema returns the streamtime_kafka_schema resource.
func NewKafkaSchema() resource.Resource { return &kafkaSchemaResource{} }

type kafkaSchemaResource struct{ c *client.Client }

type kafkaSchemaModel struct {
	ID            types.String `tfsdk:"id"`
	Kafka         types.String `tfsdk:"kafka"`
	Subject       types.String `tfsdk:"subject"`
	SchemaType    types.String `tfsdk:"schema_type"`
	Schema        types.String `tfsdk:"schema"`
	Compatibility types.String `tfsdk:"compatibility"`
	HardDelete    types.Bool   `tfsdk:"hard_delete"`
	SchemaID      types.Int64  `tfsdk:"schema_id"`
	Version       types.Int64  `tfsdk:"version"`
}

// schemaVersionWire is the SchemaVersion component of the spec.
type schemaVersionWire struct {
	Subject    string `json:"subject"`
	Version    int64  `json:"version"`
	ID         int64  `json:"id"`
	SchemaType string `json:"schema_type"`
	Schema     string `json:"schema"`
}

// schemaRegisterWire is the SchemaRegisterResponse component of the spec.
type schemaRegisterWire struct {
	ID      int64 `json:"id"`
	Version int64 `json:"version"`
}

// subjectCompatibilityWire is the SubjectCompatibility component of the spec.
type subjectCompatibilityWire struct {
	Compatibility string `json:"compatibility"`
	Inherited     bool   `json:"inherited"`
}

var (
	schemaTypes         = []string{"avro", "json", "protobuf"}
	compatibilityLevels = []string{"BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE",
		"FULL", "FULL_TRANSITIVE", "NONE"}
)

func (r *kafkaSchemaResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka_schema"
}

func (r *kafkaSchemaResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	keep := []planmodifier.Int64{keepUnlessSchemaChanges{}}
	resp.Schema = schema.Schema{
		Description: "A subject in a Kafka cluster's schema registry, and its latest schema. Changing the schema " +
			"registers a new version; earlier versions stay in the registry. The cluster must have a schema registry, " +
			"and its fleet a recent Agent.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
				Description: "`<kafka>/<subject>`. Import with the same value."},
			"kafka": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the streamtime_kafka cluster whose schema registry holds the subject. Changing it creates a new subject."},
			"subject": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Validators: []validator.String{subjectValidator{}},
				Description: "Subject name, usually `<topic>-value` or `<topic>-key`, without / or \\. If the subject " +
					"already exists, its versions are kept, the configured compatibility level is applied, and then the " +
					"schema is registered as its next version. Changing it creates a new subject."},
			"schema_type": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Validators:  []validator.String{oneOf(schemaTypes)},
				Description: "avro, json (JSON Schema) or protobuf. Changing it creates a new subject."},
			"schema": schema.StringAttribute{Required: true, PlanModifiers: []planmodifier.String{sameSchema{}},
				Description: "The schema text, at most 1 MiB, for example file(\"payment.avsc\"). Schemas that reference " +
					"other schemas are not supported. A change registers a new version, which the subject's compatibility " +
					"level must allow. Avro and JSON Schema texts that differ only in formatting are not a change."},
			"compatibility": schema.StringAttribute{Optional: true,
				Validators: []validator.String{oneOf(compatibilityLevels)},
				Description: "The subject's own compatibility level: BACKWARD, BACKWARD_TRANSITIVE, FORWARD, " +
					"FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE or NONE. Leave it out to use the registry's default; " +
					"removing it clears the subject's level. A changed level applies before a changed schema is registered."},
			"hard_delete": schema.BoolAttribute{Optional: true, Computed: true, Default: booldefault.StaticBool(false),
				Description: "When true, destroying the resource removes the subject permanently, and messages written " +
					"with its schemas can no longer be decoded. When false (the default), the subject is soft-deleted: " +
					"its schemas and IDs are kept, and it can be removed permanently in Streamtime later."},
			"schema_id": schema.Int64Attribute{Computed: true, PlanModifiers: keep,
				Description: "The registry's ID for the latest schema, as carried in messages."},
			"version": schema.Int64Attribute{Computed: true, PlanModifiers: keep,
				Description: "The subject's latest version number."},
		},
	}
}

// oneOfValidator refuses a string outside a fixed list.
type oneOfValidator []string

func oneOf(values []string) validator.String { return oneOfValidator(values) }

func (v oneOfValidator) Description(context.Context) string {
	return "Must be one of: " + strings.Join(v, ", ") + "."
}
func (v oneOfValidator) MarkdownDescription(ctx context.Context) string { return v.Description(ctx) }
func (v oneOfValidator) ValidateString(ctx context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() || slices.Contains(v, req.ConfigValue.ValueString()) {
		return
	}
	resp.Diagnostics.AddAttributeError(req.Path, "Unsupported value",
		fmt.Sprintf("%q is not supported. %s", req.ConfigValue.ValueString(), v.Description(ctx)))
}

// sameSchema keeps the stored schema text when the configured one means the same
// schema. The registry stores its own formatting, so without this a reformatted
// file, or the first plan after an import, would register again.
type sameSchema struct{}

func (sameSchema) Description(context.Context) string {
	return "Avro and JSON Schema texts that differ only in formatting are not a change."
}
func (s sameSchema) MarkdownDescription(ctx context.Context) string { return s.Description(ctx) }
func (sameSchema) PlanModifyString(ctx context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.StateValue.IsNull() || req.PlanValue.IsUnknown() || req.PlanValue.IsNull() {
		return
	}
	var schemaType types.String
	req.Plan.GetAttribute(ctx, path.Root("schema_type"), &schemaType)
	if schemasEqual(schemaType.ValueString(), req.PlanValue.ValueString(), req.StateValue.ValueString()) {
		resp.PlanValue = req.StateValue
	}
}

// keepUnlessSchemaChanges keeps schema_id and version from state unless the plan
// registers a new schema, so a compatibility change does not show them as unknown.
type keepUnlessSchemaChanges struct{}

func (keepUnlessSchemaChanges) Description(context.Context) string {
	return "Known until the schema changes."
}
func (k keepUnlessSchemaChanges) MarkdownDescription(ctx context.Context) string {
	return k.Description(ctx)
}
func (keepUnlessSchemaChanges) PlanModifyInt64(ctx context.Context, req planmodifier.Int64Request, resp *planmodifier.Int64Response) {
	if req.StateValue.IsNull() || !req.PlanValue.IsUnknown() {
		return
	}
	var planned, stored types.String
	req.Plan.GetAttribute(ctx, path.Root("schema"), &planned)
	req.State.GetAttribute(ctx, path.Root("schema"), &stored)
	if !planned.IsUnknown() && planned.Equal(stored) {
		resp.PlanValue = req.StateValue
	}
}

// schemasEqual reports whether two schema texts of a type are the same schema.
// Avro and JSON Schema are JSON documents, compared parsed; Protobuf is compared
// as text, ignoring leading and trailing whitespace.
func schemasEqual(schemaType, a, b string) bool {
	if strings.TrimSpace(a) == strings.TrimSpace(b) {
		return true
	}
	if schemaType != "avro" && schemaType != "json" {
		return false
	}
	x, errA := decodeJSON([]byte(a))
	y, errB := decodeJSON([]byte(b))
	return errA == nil && errB == nil && reflect.DeepEqual(x, y)
}

func (r *kafkaSchemaResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func subjectPath(kafka, subject string) string {
	return kafkaPath(kafka) + "schemas/subjects/" + url.PathEscape(subject) + "/"
}

func splitSchemaID(id string) (string, string, error) {
	kafka, subject, ok := strings.Cut(id, "/")
	if !ok || kafka == "" || subject == "" {
		return "", "", fmt.Errorf("expected <kafka>/<subject>, for example orders-cluster/payments-value; got %q", id)
	}
	if !addressableSubject(subject) {
		return "", "", fmt.Errorf("%s: %s", subject, unaddressableSubject)
	}
	return kafka, subject, nil
}

// The API addresses a subject as one URL path segment and refuses a "/" or "\" in
// it, so such a subject cannot be managed here.
const unaddressableSubject = "a subject containing / or \\ cannot be managed through the API"

func addressableSubject(s string) bool { return !strings.ContainsAny(s, `/\`) }

// subjectValidator refuses, at plan time, a subject the API cannot address.
type subjectValidator struct{}

func (subjectValidator) Description(context.Context) string {
	return "Must not contain / or \\."
}
func (v subjectValidator) MarkdownDescription(ctx context.Context) string { return v.Description(ctx) }
func (subjectValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() || addressableSubject(req.ConfigValue.ValueString()) {
		return
	}
	resp.Diagnostics.AddAttributeError(req.Path, "Unsupported subject name",
		fmt.Sprintf("%q: %s. Use a subject name without them.", req.ConfigValue.ValueString(), unaddressableSubject))
}

// setCompatibility sets the subject's level, or clears it when the configuration
// leaves it out.
func (r *kafkaSchemaResource) setCompatibility(ctx context.Context, kafka, subject string, level types.String) error {
	if level.IsNull() {
		return r.c.Do(ctx, "DELETE", subjectPath(kafka, subject)+"config/", nil, nil)
	}
	return r.c.Do(ctx, "PUT", subjectPath(kafka, subject)+"config/", map[string]any{"compatibility": level.ValueString()}, nil)
}

// register registers the configured schema and records its ID and version. The
// registry answers a schema identical to an older version with that version and
// registers nothing; the subject's latest would then differ from the configuration
// on every plan, so that is an error.
func (r *kafkaSchemaResource) register(ctx context.Context, m *kafkaSchemaModel) error {
	kafka, subject := m.Kafka.ValueString(), m.Subject.ValueString()
	var out schemaRegisterWire
	body := map[string]any{"schema": m.Schema.ValueString(), "schema_type": m.SchemaType.ValueString()}
	if err := r.c.Do(ctx, "POST", subjectPath(kafka, subject)+"versions/", body, &out); err != nil {
		return err
	}
	var latest schemaVersionWire
	if err := r.c.Do(ctx, "GET", subjectPath(kafka, subject)+"versions/latest/", nil, &latest); err != nil {
		return err
	}
	if latest.ID != out.ID {
		return fmt.Errorf("the schema is the same as version %d of %s, but the latest version is %d, so nothing was "+
			"registered. Change the schema, or delete the versions after %d in Streamtime", out.Version, subject,
			latest.Version, out.Version)
	}
	m.SchemaID, m.Version = types.Int64Value(out.ID), types.Int64Value(out.Version)
	return nil
}

func (r *kafkaSchemaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m kafkaSchemaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	kafka, subject := m.Kafka.ValueString(), m.Subject.ValueString()
	m.ID = types.StringValue(kafka + "/" + subject)
	// An existing subject gets the configured level before the schema registers, as
	// in Update, so a schema only that level allows registers in the same apply. A
	// new subject has to exist first, so its level follows the registration.
	var latest schemaVersionWire
	err := r.c.Do(ctx, "GET", subjectPath(kafka, subject)+"versions/latest/", nil, &latest)
	exists := err == nil
	if err != nil && !isGone(err) {
		resp.Diagnostics.AddError("Could not read the subject", err.Error())
		return
	}
	if exists {
		if err := r.applyCompatibility(ctx, kafka, subject, m.Compatibility); err != nil {
			resp.Diagnostics.AddError("Could not set the subject's compatibility level", err.Error())
			return
		}
	}
	if err := r.register(ctx, &m); err != nil {
		resp.Diagnostics.AddError("Could not register the schema", err.Error())
		return
	}
	if !exists {
		if err := r.applyCompatibility(ctx, kafka, subject, m.Compatibility); err != nil {
			resp.Diagnostics.AddError("Schema registered, but its compatibility level could not be set", err.Error())
		}
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// applyCompatibility makes the subject's level match the configuration. Without a
// configured level it clears the subject's own, but only when it has one, since
// every change is an audit log entry.
func (r *kafkaSchemaResource) applyCompatibility(ctx context.Context, kafka, subject string, level types.String) error {
	if !level.IsNull() {
		return r.setCompatibility(ctx, kafka, subject, level)
	}
	var cfg subjectCompatibilityWire
	if err := r.c.Do(ctx, "GET", subjectPath(kafka, subject)+"config/", nil, &cfg); err != nil || cfg.Inherited {
		return err
	}
	return r.setCompatibility(ctx, kafka, subject, level)
}

func (r *kafkaSchemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m kafkaSchemaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	kafka, subject, err := splitSchemaID(m.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Invalid schema ID", err.Error())
		return
	}
	var latest schemaVersionWire
	var cfg subjectCompatibilityWire
	err = r.c.Do(ctx, "GET", subjectPath(kafka, subject)+"versions/latest/", nil, &latest)
	if err == nil {
		err = r.c.Do(ctx, "GET", subjectPath(kafka, subject)+"config/", nil, &cfg)
	}
	if err != nil {
		if isGone(err) { // the subject, deleted or soft-deleted, or the cluster is gone
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the schema", err.Error())
		return
	}
	m.ID, m.Kafka, m.Subject = types.StringValue(kafka+"/"+subject), types.StringValue(kafka), types.StringValue(subject)
	m.SchemaType = types.StringValue(latest.SchemaType)
	// Keep the configured text while the latest schema is still the one registered
	// from it: the registry stores the schema in its own formatting.
	same := !m.Schema.IsNull() && (m.SchemaID.ValueInt64() == latest.ID || schemasEqual(latest.SchemaType, m.Schema.ValueString(), latest.Schema))
	if !same {
		m.Schema = types.StringValue(latest.Schema)
	}
	m.SchemaID, m.Version = types.Int64Value(latest.ID), types.Int64Value(latest.Version)
	if cfg.Inherited {
		m.Compatibility = types.StringNull()
	} else {
		m.Compatibility = types.StringValue(cfg.Compatibility)
	}
	if m.HardDelete.IsNull() { // after an import
		m.HardDelete = types.BoolValue(false)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaSchemaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior kafkaSchemaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	kafka, subject := prior.Kafka.ValueString(), prior.Subject.ValueString()
	// The level first, so relaxing it lets an otherwise incompatible schema register
	// in the same apply.
	if !m.Compatibility.Equal(prior.Compatibility) {
		if err := r.setCompatibility(ctx, kafka, subject, m.Compatibility); err != nil {
			resp.Diagnostics.AddError("Could not change the compatibility level", err.Error())
			return
		}
	}
	if m.Schema.Equal(prior.Schema) {
		m.SchemaID, m.Version = prior.SchemaID, prior.Version
	} else if err := r.register(ctx, &m); err != nil {
		resp.Diagnostics.AddError("Could not register the schema", err.Error())
		// Keep what did change, so the next plan shows only the schema.
		prior.Compatibility = m.Compatibility
		resp.Diagnostics.Append(resp.State.Set(ctx, prior)...)
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaSchemaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m kafkaSchemaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// Deletes need confirm=true. A permanent delete removes only what is already
	// soft-deleted, so it follows the soft delete. Already gone is success.
	p := subjectPath(m.Kafka.ValueString(), m.Subject.ValueString()) + "?confirm=true"
	if err := r.c.Do(ctx, "DELETE", p, nil, nil); err != nil && !isGone(err) {
		resp.Diagnostics.AddError("Could not delete the subject", err.Error())
		return
	}
	if m.HardDelete.ValueBool() {
		if err := r.c.Do(ctx, "DELETE", p+"&permanent=true", nil, nil); err != nil && !isGone(err) {
			resp.Diagnostics.AddError("Subject soft-deleted, but it could not be removed permanently", err.Error())
		}
	}
}

func (r *kafkaSchemaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	kafka, subject, err := splitSchemaID(req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Invalid import ID", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), kafka+"/"+subject)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("kafka"), kafka)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("subject"), subject)...)
}
