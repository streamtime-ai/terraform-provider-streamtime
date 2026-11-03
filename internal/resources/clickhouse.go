package resources

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-timeouts/resource/timeouts"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewClickHouse returns the streamtime_clickhouse resource.
func NewClickHouse() resource.Resource { return &clickHouseResource{} }

type clickHouseResource struct{ c *client.Client }

type clickHouseModel struct {
	ID                    types.String   `tfsdk:"id"`
	Identifier            types.String   `tfsdk:"identifier"`
	Description           types.String   `tfsdk:"description"`
	Fleet                 types.String   `tfsdk:"fleet"`
	CloudProvider         types.String   `tfsdk:"cloud_provider"`
	TenancyMode           types.String   `tfsdk:"tenancy_mode"`
	Region                types.String   `tfsdk:"region"`
	RetentionDays         types.Int64    `tfsdk:"retention_days"`
	Tags                  types.Map      `tfsdk:"tags"`
	AdvancedConfiguration types.String   `tfsdk:"advanced_configuration"`
	HTTPURL               types.String   `tfsdk:"http_url"`
	DNSRecord             types.String   `tfsdk:"dns_record"`
	CredentialsSecretURI  types.String   `tfsdk:"credentials_secret_uri"`
	Status                types.String   `tfsdk:"status"`
	ReconciliationState   types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState    types.String   `tfsdk:"configuration_state"`
	Generation            types.Int64    `tfsdk:"generation"`
	ObservedGeneration    types.Int64    `tfsdk:"observed_generation"`
	Timeouts              timeouts.Value `tfsdk:"timeouts"`
}

// clickHouseWire is the server's ClickHouse instance (ClickHouseResponseSchema in the spec).
type clickHouseWire struct {
	Identifier            string          `json:"identifier"`
	Description           *string         `json:"description"`
	Fleet                 *string         `json:"fleet"`
	CloudProvider         string          `json:"cloud_provider"`
	TenancyMode           string          `json:"tenancy_mode"`
	Region                string          `json:"region"`
	RetentionDays         int64           `json:"retention_days"`
	Tags                  []kafkaTag      `json:"tags"`
	AdvancedConfiguration json.RawMessage `json:"advanced_configuration"`
	HTTPURL               *string         `json:"http_url"`
	DNSRecord             *string         `json:"dns_record"`
	CredentialsSecretURI  *string         `json:"credentials_secret_uri"`
	Status                *string         `json:"status"`
	ReconciliationState   *string         `json:"reconciliation_state"`
	ConfigurationState    *string         `json:"configuration_state"`
	DesiredLifecycle      *string         `json:"desired_lifecycle"`
	Generation            int64           `json:"generation"`
	ObservedGeneration    int64           `json:"observed_generation"`
}

// Polling bounds while a ClickHouse instance is deployed, changed or removed.
var clickHousePoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w clickHouseWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *clickHouseResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_clickhouse"
}

func (r *clickHouseResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A ClickHouse analytics database on a fleet. Creating one deploys it and waits until it is ready; " +
			"a change is deployed and waited for too. " +
			"If a change fails to deploy, the next plan shows it as an update and that apply deploys it again.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name of the database: up to 24 lowercase letters, digits and dashes. Changing it creates a new database."},
			"description": schema.StringAttribute{Optional: true, Description: "Free-text description."},
			"fleet": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the fleet to deploy on. Changing it creates a new database."},
			"cloud_provider": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Cloud the database runs in, for your records."},
			"tenancy_mode": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Shared, Isolated or Dedicated, for your records."},
			"region": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Region the database runs in, for your records."},
			"retention_days": schema.Int64Attribute{Optional: true, Computed: true,
				PlanModifiers: []planmodifier.Int64{int64planmodifier.UseStateForUnknown()},
				Description:   "Days ingested data is kept before it ages out. Streamtime uses 30 when it is left out."},
			"tags": schema.MapAttribute{ElementType: types.StringType, Optional: true, Computed: true,
				PlanModifiers: []planmodifier.Map{mapplanmodifier.UseStateForUnknown()},
				Description:   "Tags on the database, as key = value. Removing the attribute keeps the current tags; set tags = {} to clear them."},
			"advanced_configuration": schema.StringAttribute{Optional: true, Computed: true, Sensitive: true, PlanModifiers: keepStr,
				Description: "Advanced settings, as a JSON object set with jsonencode(): shards, replicas, resources, image, " +
					"users, profiles and settings. Users may include credentials. Settings Streamtime fills in are not shown as changes."},
			"http_url":   schema.StringAttribute{Computed: true, Description: "HTTP address of the database, once it is ready."},
			"dns_record": schema.StringAttribute{Computed: true, Description: "Address of the database's own gateway, once it is ready."},
			"credentials_secret_uri": schema.StringAttribute{Computed: true,
				Description: "Path of the Streamtime secret that holds the default user's credentials. It is never the password itself."},
			"status":               schema.StringAttribute{Computed: true, Description: "Health of the database, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true, Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the database's settings are complete: incomplete, valid or invalid."},
			"generation":          schema.Int64Attribute{Computed: true, Description: "Version of the database's settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings the running database has."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Update: true, Delete: true}),
		},
	}
}

func (r *clickHouseResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func clickHousePath(id string) string { return "clickhouses/" + url.PathEscape(id) + "/" }

// body is the create or update request. Streamtime does not change the identifier
// on update; it is sent on create only.
func (r *clickHouseResource) body(ctx context.Context, m clickHouseModel, create bool) (map[string]any, error) {
	b := map[string]any{
		"fleet":       m.Fleet.ValueString(),
		"description": optString(m.Description), // null clears it
	}
	if create {
		b["identifier"] = m.Identifier.ValueString()
	}
	for k, v := range map[string]types.String{"cloud_provider": m.CloudProvider, "tenancy_mode": m.TenancyMode, "region": m.Region} {
		if p := optString(v); p != nil {
			b[k] = *p
		}
	}
	if !m.RetentionDays.IsNull() && !m.RetentionDays.IsUnknown() {
		b["retention_days"] = m.RetentionDays.ValueInt64()
	}
	if !m.Tags.IsNull() && !m.Tags.IsUnknown() {
		list, err := tagList(ctx, m.Tags)
		if err != nil {
			return nil, err
		}
		b["tags"] = list
	}
	adv, err := jsonObject(m.AdvancedConfiguration, "advanced_configuration")
	if err != nil {
		return nil, err
	}
	if adv != nil {
		b["advanced_configuration"] = adv
	}
	return b, nil
}

// apply copies the server's view into state. The advanced settings keep the
// configured text while the server holds what it sets.
func (m *clickHouseModel) apply(a clickHouseWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.Description = stringOrNull(a.Description)
	m.Fleet = stringOrNull(a.Fleet)
	m.CloudProvider = types.StringValue(a.CloudProvider)
	m.TenancyMode = types.StringValue(a.TenancyMode)
	m.Region = types.StringValue(a.Region)
	m.RetentionDays = types.Int64Value(a.RetentionDays)
	m.Tags = tagMap(a.Tags)
	m.AdvancedConfiguration = keptOrServerJSON(m.AdvancedConfiguration, a.AdvancedConfiguration, true)
	m.HTTPURL = stringOrNull(a.HTTPURL)
	m.DNSRecord = stringOrNull(a.DNSRecord)
	m.CredentialsSecretURI = stringOrNull(a.CredentialsSecretURI)
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

func (r *clickHouseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m clickHouseModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m, true)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the ClickHouse database", err.Error())
		return
	}
	var out clickHouseWire
	if err := r.c.Do(ctx, "POST", "clickhouses/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the ClickHouse database", err.Error())
		return
	}
	// The database exists from here on: record it, so a failure below leaves it in
	// state (marked for replacement) rather than unmanaged.
	save := func() {
		m.apply(out)
		resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	}
	save()
	if resp.Diagnostics.HasError() {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := reconcileAndWait(wctx, r.c, clickHousePath(out.Identifier), "ClickHouse database", "create", out.Identifier, out.Generation, clickHousePoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The ClickHouse database was not created", werr.Error())
	}
}

func (r *clickHouseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m clickHouseModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out clickHouseWire
	if err := r.c.Do(ctx, "GET", clickHousePath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the ClickHouse database", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *clickHouseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior clickHouseModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Update(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m, false)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the ClickHouse database", err.Error())
		return
	}
	var out clickHouseWire
	if err := r.c.Do(ctx, "PUT", clickHousePath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the ClickHouse database", err.Error())
		return
	}
	save := func() {
		m.apply(out)
		resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	}
	save()
	if resp.Diagnostics.HasError() {
		return
	}
	// A change to what is deployed raises the generation. That generation, or an
	// earlier one that failed to deploy, is deployed; a change to nothing that runs
	// (the description) is only saved.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := deploySaved(wctx, r.c, clickHousePath(out.Identifier), "ClickHouse database", out.Identifier, out.Generation, clickHousePoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The change to the ClickHouse database was not deployed", werr.Error())
	}
}

// ModifyPlan plans an update when the last change is saved but not running, so
// the next apply deploys it.
func (r *clickHouseResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	planRedeploy(ctx, req, resp, "ClickHouse database", []string{"http_url", "dns_record", "credentials_secret_uri", "status", "reconciliation_state", "configuration_state"})
}

func (r *clickHouseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m clickHouseModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Delete(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	id := m.ID.ValueString()
	if err := r.c.Do(ctx, "DELETE", clickHousePath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the ClickHouse database", err.Error())
		return
	}
	// Deleting starts the teardown; the database is gone once Streamtime has removed it.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[clickHouseWire](wctx, r.c, clickHousePath(id), "ClickHouse database", clickHousePoll); err != nil {
		resp.Diagnostics.AddError("The ClickHouse database was not deleted", err.Error())
	}
}

func (r *clickHouseResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
