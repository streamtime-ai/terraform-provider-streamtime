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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewFlink returns the streamtime_flink resource.
func NewFlink() resource.Resource { return &flinkResource{} }

type flinkResource struct{ c *client.Client }

type flinkModel struct {
	ID                    types.String         `tfsdk:"id"`
	Identifier            types.String         `tfsdk:"identifier"`
	Description           types.String         `tfsdk:"description"`
	FlinkProvider         types.String         `tfsdk:"flink_provider"`
	Mode                  types.String         `tfsdk:"mode"`
	Fleet                 types.String         `tfsdk:"fleet"`
	FlinkUnits            types.Int64          `tfsdk:"flink_units"`
	MaxFlinkUnits         types.Int64          `tfsdk:"max_flink_units"`
	Tags                  types.Map            `tfsdk:"tags"`
	AdvancedConfiguration types.String         `tfsdk:"advanced_configuration"`
	ProjectVersion        *projectVersionModel `tfsdk:"project_version"`
	UIURL                 types.String         `tfsdk:"ui_url"`
	Status                types.String         `tfsdk:"status"`
	ReconciliationState   types.String         `tfsdk:"reconciliation_state"`
	ConfigurationState    types.String         `tfsdk:"configuration_state"`
	Generation            types.Int64          `tfsdk:"generation"`
	ObservedGeneration    types.Int64          `tfsdk:"observed_generation"`
	Timeouts              timeouts.Value       `tfsdk:"timeouts"`
}

type projectVersionModel struct {
	Project types.String `tfsdk:"project"`
	Version types.String `tfsdk:"version"`
}

// flinkWire is the server's Flink instance (FlinkResponseSchema in the spec).
type flinkWire struct {
	Identifier            string           `json:"identifier"`
	Description           *string          `json:"description"`
	Provider              string           `json:"provider"`
	Mode                  string           `json:"mode"`
	Fleet                 *string          `json:"fleet"`
	FlinkUnits            *int64           `json:"flink_units"`
	MaxFlinkUnits         *int64           `json:"max_flink_units"`
	Tags                  []kafkaTag       `json:"tags"`
	AdvancedConfiguration json.RawMessage  `json:"advanced_configuration"`
	ProjectVersion        *projectVersionW `json:"project_version"`
	UIURL                 *string          `json:"ui_url"`
	Status                *string          `json:"status"`
	ReconciliationState   *string          `json:"reconciliation_state"`
	ConfigurationState    *string          `json:"configuration_state"`
	DesiredLifecycle      *string          `json:"desired_lifecycle"`
	Generation            int64            `json:"generation"`
	ObservedGeneration    int64            `json:"observed_generation"`
}

type projectVersionW struct {
	Project string `json:"project"`
	Version string `json:"version"`
}

// Polling bounds while a Flink instance is deployed, changed or removed.
var flinkPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w flinkWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *flinkResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_flink"
}

func (r *flinkResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	keepAndReplace := []planmodifier.String{stringplanmodifier.UseStateForUnknown(), stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A Flink instance on a fleet: a single-job cluster that runs one application, or a job workspace " +
			"that runs several jobs. Creating one deploys it and waits until it is ready; a change is deployed and waited for too. " +
			"If a change fails to deploy, the next plan shows it as an update and that apply deploys it again.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
				Description: "Unique name of the instance: up to 24 lowercase letters, digits and dashes. Changing it creates a new instance."},
			"description": schema.StringAttribute{Optional: true, Description: "Free-text description."},
			"flink_provider": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepAndReplace,
				Description: "Flink distribution: open-source (the default) or cmf (Confluent Manager for Apache Flink). " +
					"Changing it creates a new instance."},
			"mode": schema.StringAttribute{Required: true, PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
				Description: "session (a job workspace that runs several jobs; the only mode for open-source Apache Flink) or " +
					"application (a single-job cluster that runs one application; Confluent Manager for Apache Flink only). " +
					"Changing it creates a new instance."},
			"fleet": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepAndReplace,
				Description: "Identifier of the fleet to deploy on. Leave it out to let Streamtime choose a fleet. " +
					"Changing it creates a new instance."},
			"flink_units": schema.Int64Attribute{Optional: true,
				Description: "Capacity of a single-job cluster (mode = application), in Flink units."},
			"max_flink_units": schema.Int64Attribute{Optional: true,
				Description: "Capacity a job workspace (mode = session) shares between its jobs, in Flink units."},
			"tags": schema.MapAttribute{ElementType: types.StringType, Optional: true, Computed: true,
				PlanModifiers: []planmodifier.Map{mapplanmodifier.UseStateForUnknown()},
				Description:   "Tags on the instance, as key = value. Removing the attribute keeps the current tags; set tags = {} to clear them."},
			"advanced_configuration": schema.StringAttribute{Optional: true, Computed: true, Sensitive: true, PlanModifiers: keepStr,
				Description: "Advanced settings, as a JSON object set with jsonencode(): the application, image, runtime of a job " +
					"workspace (java or python), parallelism, " +
					"resources and Flink settings. It may include a license key. Settings Streamtime fills in are not shown as changes."},
			"project_version": schema.SingleNestedAttribute{Optional: true,
				Description: "Version of a Flink project to run, instead of an application location in advanced_configuration.",
				Attributes: map[string]schema.Attribute{
					"project": schema.StringAttribute{Required: true, Description: "Identifier of the Flink project."},
					"version": schema.StringAttribute{Required: true, Description: "Version uploaded to that project."},
				}},
			"ui_url":               schema.StringAttribute{Computed: true, Description: "Address of the Flink web UI, once the instance is ready."},
			"status":               schema.StringAttribute{Computed: true, Description: "Health of the instance, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true, Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the instance's settings are complete: incomplete, valid or invalid."},
			"generation": schema.Int64Attribute{Computed: true,
				Description: "Version of the instance's settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings the running instance has."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Update: true, Delete: true}),
		},
	}
}

func (r *flinkResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func flinkPath(id string) string { return "flink/" + url.PathEscape(id) + "/" }

func (r *flinkResource) body(ctx context.Context, m flinkModel) (map[string]any, error) {
	b := map[string]any{
		"identifier":  m.Identifier.ValueString(),
		"mode":        m.Mode.ValueString(),
		"description": optString(m.Description), // null clears it
	}
	if p := optString(m.FlinkProvider); p != nil && *p != "" {
		b["provider"] = *p
	}
	if p := optString(m.Fleet); p != nil && *p != "" {
		b["fleet"] = *p
	}
	for k, v := range map[string]types.Int64{"flink_units": m.FlinkUnits, "max_flink_units": m.MaxFlinkUnits} {
		if !v.IsNull() && !v.IsUnknown() {
			b[k] = v.ValueInt64()
		} else {
			b[k] = nil
		}
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
	// Always sent: Streamtime deploys an instance only once its advanced settings are saved.
	if adv == nil {
		adv = map[string]any{}
	}
	b["advanced_configuration"] = adv
	b["project_version"] = nil
	if pv := m.ProjectVersion; pv != nil {
		b["project_version"] = projectVersionW{Project: pv.Project.ValueString(), Version: pv.Version.ValueString()}
	}
	return b, nil
}

// apply copies the server's view into state. The advanced settings keep the
// configured text while the server holds what it sets.
func (m *flinkModel) apply(a flinkWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.Description = stringOrNull(a.Description)
	m.FlinkProvider = types.StringValue(a.Provider)
	m.Mode = types.StringValue(a.Mode)
	m.Fleet = stringOrNull(a.Fleet)
	m.FlinkUnits = types.Int64PointerValue(a.FlinkUnits)
	m.MaxFlinkUnits = types.Int64PointerValue(a.MaxFlinkUnits)
	m.Tags = tagMap(a.Tags)
	m.AdvancedConfiguration = keptOrServerJSON(m.AdvancedConfiguration, a.AdvancedConfiguration, true)
	m.ProjectVersion = nil
	if a.ProjectVersion != nil {
		m.ProjectVersion = &projectVersionModel{Project: types.StringValue(a.ProjectVersion.Project), Version: types.StringValue(a.ProjectVersion.Version)}
	}
	m.UIURL = stringOrNull(a.UIURL)
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

func (r *flinkResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m flinkModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the Flink instance", err.Error())
		return
	}
	var out flinkWire
	if err := r.c.Do(ctx, "POST", "flink/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the Flink instance", err.Error())
		return
	}
	// The instance exists from here on: record it, so a failure below leaves it in
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
	werr := reconcileAndWait(wctx, r.c, flinkPath(out.Identifier), "Flink instance", "create", out.Identifier, out.Generation, flinkPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The Flink instance was not created", werr.Error())
	}
}

func (r *flinkResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m flinkModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out flinkWire
	if err := r.c.Do(ctx, "GET", flinkPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the Flink instance", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *flinkResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior flinkModel
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
	body, err := r.body(ctx, m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the Flink instance", err.Error())
		return
	}
	var out flinkWire
	if err := r.c.Do(ctx, "PUT", flinkPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the Flink instance", err.Error())
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
	werr := deploySaved(wctx, r.c, flinkPath(out.Identifier), "Flink instance", out.Identifier, out.Generation, flinkPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The change to the Flink instance was not deployed", werr.Error())
	}
}

// ModifyPlan plans an update when the last change is saved but not running, so
// the next apply deploys it.
func (r *flinkResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	planRedeploy(ctx, req, resp, "Flink instance", []string{"ui_url", "status", "reconciliation_state", "configuration_state"})
}

func (r *flinkResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m flinkModel
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
	if err := r.c.Do(ctx, "DELETE", flinkPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the Flink instance", err.Error())
		return
	}
	// Deleting starts the teardown; the instance is gone once Streamtime has removed it.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[flinkWire](wctx, r.c, flinkPath(id), "Flink instance", flinkPoll); err != nil {
		resp.Diagnostics.AddError("The Flink instance was not deleted", err.Error())
	}
}

func (r *flinkResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
