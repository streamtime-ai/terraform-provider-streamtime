package resources

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-timeouts/resource/timeouts"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/setplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewFleet returns the streamtime_fleet resource.
func NewFleet() resource.Resource { return &fleetResource{} }

type fleetResource struct{ c *client.Client }

type fleetModel struct {
	ID                     types.String   `tfsdk:"id"`
	Identifier             types.String   `tfsdk:"identifier"`
	FleetProvider          types.String   `tfsdk:"fleet_provider"`
	Domain                 types.String   `tfsdk:"domain"`
	Network                types.String   `tfsdk:"network"`
	TenancyMode            types.String   `tfsdk:"tenancy_mode"`
	MaxTenants             types.Int64    `tfsdk:"max_tenants"`
	MaxKafkaUnits          types.Int64    `tfsdk:"max_kafka_units"`
	Autoscale              types.Bool     `tfsdk:"autoscale"`
	EnableAI               types.Bool     `tfsdk:"enable_ai"`
	StreamtimeAIAccess     types.String   `tfsdk:"streamtime_ai_access"`
	Workloads              types.String   `tfsdk:"workloads"`
	PlacementConfig        types.String   `tfsdk:"placement_config"`
	AdvancedConfig         types.String   `tfsdk:"advanced_config"`
	AlertChannels          types.Set      `tfsdk:"alert_channels"`
	ObservabilityProviders types.Set      `tfsdk:"observability_providers"`
	AlertWebhookSecret     types.String   `tfsdk:"alert_webhook_secret"`
	Status                 types.String   `tfsdk:"status"`
	ReconciliationState    types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState     types.String   `tfsdk:"configuration_state"`
	Generation             types.Int64    `tfsdk:"generation"`
	ObservedGeneration     types.Int64    `tfsdk:"observed_generation"`
	Timeouts               timeouts.Value `tfsdk:"timeouts"`
}

// fleetWire is the server's Fleet. It is read with its own type rather than the
// generated one: the spec types workloads as a string though the API sends an
// object, and the generated enum type names change with their values.
type fleetWire struct {
	Identifier          string          `json:"identifier"`
	Provider            string          `json:"provider"`
	Domain              string          `json:"domain"`
	Network             *string         `json:"network"`
	TenancyMode         *string         `json:"tenancy_mode"`
	MaxTenants          *int64          `json:"max_tenants"`
	MaxKafkaUnits       *int64          `json:"max_kafka_units"`
	Autoscale           *bool           `json:"autoscale"`
	EnableAI            *bool           `json:"enable_ai"`
	StreamtimeAIAccess  *string         `json:"streamtime_ai_access"`
	Workloads           json.RawMessage `json:"workloads"`
	PlacementConfig     json.RawMessage `json:"placement_config"`
	AdvancedConfig      json.RawMessage `json:"advanced_config"`
	Config              *fleetConfig    `json:"config"`
	Status              *string         `json:"status"`
	ReconciliationState *string         `json:"reconciliation_state"`
	ConfigurationState  *string         `json:"configuration_state"`
	DesiredLifecycle    *string         `json:"desired_lifecycle"`
	Generation          int64           `json:"generation"`
	ObservedGeneration  int64           `json:"observed_generation"`
}

type fleetConfig struct {
	AlertChannels          []string `json:"alert_channels"`
	ObservabilityProviders []string `json:"observability_providers"`
}

// Polling bounds while a fleet is created or removed.
var fleetPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w fleetWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *fleetResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_fleet"
}

func (r *fleetResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	keepInt := []planmodifier.Int64{int64planmodifier.UseStateForUnknown()}
	keepBool := []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()}
	keepSet := []planmodifier.Set{setplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A fleet: the Kubernetes cluster Streamtime runs workloads on. Creating one provisions the cluster " +
			"(or, for bring-your-own Kubernetes, registers it; install the Agent afterwards) and waits until it is ready. " +
			"Use streamtime_fleet_configuration_profile to see the placement and advanced settings each fleet provider takes.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name of the fleet: up to 24 lowercase letters, digits and dashes. Changing it creates a new fleet."},
			"fleet_provider": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Where the fleet runs, for example aws, gcp, azure, oci, digitalocean, civo, byok (your own Kubernetes) " +
					"or byovm (your own virtual machines). See streamtime_fleet_configuration_profiles. Changing it creates a new fleet."},
			"domain": schema.StringAttribute{Required: true,
				Description: "Base domain the fleet's endpoints are served under (a streamtime_base_domain's domain)."},
			"network": schema.StringAttribute{Optional: true, PlanModifiers: []planmodifier.String{networkReplace},
				Description: "Identifier of the streamtime_network to create the fleet in. Changing it creates a new fleet. " +
					"If Streamtime no longer links the fleet to its network (the network was deleted and created again), " +
					"apply links it again in place."},
			"tenancy_mode": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Shared, Isolated or Dedicated."},
			"max_tenants": schema.Int64Attribute{Optional: true, Computed: true, PlanModifiers: keepInt,
				Description: "Most workloads that can share the fleet."},
			"max_kafka_units": schema.Int64Attribute{Optional: true, Computed: true, PlanModifiers: keepInt,
				Description: "Cluster capacity the fleet can hold, in capacity units."},
			"autoscale": schema.BoolAttribute{Optional: true, Computed: true, PlanModifiers: keepBool,
				Description: "Let the fleet's node pools grow and shrink with load, where the cloud supports it."},
			"enable_ai": schema.BoolAttribute{Optional: true, Computed: true, PlanModifiers: keepBool,
				Description: "Turn on the AI assistant for this fleet."},
			"streamtime_ai_access": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "What the AI assistant may do on the fleet: read_only or read_write."},
			"workloads": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Workload categories the fleet hosts, as a JSON object set with jsonencode(): streaming, " +
					"stream_processing and analytics, each with enabled, providers and units (cloud fleets) or weight (your own Kubernetes)."},
			"placement_config": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Where and how the fleet is placed (region, sizes, versions), as a JSON object set with jsonencode(). " +
					"The fields depend on fleet_provider. Settings Streamtime fills in are not shown as changes."},
			"advanced_config": schema.StringAttribute{Optional: true, Computed: true, Sensitive: true, PlanModifiers: keepStr,
				Description: "Advanced settings, as a JSON object set with jsonencode(). The fields depend on fleet_provider and " +
					"may include credentials. Changes to placement_config or advanced_config are saved but not yet applied to a running fleet."},
			"alert_channels": schema.SetAttribute{ElementType: types.StringType, Optional: true, Computed: true, PlanModifiers: keepSet,
				Description: "Identifiers of alert channels that receive the fleet's alerts."},
			"observability_providers": schema.SetAttribute{ElementType: types.StringType, Optional: true, Computed: true, PlanModifiers: keepSet,
				Description: "Identifiers of observability accounts the fleet sends metrics and logs to."},
			"alert_webhook_secret": schema.StringAttribute{Optional: true, Sensitive: true,
				Description: "Secret used to sign the fleet's alert webhooks. Streamtime never returns it."},
			"status": schema.StringAttribute{Computed: true, Description: "Health of the fleet, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true,
				Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the fleet's settings are complete: incomplete, valid or invalid."},
			"generation":          schema.Int64Attribute{Computed: true, Description: "Version of the fleet's settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings the running fleet has."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Delete: true}),
		},
	}
}

// networkReplace replaces a fleet only when it moves from one network to another:
// a running cluster cannot change networks. When Streamtime has dropped the link
// (deleting a network clears it on its fleets), the state reads no network and
// the plan re-links it in place instead of destroying the cluster.
var networkReplace = stringplanmodifier.RequiresReplaceIf(
	func(_ context.Context, req planmodifier.StringRequest, resp *stringplanmodifier.RequiresReplaceIfFuncResponse) {
		resp.RequiresReplace = !req.StateValue.IsNull() && !req.StateValue.IsUnknown()
	},
	"Changing the network creates a new fleet; re-linking a fleet whose link was dropped is an in-place update.",
	"Changing the network creates a new fleet; re-linking a fleet whose link was dropped is an in-place update.",
)

func (r *fleetResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func fleetPath(id string) string { return "fleet/" + url.PathEscape(id) + "/" }

// networkUUID resolves a network identifier to the ID the fleet API takes.
func networkUUID(ctx context.Context, c *client.Client, identifier string) (string, error) {
	var n struct {
		ID string `json:"id"`
	}
	if err := c.Do(ctx, "GET", networkPath(identifier), nil, &n); err != nil {
		if isGone(err) {
			return "", fmt.Errorf("network %q does not exist in this organization", identifier)
		}
		return "", err
	}
	return n.ID, nil
}

// networkIdentifier finds the identifier of the network with the given ID. The
// state names networks by identifier, never by ID.
func networkIdentifier(ctx context.Context, c *client.Client, id, hint string) (types.String, error) {
	if hint != "" {
		if got, err := networkUUID(ctx, c, hint); err == nil && got == id {
			return types.StringValue(hint), nil
		}
	}
	items, err := listAll(ctx, c, "networks/")
	if err != nil {
		return types.StringNull(), err
	}
	for _, raw := range items {
		var n struct {
			ID         string `json:"id"`
			Identifier string `json:"identifier"`
		}
		if json.Unmarshal(raw, &n) == nil && n.ID == id {
			return types.StringValue(n.Identifier), nil
		}
	}
	return types.StringNull(), nil
}

func setOf(ctx context.Context, s types.Set) ([]string, diag.Diagnostics) {
	if s.IsNull() || s.IsUnknown() {
		return nil, nil
	}
	var out []string
	d := s.ElementsAs(ctx, &out, false)
	return out, d
}

func (r *fleetResource) body(ctx context.Context, m fleetModel) (map[string]any, error) {
	b := map[string]any{
		"identifier": m.Identifier.ValueString(),
		"provider":   m.FleetProvider.ValueString(),
		"domain":     m.Domain.ValueString(),
	}
	if n := m.Network.ValueString(); n != "" && !m.Network.IsUnknown() {
		id, err := networkUUID(ctx, r.c, n)
		if err != nil {
			return nil, err
		}
		b["network"] = id
	}
	for k, v := range map[string]types.String{"tenancy_mode": m.TenancyMode, "streamtime_ai_access": m.StreamtimeAIAccess,
		"alert_webhook_secret": m.AlertWebhookSecret} {
		if p := optString(v); p != nil && *p != "" {
			b[k] = *p
		}
	}
	for k, v := range map[string]types.Int64{"max_tenants": m.MaxTenants, "max_kafka_units": m.MaxKafkaUnits} {
		if !v.IsNull() && !v.IsUnknown() {
			b[k] = v.ValueInt64()
		}
	}
	for k, v := range map[string]types.Bool{"autoscale": m.Autoscale, "enable_ai": m.EnableAI} {
		if !v.IsNull() && !v.IsUnknown() {
			b[k] = v.ValueBool()
		}
	}
	for k, v := range map[string]types.String{"workloads": m.Workloads, "placement_config": m.PlacementConfig, "advanced_config": m.AdvancedConfig} {
		obj, err := jsonObject(v, k)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			b[k] = obj
		}
	}
	cfg := map[string]any{}
	for k, v := range map[string]types.Set{"alert_channels": m.AlertChannels, "observability_providers": m.ObservabilityProviders} {
		list, d := setOf(ctx, v)
		if d.HasError() {
			return nil, fmt.Errorf("%s: %v", k, d)
		}
		if list != nil {
			cfg[k] = list
		}
	}
	if len(cfg) > 0 {
		b["config"] = cfg
	}
	return b, nil
}

func stringOrNull(p *string) types.String {
	if p == nil {
		return types.StringNull()
	}
	return types.StringValue(*p)
}

func stringSet(list []string) types.Set {
	vals := make([]attr.Value, 0, len(list))
	for _, s := range list {
		vals = append(vals, types.StringValue(s))
	}
	return types.SetValueMust(types.StringType, vals)
}

// apply copies the server's view into state. JSON settings keep the configured
// text while the server holds what it sets; the webhook secret is never returned.
func (m *fleetModel) apply(ctx context.Context, c *client.Client, a fleetWire) error {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.FleetProvider = types.StringValue(a.Provider)
	m.Domain = types.StringValue(a.Domain)
	if a.Network == nil || *a.Network == "" {
		m.Network = types.StringNull()
	} else {
		hint := ""
		if !m.Network.IsNull() && !m.Network.IsUnknown() {
			hint = m.Network.ValueString()
		}
		n, err := networkIdentifier(ctx, c, *a.Network, hint)
		if err != nil {
			return err
		}
		m.Network = n
	}
	m.TenancyMode = stringOrNull(a.TenancyMode)
	m.StreamtimeAIAccess = stringOrNull(a.StreamtimeAIAccess)
	m.MaxTenants, m.MaxKafkaUnits = types.Int64PointerValue(a.MaxTenants), types.Int64PointerValue(a.MaxKafkaUnits)
	m.Autoscale, m.EnableAI = types.BoolPointerValue(a.Autoscale), types.BoolPointerValue(a.EnableAI)
	m.Workloads = keptOrServerJSON(m.Workloads, a.Workloads, false)
	m.PlacementConfig = keptOrServerJSON(m.PlacementConfig, a.PlacementConfig, false)
	m.AdvancedConfig = keptOrServerJSON(m.AdvancedConfig, a.AdvancedConfig, true)
	var cfg fleetConfig
	if a.Config != nil {
		cfg = *a.Config
	}
	m.AlertChannels, m.ObservabilityProviders = stringSet(cfg.AlertChannels), stringSet(cfg.ObservabilityProviders)
	if m.AlertWebhookSecret.IsUnknown() {
		m.AlertWebhookSecret = types.StringNull()
	}
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
	return nil
}

func idempotencyKey(op, identifier string, generation int64) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("terraform-%s-%s-%d-%s", op, identifier, generation, hex.EncodeToString(b))
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (r *fleetResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m fleetModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 90*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the fleet", err.Error())
		return
	}
	var out fleetWire
	if err := r.c.Do(ctx, "POST", "fleet/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the fleet", err.Error())
		return
	}
	// The fleet exists from here on: record it, so a failure below leaves it in
	// state (marked for replacement) rather than unmanaged.
	save := func() {
		if err := m.apply(ctx, r.c, out); err != nil {
			resp.Diagnostics.AddError("Could not read the fleet", err.Error())
		}
		resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	}
	save()
	if resp.Diagnostics.HasError() {
		return
	}

	// Saving the fleet does not start it; reconcile provisions the saved settings.
	headers := map[string]string{"Idempotency-Key": idempotencyKey("create", out.Identifier, out.Generation)}
	if err := r.c.DoWithHeaders(ctx, "POST", fleetPath(out.Identifier)+"reconcile/", headers,
		map[string]any{"generation": out.Generation}, nil); err != nil {
		resp.Diagnostics.AddError("Could not start creating the fleet", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := waitGeneration(wctx, r.c, fleetPath(out.Identifier), "fleet", out.Generation, fleetPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The fleet was not created", werr.Error())
	}
}

func (r *fleetResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m fleetModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out fleetWire
	if err := r.c.Do(ctx, "GET", fleetPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the fleet", err.Error())
		return
	}
	if err := m.apply(ctx, r.c, out); err != nil {
		resp.Diagnostics.AddError("Could not read the fleet", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *fleetResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior fleetModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the fleet", err.Error())
		return
	}
	// The server never returns the webhook secret, and leaving it out of a PUT keeps
	// the stored one. A secret removed from the configuration is cleared explicitly,
	// or the old signing secret would stay active while the state says there is none.
	if m.AlertWebhookSecret.IsNull() && !prior.AlertWebhookSecret.IsNull() && prior.AlertWebhookSecret.ValueString() != "" {
		body["alert_webhook_secret"] = nil
	}
	var out fleetWire
	if err := r.c.Do(ctx, "PUT", fleetPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the fleet", err.Error())
		return
	}
	if err := m.apply(ctx, r.c, out); err != nil {
		resp.Diagnostics.AddError("Could not read the fleet", err.Error())
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	// A placement or advanced change raises the generation. Streamtime cannot yet
	// apply it to a running fleet, so it is saved and the running fleet stays as it is.
	if !prior.Generation.IsNull() && out.Generation > prior.Generation.ValueInt64() {
		resp.Diagnostics.AddWarning("Fleet settings saved but not applied",
			"The new placement or advanced settings of fleet "+out.Identifier+" are saved, but Streamtime cannot yet apply "+
				"them to a running fleet, so the fleet keeps running with its current settings. To run with the new "+
				"settings now, recreate the fleet: terraform apply -replace=<resource address of this fleet>.")
	}
}

func (r *fleetResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m fleetModel
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
	if err := r.c.Do(ctx, "DELETE", fleetPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the fleet", err.Error())
		return
	}
	// Deleting starts the teardown; the fleet is gone once Streamtime has removed it.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[fleetWire](wctx, r.c, fleetPath(id), "fleet", fleetPoll); err != nil {
		resp.Diagnostics.AddError("The fleet was not deleted", err.Error())
	}
}

func (r *fleetResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
