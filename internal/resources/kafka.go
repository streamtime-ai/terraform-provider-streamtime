package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-timeouts/resource/timeouts"
	"github.com/hashicorp/terraform-plugin-framework/attr"
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

// NewKafka returns the streamtime_kafka resource.
func NewKafka() resource.Resource { return &kafkaResource{} }

type kafkaResource struct{ c *client.Client }

type kafkaModel struct {
	ID                  types.String   `tfsdk:"id"`
	Identifier          types.String   `tfsdk:"identifier"`
	Workload            types.String   `tfsdk:"workload"`
	Fleet               types.String   `tfsdk:"fleet"`
	KafkaUnits          types.Int64    `tfsdk:"kafka_units"`
	TenancyMode         types.String   `tfsdk:"tenancy_mode"`
	CloudProvider       types.String   `tfsdk:"cloud_provider"`
	Region              types.String   `tfsdk:"region"`
	OptimizationGoal    types.String   `tfsdk:"optimization_goal"`
	RetentionDays       types.Int64    `tfsdk:"retention_days"`
	RetentionMode       types.String   `tfsdk:"retention_mode"`
	Tags                types.Map      `tfsdk:"tags"`
	Metadata            types.String   `tfsdk:"metadata"`
	AdvancedConfig      types.String   `tfsdk:"advanced_config"`
	AlertWebhookSecret  types.String   `tfsdk:"alert_webhook_secret"`
	Spec                types.String   `tfsdk:"spec"`
	Status              types.String   `tfsdk:"status"`
	ReconciliationState types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState  types.String   `tfsdk:"configuration_state"`
	Generation          types.Int64    `tfsdk:"generation"`
	ObservedGeneration  types.Int64    `tfsdk:"observed_generation"`
	Timeouts            timeouts.Value `tfsdk:"timeouts"`
}

// kafkaWire is the server's Kafka cluster (the Kafka component of the spec).
type kafkaWire struct {
	Identifier          string          `json:"identifier"`
	Workload            string          `json:"workload"`
	Fleet               *string         `json:"fleet"`
	KafkaUnits          int64           `json:"kafka_units"`
	TenancyMode         *string         `json:"tenancy_mode"`
	CloudProvider       *string         `json:"cloud_provider"`
	Region              *string         `json:"region"`
	OptimizationGoal    *string         `json:"optimization_goal"`
	RetentionDays       *int64          `json:"retention_days"`
	Tags                []kafkaTag      `json:"tags"`
	Metadata            json.RawMessage `json:"metadata"`
	AdvancedConfig      json.RawMessage `json:"advanced_config"`
	Spec                json.RawMessage `json:"spec"`
	Status              *string         `json:"status"`
	ReconciliationState *string         `json:"reconciliation_state"`
	ConfigurationState  *string         `json:"configuration_state"`
	DesiredLifecycle    *string         `json:"desired_lifecycle"`
	Generation          int64           `json:"generation"`
	ObservedGeneration  int64           `json:"observed_generation"`
}

type kafkaTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Polling bounds while a Kafka cluster is created or removed.
var kafkaPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w kafkaWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *kafkaResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka"
}

func (r *kafkaResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	keepInt := []planmodifier.Int64{int64planmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A Kafka cluster on a fleet. Creating one deploys the cluster and waits until it is ready. " +
			"Changes to its settings are saved, but a running cluster keeps its current settings until " +
			"Streamtime can apply changes to running clusters.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name of the cluster: up to 24 lowercase letters, digits and dashes. Changing it creates a new cluster."},
			"workload": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Kafka provider of the cluster, for example strimzi, cfk, redpanda, warpstream or cpansible " +
					"(Confluent Platform on your own virtual machines). Changing it creates a new cluster."},
			"fleet": schema.StringAttribute{Optional: true, Computed: true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown(), stringplanmodifier.RequiresReplace()},
				Description: "Identifier of the fleet to deploy the cluster on. Leave it out to let Streamtime choose a fleet " +
					"with room for the cluster's capacity, tenancy mode and cloud. Changing it creates a new cluster."},
			"kafka_units": schema.Int64Attribute{Required: true,
				Description: "Cluster capacity, in capacity units."},
			"tenancy_mode": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Shared, Isolated or Dedicated."},
			"cloud_provider": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Cloud the cluster runs in, used to choose a fleet."},
			"region": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Region the cluster runs in, used to choose a fleet."},
			"optimization_goal": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "What the cluster's sizing favours, for example throughput or latency."},
			"retention_days": schema.Int64Attribute{Optional: true, Computed: true, PlanModifiers: keepInt,
				Description: "Days messages are kept, when retention is finite."},
			"retention_mode": schema.StringAttribute{Optional: true,
				Description: "Finite or Infinite. Streamtime does not return it, so a change made outside Terraform is not shown."},
			"tags": schema.MapAttribute{ElementType: types.StringType, Optional: true, Computed: true,
				PlanModifiers: []planmodifier.Map{mapplanmodifier.UseStateForUnknown()},
				Description:   "Tags on the cluster, as key = value. Removing the attribute keeps the current tags; set tags = {} to clear them."},
			"metadata": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Free-form metadata, as a JSON object set with jsonencode(). Values Streamtime adds are not shown as changes."},
			"advanced_config": schema.StringAttribute{Optional: true, Computed: true, Sensitive: true, PlanModifiers: keepStr,
				Description: "Advanced settings, as a JSON object set with jsonencode(): component versions, values and " +
					"authentication. The fields depend on workload and may include credentials. Settings Streamtime fills in are not shown as changes."},
			"alert_webhook_secret": schema.StringAttribute{Optional: true, Sensitive: true,
				Description: "Secret used to sign the cluster's alert webhooks. Streamtime never returns it."},
			"spec": schema.StringAttribute{Computed: true,
				Description: "What Streamtime reports about the running cluster (bootstrap servers, listeners, version), as JSON. " +
					"Read it with jsondecode()."},
			"status": schema.StringAttribute{Computed: true, Description: "Health of the cluster, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true,
				Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the cluster's settings are complete: incomplete, valid or invalid."},
			"generation":          schema.Int64Attribute{Computed: true, Description: "Version of the cluster's settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings the running cluster has."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Delete: true}),
		},
	}
}

func (r *kafkaResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func kafkaPath(id string) string { return "kafka/" + url.PathEscape(id) + "/" }

func (r *kafkaResource) body(ctx context.Context, m kafkaModel) (map[string]any, error) {
	b := map[string]any{
		"identifier":  m.Identifier.ValueString(),
		"workload":    m.Workload.ValueString(),
		"kafka_units": m.KafkaUnits.ValueInt64(),
	}
	for k, v := range map[string]types.String{"fleet": m.Fleet, "tenancy_mode": m.TenancyMode, "cloud_provider": m.CloudProvider,
		"region": m.Region, "optimization_goal": m.OptimizationGoal, "retention_mode": m.RetentionMode,
		"alert_webhook_secret": m.AlertWebhookSecret} {
		if p := optString(v); p != nil && *p != "" {
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
	meta, err := jsonObject(m.Metadata, "metadata")
	if err != nil {
		return nil, err
	}
	if meta != nil {
		b["metadata"] = meta
	}
	adv, err := jsonObject(m.AdvancedConfig, "advanced_config")
	if err != nil {
		return nil, err
	}
	// Always sent: Streamtime treats a create without advanced_config as an
	// unfinished draft, which it neither places on a fleet nor deploys.
	if adv == nil {
		adv = map[string]any{}
	}
	b["advanced_config"] = adv
	return b, nil
}

// tagList is a tags map as the API's key/value list, sorted by key.
func tagList(ctx context.Context, m types.Map) ([]kafkaTag, error) {
	var tags map[string]string
	if d := m.ElementsAs(ctx, &tags, false); d.HasError() {
		return nil, fmt.Errorf("tags: %v", d)
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	list := make([]kafkaTag, 0, len(keys))
	for _, k := range keys {
		list = append(list, kafkaTag{Key: k, Value: tags[k]})
	}
	return list, nil
}

func tagMap(tags []kafkaTag) types.Map {
	vals := make(map[string]attr.Value, len(tags))
	for _, t := range tags {
		vals[t.Key] = types.StringValue(t.Value)
	}
	return types.MapValueMust(types.StringType, vals)
}

// apply copies the server's view into state. JSON settings keep the configured
// text while the server holds what it sets; write-only fields keep the plan.
func (m *kafkaModel) apply(a kafkaWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.Workload = types.StringValue(a.Workload)
	m.Fleet = stringOrNull(a.Fleet)
	m.KafkaUnits = types.Int64Value(a.KafkaUnits)
	m.TenancyMode = stringOrNull(a.TenancyMode)
	m.CloudProvider = stringOrNull(a.CloudProvider)
	m.Region = stringOrNull(a.Region)
	m.OptimizationGoal = stringOrNull(a.OptimizationGoal)
	m.RetentionDays = types.Int64PointerValue(a.RetentionDays)
	m.Tags = tagMap(a.Tags)
	m.Metadata = keptOrServerJSON(m.Metadata, a.Metadata, false)
	m.AdvancedConfig = keptOrServerJSON(m.AdvancedConfig, a.AdvancedConfig, true)
	m.Spec = canonicalJSON(a.Spec)
	if m.RetentionMode.IsUnknown() {
		m.RetentionMode = types.StringNull()
	}
	if m.AlertWebhookSecret.IsUnknown() {
		m.AlertWebhookSecret = types.StringNull()
	}
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

func (r *kafkaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m kafkaModel
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
		resp.Diagnostics.AddError("Could not create the Kafka cluster", err.Error())
		return
	}
	var out kafkaWire
	if err := r.c.Do(ctx, "POST", "kafka/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the Kafka cluster", err.Error())
		return
	}
	// The cluster exists from here on: record it, so a failure below leaves it in
	// state (marked for replacement) rather than unmanaged.
	save := func() {
		m.apply(out)
		resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	}
	save()
	if resp.Diagnostics.HasError() {
		return
	}

	// Saving the cluster does not deploy it; reconcile deploys the saved settings.
	headers := map[string]string{"Idempotency-Key": idempotencyKey("create", out.Identifier, out.Generation)}
	if err := r.c.DoWithHeaders(ctx, "POST", kafkaPath(out.Identifier)+"reconcile/", headers,
		map[string]any{"generation": out.Generation}, nil); err != nil {
		resp.Diagnostics.AddError("Could not start creating the Kafka cluster", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := waitGeneration(wctx, r.c, kafkaPath(out.Identifier), "Kafka cluster", out.Generation, kafkaPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The Kafka cluster was not created", werr.Error())
	}
}

func (r *kafkaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m kafkaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out kafkaWire
	if err := r.c.Do(ctx, "GET", kafkaPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the Kafka cluster", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior kafkaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(ctx, m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the Kafka cluster", err.Error())
		return
	}
	// Leaving the webhook secret out of a PUT keeps the stored one, so a secret
	// removed from the configuration is cleared explicitly.
	if m.AlertWebhookSecret.IsNull() && !prior.AlertWebhookSecret.IsNull() && prior.AlertWebhookSecret.ValueString() != "" {
		body["alert_webhook_secret"] = nil
	}
	var out kafkaWire
	if err := r.c.Do(ctx, "PUT", kafkaPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the Kafka cluster", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	// A change to what is deployed raises the generation. Streamtime cannot yet
	// apply it to a running cluster, so it is saved and the cluster stays as it is.
	if !prior.Generation.IsNull() && out.Generation > prior.Generation.ValueInt64() {
		resp.Diagnostics.AddWarning("Kafka cluster settings saved but not applied",
			"The new settings of Kafka cluster "+out.Identifier+" are saved, but Streamtime cannot yet apply them to a "+
				"running cluster, so it keeps running with its current settings. To run with the new settings now, "+
				"recreate the cluster: terraform apply -replace=<resource address of this cluster>. Recreating it deletes its data.")
	}
}

func (r *kafkaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m kafkaModel
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
	if err := r.c.Do(ctx, "DELETE", kafkaPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the Kafka cluster", err.Error())
		return
	}
	// Deleting starts the teardown; the cluster is gone once Streamtime has removed it.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[kafkaWire](wctx, r.c, kafkaPath(id), "Kafka cluster", kafkaPoll); err != nil {
		resp.Diagnostics.AddError("The Kafka cluster was not deleted", err.Error())
	}
}

func (r *kafkaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
