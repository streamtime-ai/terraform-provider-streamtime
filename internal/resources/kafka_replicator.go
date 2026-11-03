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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewKafkaReplicator returns the streamtime_kafka_replicator resource.
func NewKafkaReplicator() resource.Resource { return &kafkaReplicatorResource{} }

type kafkaReplicatorResource struct{ c *client.Client }

type kafkaReplicatorModel struct {
	ID                           types.String   `tfsdk:"id"`
	Identifier                   types.String   `tfsdk:"identifier"`
	ReplicatorProvider           types.String   `tfsdk:"replicator_provider"`
	SourceKafkaID                types.String   `tfsdk:"source_kafka_id"`
	SourceConnectionDetails      types.String   `tfsdk:"source_connection_details"`
	DestinationKafkaID           types.String   `tfsdk:"destination_kafka_id"`
	DestinationConnectionDetails types.String   `tfsdk:"destination_connection_details"`
	SourceSecretPath             types.String   `tfsdk:"source_secret_path"`
	DestinationSecretPath        types.String   `tfsdk:"destination_secret_path"`
	ReplicationMode              types.String   `tfsdk:"replication_mode"`
	LagAlertThreshold            types.Float64  `tfsdk:"lag_alert_threshold"`
	LagAlertThresholdUnit        types.String   `tfsdk:"lag_alert_threshold_unit"`
	TopicWhitelist               types.String   `tfsdk:"topic_whitelist"`
	TopicBlacklist               types.String   `tfsdk:"topic_blacklist"`
	GroupsWhitelist              types.String   `tfsdk:"groups_whitelist"`
	GroupsBlacklist              types.String   `tfsdk:"groups_blacklist"`
	SourceInitiatedLink          types.Bool     `tfsdk:"source_initiated_link"`
	LinkMode                     types.String   `tfsdk:"link_mode"`
	MirrorMaker2Settings         types.String   `tfsdk:"mirrormaker2_advanced_settings"`
	ClusterLinkSettings          types.String   `tfsdk:"cluster_link_advanced_settings"`
	Fleet                        types.String   `tfsdk:"fleet"`
	Warnings                     types.String   `tfsdk:"warnings"`
	Status                       types.String   `tfsdk:"status"`
	ReconciliationState          types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState           types.String   `tfsdk:"configuration_state"`
	Generation                   types.Int64    `tfsdk:"generation"`
	ObservedGeneration           types.Int64    `tfsdk:"observed_generation"`
	Timeouts                     timeouts.Value `tfsdk:"timeouts"`
}

// kafkaReplicatorWire is the server's replicator. A request names the Kafka
// clusters source_kafka_id / destination_kafka_id; the response, source_kafka /
// destination_kafka.
type kafkaReplicatorWire struct {
	Identifier              string          `json:"identifier"`
	Provider                string          `json:"provider"`
	SourceKafka             *string         `json:"source_kafka"`
	DestinationKafka        *string         `json:"destination_kafka"`
	SourceConnectionDetails json.RawMessage `json:"source_connection_details"`
	DestConnectionDetails   json.RawMessage `json:"destination_connection_details"`
	SourceSecretPath        *string         `json:"source_secret_path"`
	DestinationSecretPath   *string         `json:"destination_secret_path"`
	ReplicationMode         string          `json:"replication_mode"`
	LagAlertThreshold       float64         `json:"lag_alert_threshold"`
	LagAlertThresholdUnit   string          `json:"lag_alert_threshold_unit"`
	TopicWhitelist          *string         `json:"topic_whitelist"`
	TopicBlacklist          *string         `json:"topic_blacklist"`
	GroupsWhitelist         *string         `json:"groups_whitelist"`
	GroupsBlacklist         *string         `json:"groups_blacklist"`
	SourceInitiatedLink     bool            `json:"source_initiated_link"`
	LinkMode                *string         `json:"link_mode"`
	MirrorMaker2Settings    json.RawMessage `json:"mirrormaker2_advanced_settings"`
	ClusterLinkSettings     json.RawMessage `json:"cluster_link_advanced_settings"`
	Fleet                   *string         `json:"fleet"`
	Warnings                json.RawMessage `json:"warnings"`
	Status                  *string         `json:"status"`
	ReconciliationState     *string         `json:"reconciliation_state"`
	ConfigurationState      *string         `json:"configuration_state"`
	DesiredLifecycle        *string         `json:"desired_lifecycle"`
	Generation              int64           `json:"generation"`
	ObservedGeneration      int64           `json:"observed_generation"`
}

var kafkaReplicatorPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w kafkaReplicatorWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *kafkaReplicatorResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka_replicator"
}

func (r *kafkaReplicatorResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	optStr := func(desc string) schema.StringAttribute {
		return schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr, Description: desc}
	}
	resp.Schema = schema.Schema{
		Description: "Copies topics and consumer groups from one Kafka cluster to another. Creating a replicator " +
			"deploys it and waits until it is ready. Changes to its settings are saved, but a running replicator " +
			"keeps its current settings until Streamtime can apply changes to running replicators.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name: up to 24 lowercase letters, digits and dashes. Changing it creates a new replicator."},
			"replicator_provider": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "How data is copied: MirrorMaker2 or Cluster Link. Changing it creates a new replicator."},
			"source_kafka_id": optStr("Identifier of the Streamtime Kafka cluster to copy from. Leave it out for an external " +
				"cluster and set source_connection_details."),
			"source_connection_details": optStr("How to reach an external source cluster, as a JSON object set with jsonencode(): " +
				"bootstrap_servers, security_protocol, and for SASL sasl_mechanism (and cluster_id for Cluster Link)."),
			"destination_kafka_id": optStr("Identifier of the Streamtime Kafka cluster to copy to. Leave it out for an external " +
				"cluster and set destination_connection_details."),
			"destination_connection_details": optStr("How to reach an external destination cluster, as a JSON object set with " +
				"jsonencode(), with the same fields as source_connection_details."),
			"source_secret_path": schema.StringAttribute{Required: true,
				Description: "Where the credentials for the source cluster are stored, as a secret URI, for example " +
					"builtin:///replicators/<identifier>/source-secret. The secret must already exist."},
			"destination_secret_path": schema.StringAttribute{Required: true,
				Description: "Where the credentials for the destination cluster are stored, as a secret URI. The secret must already exist."},
			"replication_mode": schema.StringAttribute{Required: true,
				Description: "active-passive or active-active."},
			"lag_alert_threshold": schema.Float64Attribute{Required: true,
				Description: "Replication lag that raises an alert, in the unit of lag_alert_threshold_unit."},
			"lag_alert_threshold_unit": schema.StringAttribute{Required: true,
				Description: "Unit of the lag threshold: messages, bytes or seconds."},
			"topic_whitelist":  optStr("Topics to copy, as a comma-separated list or a pattern."),
			"topic_blacklist":  optStr("Topics not to copy, as a comma-separated list or a pattern."),
			"groups_whitelist": optStr("Consumer groups to copy, as a comma-separated list or a pattern."),
			"groups_blacklist": optStr("Consumer groups not to copy, as a comma-separated list or a pattern."),
			"source_initiated_link": schema.BoolAttribute{Optional: true, Computed: true,
				PlanModifiers: []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()},
				Description:   "Whether the link is started from the source cluster (Cluster Link only). Requires link_mode."},
			"link_mode":                      optStr("Bidirectional, Destination or Source. Required when source_initiated_link is true."),
			"mirrormaker2_advanced_settings": optStr("MirrorMaker2 settings, as a JSON object set with jsonencode()."),
			"cluster_link_advanced_settings": optStr("Cluster Link settings, as a JSON object set with jsonencode()."),
			"fleet": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Identifier of the fleet the replicator runs on, chosen from its clusters."},
			"warnings": schema.StringAttribute{Computed: true,
				Description: "Problems Streamtime found with where the replicator runs, as JSON. Read it with jsondecode()."},
			"status": schema.StringAttribute{Computed: true,
				Description: "Health of the replicator, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true,
				Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the settings are complete: incomplete, valid or invalid."},
			"generation":          schema.Int64Attribute{Computed: true, Description: "Version of the settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings that is running."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Delete: true}),
		},
	}
}

func (r *kafkaReplicatorResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func kafkaReplicatorPath(id string) string { return "kafka-replicator/" + url.PathEscape(id) + "/" }

// body is the request for create and update. Unset optional settings are left out, so
// the server keeps (or defaults) them.
func (m kafkaReplicatorModel) body(create bool) (map[string]any, error) {
	b := map[string]any{
		// Create and update both validate the full representation.
		"identifier":               m.Identifier.ValueString(),
		"replication_mode":         m.ReplicationMode.ValueString(),
		"lag_alert_threshold":      m.LagAlertThreshold.ValueFloat64(),
		"lag_alert_threshold_unit": m.LagAlertThresholdUnit.ValueString(),
		"source_secret_path":       m.SourceSecretPath.ValueString(),
		"destination_secret_path":  m.DestinationSecretPath.ValueString(),
	}
	if create {
		b["provider"] = m.ReplicatorProvider.ValueString()
	}
	// The API's request field for a Streamtime cluster is source_kafka / destination_kafka
	// (its create schema says *_id, which it does not read).
	for k, v := range map[string]types.String{"source_kafka": m.SourceKafkaID, "destination_kafka": m.DestinationKafkaID,
		"topic_whitelist": m.TopicWhitelist, "topic_blacklist": m.TopicBlacklist,
		"groups_whitelist": m.GroupsWhitelist, "groups_blacklist": m.GroupsBlacklist, "link_mode": m.LinkMode} {
		if p := optString(v); p != nil && *p != "" {
			b[k] = *p
		}
	}
	if !m.SourceInitiatedLink.IsNull() && !m.SourceInitiatedLink.IsUnknown() {
		b["source_initiated_link"] = m.SourceInitiatedLink.ValueBool()
	}
	for k, v := range map[string]types.String{"source_connection_details": m.SourceConnectionDetails,
		"destination_connection_details": m.DestinationConnectionDetails,
		"mirrormaker2_advanced_settings": m.MirrorMaker2Settings, "cluster_link_advanced_settings": m.ClusterLinkSettings} {
		obj, err := jsonObject(v, k)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			b[k] = obj
		}
	}
	return b, nil
}

func (m *kafkaReplicatorModel) apply(a kafkaReplicatorWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.ReplicatorProvider = types.StringValue(a.Provider)
	m.SourceKafkaID = stringOrNull(a.SourceKafka)
	m.DestinationKafkaID = stringOrNull(a.DestinationKafka)
	m.SourceConnectionDetails = keptOrServerJSON(m.SourceConnectionDetails, a.SourceConnectionDetails, false)
	m.DestinationConnectionDetails = keptOrServerJSON(m.DestinationConnectionDetails, a.DestConnectionDetails, false)
	m.SourceSecretPath = stringOrNull(a.SourceSecretPath)
	m.DestinationSecretPath = stringOrNull(a.DestinationSecretPath)
	m.ReplicationMode = types.StringValue(a.ReplicationMode)
	m.LagAlertThreshold = types.Float64Value(a.LagAlertThreshold)
	m.LagAlertThresholdUnit = types.StringValue(a.LagAlertThresholdUnit)
	m.TopicWhitelist, m.TopicBlacklist = stringOrNull(a.TopicWhitelist), stringOrNull(a.TopicBlacklist)
	m.GroupsWhitelist, m.GroupsBlacklist = stringOrNull(a.GroupsWhitelist), stringOrNull(a.GroupsBlacklist)
	m.SourceInitiatedLink = types.BoolValue(a.SourceInitiatedLink)
	m.LinkMode = stringOrNull(a.LinkMode)
	m.MirrorMaker2Settings = keptOrServerJSON(m.MirrorMaker2Settings, a.MirrorMaker2Settings, false)
	m.ClusterLinkSettings = keptOrServerJSON(m.ClusterLinkSettings, a.ClusterLinkSettings, false)
	m.Fleet = stringOrNull(a.Fleet)
	m.Warnings = canonicalJSON(a.Warnings)
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

func (r *kafkaReplicatorResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m kafkaReplicatorModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := m.body(true)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the replicator", err.Error())
		return
	}
	var out kafkaReplicatorWire
	if err := r.c.Do(ctx, "POST", "kafka-replicator/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the replicator", err.Error())
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
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := reconcileAndWait(wctx, r.c, kafkaReplicatorPath(out.Identifier), "replicator", "create", out.Identifier, out.Generation, kafkaReplicatorPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The replicator was not created", werr.Error())
	}
}

func (r *kafkaReplicatorResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m kafkaReplicatorModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out kafkaReplicatorWire
	if err := r.c.Do(ctx, "GET", kafkaReplicatorPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the replicator", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaReplicatorResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior kafkaReplicatorModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := m.body(false)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the replicator", err.Error())
		return
	}
	var out kafkaReplicatorWire
	if err := r.c.Do(ctx, "PUT", kafkaReplicatorPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the replicator", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	// A change to what is deployed raises the generation. Streamtime cannot yet
	// apply it to a running replicator, so it is saved and the replicator stays as it is.
	if !prior.Generation.IsNull() && out.Generation > prior.Generation.ValueInt64() {
		resp.Diagnostics.AddWarning("Replicator settings saved but not applied",
			"The new settings of replicator "+out.Identifier+" are saved, but Streamtime cannot yet apply them to a "+
				"running replicator, so it keeps running with its current settings. To run with the new settings now, "+
				"recreate it: terraform apply -replace=<resource address of this replicator>.")
	}
}

func (r *kafkaReplicatorResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m kafkaReplicatorModel
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
	if err := r.c.Do(ctx, "DELETE", kafkaReplicatorPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the replicator", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[kafkaReplicatorWire](wctx, r.c, kafkaReplicatorPath(id), "replicator", kafkaReplicatorPoll); err != nil {
		resp.Diagnostics.AddError("The replicator was not deleted", err.Error())
	}
}

func (r *kafkaReplicatorResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
