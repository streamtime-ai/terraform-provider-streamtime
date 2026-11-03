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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewKafkaConnect returns the streamtime_kafka_connect resource.
func NewKafkaConnect() resource.Resource { return &kafkaConnectResource{} }

type kafkaConnectResource struct{ c *client.Client }

type kafkaConnectModel struct {
	ID                  types.String   `tfsdk:"id"`
	Identifier          types.String   `tfsdk:"identifier"`
	KafkaID             types.String   `tfsdk:"kafka_id"`
	ConnectProvider     types.String   `tfsdk:"connect_provider"`
	AdvancedConfig      types.String   `tfsdk:"advanced_config"`
	Fleet               types.String   `tfsdk:"fleet"`
	Status              types.String   `tfsdk:"status"`
	ReconciliationState types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState  types.String   `tfsdk:"configuration_state"`
	Generation          types.Int64    `tfsdk:"generation"`
	ObservedGeneration  types.Int64    `tfsdk:"observed_generation"`
	Timeouts            timeouts.Value `tfsdk:"timeouts"`
}

// kafkaConnectWire is the server's Kafka Connect instance (KafkaConnectResponseSchema).
type kafkaConnectWire struct {
	Identifier          string          `json:"identifier"`
	Kafka               *string         `json:"kafka"`
	Provider            string          `json:"provider"`
	Fleet               *string         `json:"fleet"`
	AdvancedConfig      json.RawMessage `json:"advanced_config"`
	Status              *string         `json:"status"`
	ReconciliationState *string         `json:"reconciliation_state"`
	ConfigurationState  *string         `json:"configuration_state"`
	DesiredLifecycle    *string         `json:"desired_lifecycle"`
	Generation          int64           `json:"generation"`
	ObservedGeneration  int64           `json:"observed_generation"`
}

var kafkaConnectPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w kafkaConnectWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *kafkaConnectResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka_connect"
}

func (r *kafkaConnectResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "Kafka Connect for a Kafka cluster, to run connectors. Creating it deploys Kafka Connect and waits " +
			"until it is ready. Changes to its settings are saved, but running Kafka Connect keeps its current " +
			"settings until Streamtime can apply changes to running instances.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name: up to 24 lowercase letters, digits and dashes. Changing it creates a new Kafka Connect."},
			"kafka_id": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the Kafka cluster Kafka Connect works with. Changing it creates a new Kafka Connect."},
			"connect_provider": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Kafka Connect flavour: skc (open-source Apache Kafka Connect) or cfk (Confluent Kafka Connect). " +
					"Changing it creates a new Kafka Connect."},
			"advanced_config": schema.StringAttribute{Optional: true, Computed: true, Sensitive: true, PlanModifiers: keepStr,
				Description: "Advanced settings, as a JSON object set with jsonencode(), for example the connector plugin " +
					"archives to install. Settings Streamtime fills in are not shown as changes."},
			"fleet": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Identifier of the fleet Kafka Connect runs on: the fleet of its Kafka cluster."},
			"status": schema.StringAttribute{Computed: true,
				Description: "Health of Kafka Connect, for example Provisioning, Healthy or Error."},
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

func (r *kafkaConnectResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func kafkaConnectPath(id string) string { return "kafka-connect/" + url.PathEscape(id) + "/" }

func (m *kafkaConnectModel) apply(a kafkaConnectWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.KafkaID = stringOrNull(a.Kafka)
	m.ConnectProvider = types.StringValue(a.Provider)
	m.AdvancedConfig = keptOrServerJSON(m.AdvancedConfig, a.AdvancedConfig, true)
	m.Fleet = stringOrNull(a.Fleet)
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

func (r *kafkaConnectResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m kafkaConnectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	adv, err := jsonObject(m.AdvancedConfig, "advanced_config")
	if err != nil {
		resp.Diagnostics.AddError("Could not create Kafka Connect", err.Error())
		return
	}
	if adv == nil {
		adv = map[string]any{} // required by the API; every setting has a default
	}
	body := map[string]any{"identifier": m.Identifier.ValueString(), "kafka_id": m.KafkaID.ValueString(),
		"provider": m.ConnectProvider.ValueString(), "advanced_config": adv}
	var out kafkaConnectWire
	if err := r.c.Do(ctx, "POST", "kafka-connect/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create Kafka Connect", err.Error())
		return
	}
	// It exists from here on: record it, so a failure below leaves it in state
	// (marked for replacement) rather than unmanaged.
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
	werr := reconcileAndWait(wctx, r.c, kafkaConnectPath(out.Identifier), "Kafka Connect", "create", out.Identifier, out.Generation, kafkaConnectPoll, &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("Kafka Connect was not created", werr.Error())
	}
}

func (r *kafkaConnectResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m kafkaConnectModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out kafkaConnectWire
	if err := r.c.Do(ctx, "GET", kafkaConnectPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read Kafka Connect", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaConnectResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior kafkaConnectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	adv, err := jsonObject(m.AdvancedConfig, "advanced_config")
	if err != nil {
		resp.Diagnostics.AddError("Could not update Kafka Connect", err.Error())
		return
	}
	// The API validates the full representation: identifier is required, and a
	// request naming none of kafka_id, provider or advanced_config is refused.
	body := map[string]any{"identifier": prior.ID.ValueString(), "kafka_id": prior.KafkaID.ValueString(),
		"provider": prior.ConnectProvider.ValueString()}
	if adv != nil {
		body["advanced_config"] = adv
	}
	var out kafkaConnectWire
	if err := r.c.Do(ctx, "PUT", kafkaConnectPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update Kafka Connect", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	// A change to what is deployed raises the generation. Streamtime cannot yet
	// apply it to running Kafka Connect, so it is saved and the instance stays as it is.
	if !prior.Generation.IsNull() && out.Generation > prior.Generation.ValueInt64() {
		resp.Diagnostics.AddWarning("Kafka Connect settings saved but not applied",
			"The new settings of Kafka Connect "+out.Identifier+" are saved, but Streamtime cannot yet apply them to "+
				"running Kafka Connect, so it keeps running with its current settings. To run with the new settings now, "+
				"recreate it: terraform apply -replace=<resource address of this Kafka Connect>.")
	}
}

func (r *kafkaConnectResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m kafkaConnectModel
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
	if err := r.c.Do(ctx, "DELETE", kafkaConnectPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete Kafka Connect", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[kafkaConnectWire](wctx, r.c, kafkaConnectPath(id), "Kafka Connect", kafkaConnectPoll); err != nil {
		resp.Diagnostics.AddError("Kafka Connect was not deleted", err.Error())
	}
}

func (r *kafkaConnectResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
