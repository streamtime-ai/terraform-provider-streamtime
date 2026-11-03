package resources

import (
	"context"
	"encoding/json"
	"fmt"
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

// NewVirtualKafka returns the streamtime_virtual_kafka resource.
func NewVirtualKafka() resource.Resource { return &virtualKafkaResource{} }

type virtualKafkaResource struct{ c *client.Client }

type virtualKafkaModel struct {
	ID                  types.String   `tfsdk:"id"`
	Identifier          types.String   `tfsdk:"identifier"`
	KafkaID             types.String   `tfsdk:"kafka_id"`
	KafkaUnits          types.Int64    `tfsdk:"kafka_units"`
	AdvancedConfig      types.String   `tfsdk:"advanced_config"`
	Fleet               types.String   `tfsdk:"fleet"`
	ProduceBytes        types.Int64    `tfsdk:"produce_bytes_per_second"`
	FetchBytes          types.Int64    `tfsdk:"fetch_bytes_per_second"`
	Resources           types.String   `tfsdk:"resources"`
	Status              types.String   `tfsdk:"status"`
	ReconciliationState types.String   `tfsdk:"reconciliation_state"`
	ConfigurationState  types.String   `tfsdk:"configuration_state"`
	Generation          types.Int64    `tfsdk:"generation"`
	ObservedGeneration  types.Int64    `tfsdk:"observed_generation"`
	Timeouts            timeouts.Value `tfsdk:"timeouts"`
}

// virtualKafkaWire is the server's virtual Kafka cluster (VirtualKafkaClusterResponseSchema).
type virtualKafkaWire struct {
	Identifier          string          `json:"identifier"`
	Kafka               *string         `json:"kafka"`
	Fleet               *string         `json:"fleet"`
	KafkaUnits          int64           `json:"kafka_units"`
	ProduceBytes        int64           `json:"produce_bytes_per_second"`
	FetchBytes          int64           `json:"fetch_bytes_per_second"`
	AdvancedConfig      json.RawMessage `json:"advanced_config"`
	Resources           json.RawMessage `json:"resources"`
	Status              *string         `json:"status"`
	ReconciliationState *string         `json:"reconciliation_state"`
	ConfigurationState  *string         `json:"configuration_state"`
	DesiredLifecycle    *string         `json:"desired_lifecycle"`
	Generation          int64           `json:"generation"`
	ObservedGeneration  int64           `json:"observed_generation"`
}

var virtualKafkaPoll = pollBounds{10 * time.Second, 60 * time.Second}

func (w virtualKafkaWire) progress() progress {
	return progress{deref(w.ReconciliationState), deref(w.Status), deref(w.DesiredLifecycle), w.ObservedGeneration}
}

func (r *virtualKafkaResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_virtual_kafka"
}

func (r *virtualKafkaResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A virtual Kafka cluster: its own endpoint and sign-in for a share of a Kafka cluster's capacity. " +
			"Creating one deploys it and waits until it is ready. A change to its capacity or settings is deployed " +
			"in place, and apply waits until it is running.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name: up to 24 lowercase letters, digits and dashes. Changing it creates a new virtual cluster."},
			"kafka_id": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the Kafka cluster that provides the capacity. Changing it creates a new virtual cluster."},
			"kafka_units": schema.Int64Attribute{Required: true,
				Description: "Share of the Kafka cluster's capacity, in capacity units. The shares of all virtual clusters on a " +
					"Kafka cluster cannot add up to more than the cluster's capacity."},
			"advanced_config": schema.StringAttribute{Required: true, Sensitive: true,
				Description: "Advanced settings, as a JSON object set with jsonencode(). It must hold the sign-in provider " +
					"(oauth) that clients use, including its client secret. Settings Streamtime fills in are not shown as changes."},
			"fleet": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Identifier of the fleet the virtual cluster runs on: the fleet of its Kafka cluster."},
			"produce_bytes_per_second": schema.Int64Attribute{Computed: true,
				Description: "Write limit in bytes per second, from kafka_units."},
			"fetch_bytes_per_second": schema.Int64Attribute{Computed: true,
				Description: "Read limit in bytes per second, from kafka_units."},
			"resources": schema.StringAttribute{Computed: true, Sensitive: true,
				Description: "Endpoints and ready-to-use client properties, as JSON, once the virtual cluster is healthy. " +
					"Read it with jsondecode()."},
			"status": schema.StringAttribute{Computed: true,
				Description: "Health of the virtual cluster, for example Provisioning, Healthy or Error."},
			"reconciliation_state": schema.StringAttribute{Computed: true,
				Description: "Progress of the last change: pending, queued, running, succeeded or failed."},
			"configuration_state": schema.StringAttribute{Computed: true,
				Description: "Whether the settings are complete: incomplete, valid or invalid."},
			"generation":          schema.Int64Attribute{Computed: true, Description: "Version of the settings; it goes up when they change."},
			"observed_generation": schema.Int64Attribute{Computed: true, Description: "Version of the settings that is running."},
			"timeouts":            timeouts.Attributes(ctx, timeouts.Opts{Create: true, Update: true, Delete: true}),
		},
	}
}

func (r *virtualKafkaResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

// virtualKafkaPath addresses the cluster for reads, updates and deletes.
func virtualKafkaPath(id string) string { return "virtual-kafka/" + url.PathEscape(id) + "/" }

// virtualKafkaReconcilePath is where a saved change is deployed: only the
// fleet-scoped route has the reconcile call.
func virtualKafkaReconcilePath(fleet, id string) string {
	return "fleet/" + url.PathEscape(fleet) + "/kafka/kvc/" + url.PathEscape(id) + "/reconcile/"
}

func (m *virtualKafkaModel) apply(a virtualKafkaWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.KafkaID = stringOrNull(a.Kafka)
	m.KafkaUnits = types.Int64Value(a.KafkaUnits)
	m.AdvancedConfig = keptOrServerJSON(m.AdvancedConfig, a.AdvancedConfig, true)
	m.Fleet = stringOrNull(a.Fleet)
	m.ProduceBytes, m.FetchBytes = types.Int64Value(a.ProduceBytes), types.Int64Value(a.FetchBytes)
	m.Resources = canonicalJSON(a.Resources)
	m.Status = stringOrNull(a.Status)
	m.ReconciliationState = stringOrNull(a.ReconciliationState)
	m.ConfigurationState = stringOrNull(a.ConfigurationState)
	m.Generation, m.ObservedGeneration = types.Int64Value(a.Generation), types.Int64Value(a.ObservedGeneration)
}

// deploy makes sure the saved generation is running: it waits for a deployment
// already under way, and otherwise starts one.
func (r *virtualKafkaResource) deploy(ctx context.Context, op string, out *virtualKafkaWire) error {
	p := out.progress()
	switch {
	case p.state == "succeeded" && p.observedGeneration >= out.Generation:
		return nil
	case p.state == "queued" || p.state == "running":
		return waitGeneration(ctx, r.c, virtualKafkaPath(out.Identifier), "virtual Kafka cluster", out.Generation, virtualKafkaPoll, out)
	}
	if deref(out.Fleet) == "" {
		return fmt.Errorf("Streamtime has not placed the virtual Kafka cluster on a fleet, so it cannot be deployed")
	}
	headers := map[string]string{"Idempotency-Key": idempotencyKey(op, out.Identifier, out.Generation)}
	if err := r.c.DoWithHeaders(ctx, "POST", virtualKafkaReconcilePath(*out.Fleet, out.Identifier), headers,
		map[string]any{"generation": out.Generation}, nil); err != nil {
		return fmt.Errorf("Streamtime could not start deploying the virtual Kafka cluster: %w", err)
	}
	return waitGeneration(ctx, r.c, virtualKafkaPath(out.Identifier), "virtual Kafka cluster", out.Generation, virtualKafkaPoll, out)
}

func (r *virtualKafkaResource) body(m virtualKafkaModel, create bool) (map[string]any, error) {
	// Create and update both require identifier and kafka_units.
	b := map[string]any{"identifier": m.Identifier.ValueString(), "kafka_units": m.KafkaUnits.ValueInt64()}
	if create {
		b["kafka_id"] = m.KafkaID.ValueString()
	}
	adv, err := jsonObject(m.AdvancedConfig, "advanced_config")
	if err != nil {
		return nil, err
	}
	if adv != nil {
		b["advanced_config"] = adv
	}
	return b, nil
}

func (r *virtualKafkaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m virtualKafkaModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 60*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m, true)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the virtual Kafka cluster", err.Error())
		return
	}
	var out virtualKafkaWire
	if err := r.c.Do(ctx, "POST", "virtual-kafka/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the virtual Kafka cluster", err.Error())
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
	werr := r.deploy(wctx, "create", &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The virtual Kafka cluster was not created", werr.Error())
	}
}

func (r *virtualKafkaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m virtualKafkaModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out virtualKafkaWire
	if err := r.c.Do(ctx, "GET", virtualKafkaPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the virtual Kafka cluster", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *virtualKafkaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior virtualKafkaModel
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
	body, err := r.body(m, false)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the virtual Kafka cluster", err.Error())
		return
	}
	var out virtualKafkaWire
	if err := r.c.Do(ctx, "PUT", virtualKafkaPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the virtual Kafka cluster", err.Error())
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
	// A change to what is deployed raises the generation; that generation, or an
	// earlier one that failed to deploy, is deployed.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	werr := r.deploy(wctx, "update", &out)
	save()
	if werr != nil {
		resp.Diagnostics.AddError("The change to the virtual Kafka cluster was not deployed", werr.Error())
	}
}

// ModifyPlan plans an update when the last change is saved but not running, so
// the next apply deploys it.
func (r *virtualKafkaResource) ModifyPlan(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse) {
	planRedeploy(ctx, req, resp, "virtual Kafka cluster", []string{"resources", "status", "reconciliation_state", "configuration_state"})
}

func (r *virtualKafkaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m virtualKafkaModel
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
	if err := r.c.Do(ctx, "DELETE", virtualKafkaPath(id), nil, nil); err != nil {
		if isGone(err) {
			return
		}
		resp.Diagnostics.AddError("Could not delete the virtual Kafka cluster", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitGone[virtualKafkaWire](wctx, r.c, virtualKafkaPath(id), "virtual Kafka cluster", virtualKafkaPoll); err != nil {
		resp.Diagnostics.AddError("The virtual Kafka cluster was not deleted", err.Error())
	}
}

func (r *virtualKafkaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
