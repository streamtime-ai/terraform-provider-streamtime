package resources

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewKafkaTopic returns the streamtime_kafka_topic resource.
func NewKafkaTopic() resource.Resource { return &kafkaTopicResource{} }

type kafkaTopicResource struct{ c *client.Client }

type kafkaTopicModel struct {
	ID                types.String `tfsdk:"id"`
	Kafka             types.String `tfsdk:"kafka"`
	Name              types.String `tfsdk:"name"`
	Partitions        types.Int64  `tfsdk:"partitions"`
	ReplicationFactor types.Int64  `tfsdk:"replication_factor"`
	Config            types.Map    `tfsdk:"config"`
}

// topicWire is the Topic component of the spec.
type topicWire struct {
	Name              string `json:"name"`
	Partitions        int64  `json:"partitions"`
	ReplicationFactor int64  `json:"replication_factor"`
	Configs           []struct {
		Name      string  `json:"name"`
		Value     *string `json:"value"`
		IsDefault bool    `json:"is_default"`
	} `json:"configs"`
}

func (r *kafkaTopicResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka_topic"
}

func (r *kafkaTopicResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A topic on a Kafka cluster. Changes are applied to the cluster at once. " +
			"Partitions can be added but never removed; destroying the topic deletes its messages.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
				Description: "<kafka>/<name>. Import with the same value."},
			"kafka": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the streamtime_kafka cluster the topic is on. Changing it creates a new topic."},
			"name": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Topic name: letters, digits, dots, underscores and dashes, up to 249 characters. Changing it creates a new topic."},
			"partitions": schema.Int64Attribute{Required: true, PlanModifiers: []planmodifier.Int64{partitionsOnlyGrow{}},
				Description: "Number of partitions. It can only be raised; lowering it is an error."},
			"replication_factor": schema.Int64Attribute{Required: true,
				PlanModifiers: []planmodifier.Int64{int64planmodifier.RequiresReplace()},
				Description:   "Copies of each partition kept on different brokers. Changing it creates a new topic, deleting its messages."},
			"config": schema.MapAttribute{ElementType: types.StringType, Optional: true,
				Description: "Topic settings, for example retention.ms or cleanup.policy. Only the settings listed here are " +
					"managed; removing one returns it to the cluster's default."},
		},
	}
}

// partitionsOnlyGrow stops a plan that lowers the partition count: Kafka cannot
// remove partitions, and replacing the topic instead would delete its messages.
type partitionsOnlyGrow struct{}

func (partitionsOnlyGrow) Description(context.Context) string {
	return "Partitions can be raised but never lowered."
}
func (p partitionsOnlyGrow) MarkdownDescription(ctx context.Context) string {
	return p.Description(ctx)
}
func (partitionsOnlyGrow) PlanModifyInt64(_ context.Context, req planmodifier.Int64Request, resp *planmodifier.Int64Response) {
	if req.StateValue.IsNull() || req.PlanValue.IsUnknown() || req.PlanValue.IsNull() {
		return
	}
	if req.PlanValue.ValueInt64() < req.StateValue.ValueInt64() {
		resp.Diagnostics.AddAttributeError(req.Path, "Partitions cannot be lowered",
			fmt.Sprintf("The topic has %d partitions and Kafka cannot remove partitions. Keep partitions at %d or more. "+
				"To start again with fewer, recreate the topic with terraform apply -replace=<resource address of this topic>, "+
				"which deletes its messages.", req.StateValue.ValueInt64(), req.StateValue.ValueInt64()))
	}
}

func (r *kafkaTopicResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func topicsPath(kafka string) string { return kafkaPath(kafka) + "topics/" }
func topicPath(kafka, name string) string {
	return topicsPath(kafka) + url.PathEscape(name) + "/"
}

func splitTopicID(id string) (string, string, error) {
	kafka, name, ok := strings.Cut(id, "/")
	if !ok || kafka == "" || name == "" {
		return "", "", fmt.Errorf("expected <kafka>/<topic name>, for example orders-cluster/orders; got %q", id)
	}
	return kafka, name, nil
}

func configOf(ctx context.Context, m types.Map) (map[string]string, error) {
	if m.IsNull() || m.IsUnknown() {
		return nil, nil
	}
	var out map[string]string
	if d := m.ElementsAs(ctx, &out, false); d.HasError() {
		return nil, fmt.Errorf("config: %v", d)
	}
	return out, nil
}

// apply copies the server's topic into state. Only the configured settings are
// tracked; after an import (no configuration yet) every setting that differs
// from the cluster default is adopted.
func (m *kafkaTopicModel) apply(ctx context.Context, kafka string, t topicWire) error {
	imported := m.Partitions.IsNull() // import sets only the ID, kafka and name
	m.ID = types.StringValue(kafka + "/" + t.Name)
	m.Kafka = types.StringValue(kafka)
	m.Name = types.StringValue(t.Name)
	m.Partitions = types.Int64Value(t.Partitions)
	m.ReplicationFactor = types.Int64Value(t.ReplicationFactor)
	server := map[string]string{}
	nonDefault := map[string]attr.Value{}
	for _, c := range t.Configs {
		if c.Value == nil {
			continue // sensitive settings are not returned
		}
		server[c.Name] = *c.Value
		if !c.IsDefault {
			nonDefault[c.Name] = types.StringValue(*c.Value)
		}
	}
	kept, err := configOf(ctx, m.Config)
	if err != nil {
		return err
	}
	if kept == nil {
		if imported && len(nonDefault) > 0 {
			m.Config = types.MapValueMust(types.StringType, nonDefault)
		}
		return nil
	}
	vals := make(map[string]attr.Value, len(kept))
	for k, v := range kept {
		if got, ok := server[k]; ok {
			vals[k] = types.StringValue(got)
		} else {
			vals[k] = types.StringValue(v) // not returned (sensitive): keep the configured value
		}
	}
	m.Config = types.MapValueMust(types.StringType, vals)
	return nil
}

func (r *kafkaTopicResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m kafkaTopicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	cfg, err := configOf(ctx, m.Config)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the topic", err.Error())
		return
	}
	body := map[string]any{"name": m.Name.ValueString(), "partitions": m.Partitions.ValueInt64(),
		"replication_factor": m.ReplicationFactor.ValueInt64()}
	if len(cfg) > 0 {
		body["configs"] = cfg
	}
	kafka := m.Kafka.ValueString()
	var out topicWire
	if err := r.c.Do(ctx, "POST", topicsPath(kafka), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the topic", err.Error())
		return
	}
	if err := m.apply(ctx, kafka, out); err != nil {
		resp.Diagnostics.AddError("Could not read the topic", err.Error())
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaTopicResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m kafkaTopicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	kafka, name, err := splitTopicID(m.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Invalid topic ID", err.Error())
		return
	}
	var out topicWire
	if err := r.c.Do(ctx, "GET", topicPath(kafka, name), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the topic", err.Error())
		return
	}
	if err := m.apply(ctx, kafka, out); err != nil {
		resp.Diagnostics.AddError("Could not read the topic", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaTopicResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior kafkaTopicModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	want, err := configOf(ctx, m.Config)
	if err == nil {
		var had map[string]string
		if had, err = configOf(ctx, prior.Config); err == nil {
			body := map[string]any{}
			if m.Partitions.ValueInt64() != prior.Partitions.ValueInt64() {
				body["partitions"] = m.Partitions.ValueInt64()
			}
			set, del := map[string]string{}, []string{}
			for k, v := range want {
				if old, ok := had[k]; !ok || old != v {
					set[k] = v
				}
			}
			for k := range had {
				if _, ok := want[k]; !ok {
					del = append(del, k)
				}
			}
			if len(set) > 0 || len(del) > 0 {
				patch := map[string]any{}
				if len(set) > 0 {
					patch["set"] = set
				}
				if len(del) > 0 {
					patch["delete"] = del
				}
				body["configs"] = patch
			}
			kafka, name := prior.Kafka.ValueString(), prior.Name.ValueString()
			var out topicWire
			if len(body) == 0 {
				err = r.c.Do(ctx, "GET", topicPath(kafka, name), nil, &out)
			} else {
				err = r.c.Do(ctx, "PATCH", topicPath(kafka, name), body, &out)
			}
			if err == nil {
				err = m.apply(ctx, kafka, out)
			}
		}
	}
	if err != nil {
		resp.Diagnostics.AddError("Could not update the topic", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *kafkaTopicResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m kafkaTopicModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// The topic name in the body confirms the delete. A topic that is already gone
	// (deleted: false) or a cluster that is gone (404) is success.
	name := m.Name.ValueString()
	if err := r.c.Do(ctx, "DELETE", topicPath(m.Kafka.ValueString(), name), map[string]any{"name": name}, nil); err != nil && !isGone(err) {
		resp.Diagnostics.AddError("Could not delete the topic", err.Error())
	}
}

func (r *kafkaTopicResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	kafka, name, err := splitTopicID(req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Invalid import ID", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), kafka+"/"+name)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("kafka"), kafka)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), name)...)
}
