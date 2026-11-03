package resources

import (
	"context"
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
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/streamtimeapi"
)

// NewNetwork returns the streamtime_network resource.
func NewNetwork() resource.Resource { return &networkResource{} }

type networkResource struct{ c *client.Client }

type networkModel struct {
	ID           types.String   `tfsdk:"id"`
	Identifier   types.String   `tfsdk:"identifier"`
	Name         types.String   `tfsdk:"name"`
	CloudAccount types.String   `tfsdk:"cloud_account"`
	Region       types.String   `tfsdk:"region"`
	CIDR         types.String   `tfsdk:"cidr"`
	NetworkID    types.String   `tfsdk:"network_id"`
	Provisioned  types.Bool     `tfsdk:"provisioned_by_streamtime"`
	Timeouts     timeouts.Value `tfsdk:"timeouts"`
}

// Polling bounds while Streamtime creates a cloud network.
var networkPoll = struct{ initial, max time.Duration }{5 * time.Second, 30 * time.Second}

func (r *networkResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_network"
}

func (r *networkResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A cloud network (VPC or VCN) for fleets. Leave network_id out and Streamtime creates the network " +
			"in the cloud account; set it to use a network you already have.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, Description: "Same as identifier. Use the identifier to import.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name of the network in Streamtime. Changing it creates a new network."},
			"name": schema.StringAttribute{Required: true, Description: "Display name, also used for a network Streamtime creates."},
			"cloud_account": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Identifier of the cloud account the network belongs to. Changing it creates a new network."},
			"region": schema.StringAttribute{Optional: true, PlanModifiers: replace,
				Description: "Cloud region. Required when Streamtime creates the network. Changing it creates a new network."},
			"cidr": schema.StringAttribute{Optional: true, PlanModifiers: replace,
				Description: "Address range, for example 10.0.0.0/16. Leave it out to use the cloud's default. Changing it creates a new network."},
			"network_id": schema.StringAttribute{Optional: true, Computed: true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown(), stringplanmodifier.RequiresReplaceIfConfigured()},
				Description: "The cloud's ID of the network. Set it to use an existing network; leave it out and Streamtime " +
					"creates one and fills this in. Changing a configured value creates a new network."},
			"provisioned_by_streamtime": schema.BoolAttribute{Computed: true,
				PlanModifiers: []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()},
				Description:   "Whether Streamtime created the cloud network. Unknown after an import."},
			"timeouts": timeouts.Attributes(ctx, timeouts.Opts{Create: true}),
		},
	}
}

func (r *networkResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func networkPath(id string) string { return "networks/" + url.PathEscape(id) + "/" }

func (r *networkResource) body(m networkModel) map[string]any {
	b := map[string]any{
		"identifier": m.Identifier.ValueString(),
		"name":       m.Name.ValueString(),
		"provider":   m.CloudAccount.ValueString(),
	}
	for k, v := range map[string]types.String{"region": m.Region, "cidr": m.CIDR, "network_id": m.NetworkID} {
		if p := optString(v); p != nil && *p != "" {
			b[k] = *p
		}
	}
	return b
}

func (m *networkModel) apply(a streamtimeapi.Network) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.Name = types.StringValue(a.Name)
	m.CloudAccount = types.StringValue(a.Provider)
	m.Region = fromServer(a.Region)
	m.CIDR = fromServer(a.Cidr)
	m.NetworkID = fromServer(a.NetworkId)
	if m.Provisioned.IsUnknown() {
		m.Provisioned = types.BoolNull()
	}
}

func (r *networkResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m networkModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	timeout, d := m.Timeouts.Create(ctx, 30*time.Minute)
	resp.Diagnostics.Append(d...)
	if resp.Diagnostics.HasError() {
		return
	}
	provisioning := m.NetworkID.IsNull() || m.NetworkID.IsUnknown() || m.NetworkID.ValueString() == ""
	var out streamtimeapi.Network
	if err := r.c.Do(ctx, "POST", "networks/", r.body(m), &out); err != nil {
		resp.Diagnostics.AddError("Could not create the network", err.Error())
		return
	}
	m.Provisioned = types.BoolValue(provisioning)
	m.apply(out)
	if !provisioning {
		resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
		return
	}
	// Streamtime creates the cloud network in the background and records its ID
	// when it is done. It records no failure, so a timeout is the only signal.
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := client.WaitForStatus(wctx, networkPoll.initial, networkPoll.max, func(ctx context.Context) (bool, error) {
		if err := r.c.Do(ctx, "GET", networkPath(out.Identifier), nil, &out); err != nil {
			return false, err
		}
		return out.NetworkId != nil && *out.NetworkId != "", nil
	})
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	if err != nil {
		resp.Diagnostics.AddError("The cloud network was not created",
			err.Error()+". Check that the cloud account can create networks in "+m.Region.ValueString()+
				" and that the region is right. Terraform marks the network to be replaced on the next apply.")
	}
}

func (r *networkResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m networkModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out streamtimeapi.Network
	if err := r.c.Do(ctx, "GET", networkPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the network", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *networkResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior networkModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out streamtimeapi.Network
	if err := r.c.Do(ctx, "PUT", networkPath(prior.ID.ValueString()), r.body(m), &out); err != nil {
		resp.Diagnostics.AddError("Could not update the network", err.Error())
		return
	}
	m.Provisioned = prior.Provisioned
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *networkResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m networkModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := deleteIgnoringGone(ctx, r.c, networkPath(m.ID.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the network", err.Error())
		return
	}
	// Streamtime removes only its record of the network, not the cloud network.
	if m.Provisioned.IsNull() || m.Provisioned.ValueBool() {
		cloud := "the cloud network it may have created"
		if id := m.NetworkID.ValueString(); id != "" {
			cloud = "the cloud network " + id
		}
		resp.Diagnostics.AddWarning("The cloud network was not deleted",
			"Streamtime removed the network "+m.Identifier.ValueString()+" but not "+cloud+
				". Delete it in your cloud console if you no longer need it.")
	}
}

func (r *networkResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
