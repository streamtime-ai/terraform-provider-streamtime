package resources

import (
	"context"
	"net/url"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewCloudAccount returns the streamtime_cloud_account resource.
func NewCloudAccount() resource.Resource { return &cloudAccountResource{} }

type cloudAccountResource struct{ c *client.Client }

type cloudAccountModel struct {
	ID            types.String `tfsdk:"id"`
	Identifier    types.String `tfsdk:"identifier"`
	CloudProvider types.String `tfsdk:"cloud_provider"`
	Configuration types.String `tfsdk:"configuration"`
}

// cloudAccountWire is the server's BootstrapProvider. The provider is read as a
// plain string: the spec's enum does not list every cloud the API accepts.
type cloudAccountWire struct {
	Identifier string `json:"identifier"`
	Provider   string `json:"provider"`
}

func (r *cloudAccountResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_cloud_account"
}

func (r *cloudAccountResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A cloud account Streamtime uses to create fleets and networks in your cloud.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, Description: "Same as identifier. Use the identifier to import.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
			"identifier": schema.StringAttribute{Required: true,
				Description:   "Unique name of the cloud account: up to 24 lowercase letters, digits and dashes. Changing it creates a new cloud account.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()}},
			// Not RequiresReplace: deleting a cloud account also deletes the networks and node
			// pools recorded under it, which a plan would not show. The API changes it in place.
			"cloud_provider": schema.StringAttribute{Required: true,
				Description: "The cloud: aws, azure, gcp, oci, digitalocean or civo. Change configuration to that cloud's credentials in the same apply."},
			"configuration": schema.StringAttribute{Required: true, Sensitive: true,
				Description: "The account's credentials as a JSON object, set with jsonencode(). " +
					"aws: aws_access_key, aws_access_secret (optional aws_session_token). " +
					"azure: subscription_id, tenant_id, client_id, client_secret. " +
					"gcp: service_account_key (the key file's JSON as a string). " +
					"oci: tenancy_ocid, user_ocid, fingerprint, private_key (optional identity_domain_ocid). " +
					"digitalocean: digitalocean_token. civo: civo_api_token. " +
					"Terraform keeps the value you set and does not read credentials back, so a change made outside Terraform is not detected."},
		},
	}
}

func (r *cloudAccountResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func cloudAccountPath(id string) string { return "bootstrap-providers/" + url.PathEscape(id) + "/" }

func (r *cloudAccountResource) body(m cloudAccountModel) (map[string]any, error) {
	cfg, err := jsonObject(m.Configuration, "configuration")
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	return map[string]any{
		"identifier":    m.Identifier.ValueString(),
		"provider":      m.CloudProvider.ValueString(),
		"configuration": cfg,
	}, nil
}

// apply copies the server's view into state. Credentials are never adopted from
// the server: they stay as configured (or null after an import).
func (m *cloudAccountModel) apply(a cloudAccountWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.CloudProvider = types.StringValue(a.Provider)
}

func (r *cloudAccountResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m cloudAccountModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the cloud account", err.Error())
		return
	}
	var out cloudAccountWire
	if err := r.c.Do(ctx, "POST", "bootstrap-providers/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the cloud account", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *cloudAccountResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m cloudAccountModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out cloudAccountWire
	if err := r.c.Do(ctx, "GET", cloudAccountPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the cloud account", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *cloudAccountResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m cloudAccountModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the cloud account", err.Error())
		return
	}
	var out cloudAccountWire
	if err := r.c.Do(ctx, "PUT", cloudAccountPath(m.Identifier.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the cloud account", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *cloudAccountResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m cloudAccountModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := deleteIgnoringGone(ctx, r.c, cloudAccountPath(m.ID.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the cloud account", err.Error())
	}
}

// ImportState imports by identifier. Credentials are not read back, so the first
// plan after an import shows configuration as a change to apply.
func (r *cloudAccountResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
