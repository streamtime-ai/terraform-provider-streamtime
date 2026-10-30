package resources

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/streamtimeapi"
)

// NewSecret returns the streamtime_secret resource.
func NewSecret() resource.Resource { return &secretResource{} }

type secretResource struct{ c *client.Client }

type secretModel struct {
	ID       types.String `tfsdk:"id"`
	Path     types.String `tfsdk:"path"`
	Provider types.String `tfsdk:"secret_provider"`
	Content  types.String `tfsdk:"content"`
	URI      types.String `tfsdk:"uri"`
}

func (r *secretResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_secret"
}

func (r *secretResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A secret stored by Streamtime. Its content is never shown in plans or logs.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, Description: "Secret ID. Use it to import an existing secret.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
			"path": schema.StringAttribute{Required: true, Description: "Where the secret lives, for example apps/db-password. Changing it creates a new secret.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()}},
			"secret_provider": schema.StringAttribute{Optional: true, Computed: true,
				Description:   "Where the secret is stored: builtin or vault. Leave it out to use the provider your installation has set as active. Changing it creates a new secret.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown(), stringplanmodifier.RequiresReplace()}},
			"content": schema.StringAttribute{Required: true, Sensitive: true, Description: "The secret value."},
			"uri": schema.StringAttribute{Computed: true, Description: "Reference other resources use to read this secret.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
		},
	}
}

func (r *secretResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func itemPath(id string) string { return "secrets/" + url.PathEscape(id) + "/" }

func (m *secretModel) apply(a streamtimeapi.Secret) {
	if a.Id != nil {
		m.ID = types.StringValue(a.Id.String())
	}
	m.Path = types.StringValue(a.Path)
	if a.Provider != nil {
		m.Provider = types.StringValue(string(*a.Provider))
	} else if m.Provider.IsUnknown() {
		m.Provider = types.StringNull()
	}
	m.URI = fromServer(a.Uri)
	// The server does not always return the content; keep what the plan holds.
	if a.Content != nil && *a.Content != "" {
		m.Content = types.StringValue(*a.Content)
	}
}

func (r *secretResource) body(m secretModel) (json.RawMessage, error) {
	c := m.Content.ValueString()
	b := streamtimeapi.Secret{Path: m.Path.ValueString(), Content: &c}
	// An unset provider is left out so the server applies its active provider.
	if v := m.Provider.ValueString(); v != "" {
		p := streamtimeapi.ProviderBe6Enum(v)
		b.Provider = &p
	}
	return withoutNulls(b)
}

func (r *secretResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m secretModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the secret", err.Error())
		return
	}
	var out streamtimeapi.Secret
	if err := r.c.Do(ctx, "POST", "secrets/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the secret", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *secretResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m secretModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out streamtimeapi.Secret
	if err := r.c.Do(ctx, "GET", itemPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the secret", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *secretResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior secretModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the secret", err.Error())
		return
	}
	var out streamtimeapi.Secret
	if err := r.c.Do(ctx, "PUT", itemPath(prior.ID.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the secret", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *secretResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m secretModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := deleteIgnoringGone(ctx, r.c, itemPath(m.ID.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the secret", err.Error())
	}
}

// ImportState imports by secret ID. The content is not returned by the server,
// so the first plan after an import shows it as a change to apply.
func (r *secretResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
