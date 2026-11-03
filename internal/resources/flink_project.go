package resources

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// NewFlinkProject returns the streamtime_flink_project resource.
func NewFlinkProject() resource.Resource { return &flinkProjectResource{} }

type flinkProjectResource struct{ c *client.Client }

type flinkProjectModel struct {
	ID              types.String       `tfsdk:"id"`
	Identifier      types.String       `tfsdk:"identifier"`
	Name            types.String       `tfsdk:"name"`
	Description     types.String       `tfsdk:"description"`
	Runtime         types.String       `tfsdk:"runtime"`
	BuildTool       types.String       `tfsdk:"build_tool"`
	Cluster         types.String       `tfsdk:"cluster"`
	FlinkUnitBudget types.Int64        `tfsdk:"flink_unit_budget"`
	Metadata        types.String       `tfsdk:"metadata"`
	ArtifactKind    types.String       `tfsdk:"artifact_kind"`
	LatestVersion   types.String       `tfsdk:"latest_version"`
	Store           *flinkProjectStore `tfsdk:"store"`
}

type flinkProjectStore struct {
	StoreType      types.String `tfsdk:"store_type"`
	URL            types.String `tfsdk:"url"`
	CACertificate  types.String `tfsdk:"ca_certificate"`
	Authentication types.String `tfsdk:"authentication"`
	Username       types.String `tfsdk:"username"`
	Password       types.String `tfsdk:"password"`
	Token          types.String `tfsdk:"token"`
	HasCredential  types.Bool   `tfsdk:"has_credential"`
}

// flinkProjectWire is the server's Flink project (FlinkProject / FlinkProjectCreated in the spec).
type flinkProjectWire struct {
	Identifier      string          `json:"identifier"`
	Name            string          `json:"name"`
	Description     *string         `json:"description"`
	Runtime         string          `json:"runtime"`
	BuildTool       *string         `json:"build_tool"`
	Cluster         *string         `json:"cluster"`
	FlinkUnitBudget *int64          `json:"flink_unit_budget"`
	Metadata        json.RawMessage `json:"metadata"`
	ArtifactKind    string          `json:"artifact_kind"`
	LatestVersion   *string         `json:"latest_version"`
}

// flinkProjectStoreWire is the server's store connection (FlinkProjectStore). The
// credential is write-only: only has_credential comes back.
type flinkProjectStoreWire struct {
	StoreType      string `json:"store_type"`
	URL            string `json:"url"`
	CACertificate  string `json:"ca_certificate"`
	Authentication string `json:"authentication"`
	HasCredential  bool   `json:"has_credential"`
}

func (r *flinkProjectResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_flink_project"
}

func (r *flinkProjectResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	keepStr := []planmodifier.String{stringplanmodifier.UseStateForUnknown()}
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	resp.Schema = schema.Schema{
		Description: "A Flink project: the home of a Flink application's uploaded versions, which Flink instances and jobs run. " +
			"Creating one returns a key your CI system uses to upload versions to it.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Same as identifier. Use the identifier to import."},
			"identifier": schema.StringAttribute{Required: true, PlanModifiers: replace,
				Description: "Unique name of the project: up to 24 lowercase letters, digits and dashes. Changing it creates a new project."},
			"name":        schema.StringAttribute{Required: true, Description: "Display name of the project."},
			"description": schema.StringAttribute{Optional: true, Description: "Free-text description."},
			"runtime": schema.StringAttribute{Optional: true, Computed: true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown(), stringplanmodifier.RequiresReplace()},
				Description:   "Language of the application: java, scala or python. It decides which artifacts the project accepts. Changing it creates a new project."},
			"build_tool": schema.StringAttribute{Computed: true, PlanModifiers: keepStr,
				Description: "Tool your CI pipeline builds the artifact with, chosen from the runtime: Maven for java, sbt for scala, pip for python."},
			"cluster": schema.StringAttribute{Required: true,
				Description: "Identifier of the Flink job workspace (a streamtime_flink with mode = session) the project's Flink units are booked on."},
			"flink_unit_budget": schema.Int64Attribute{Required: true,
				Description: "Flink units the project may use on its job workspace, booked from the workspace's capacity."},
			"metadata": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
				Description: "Project defaults and labels, as a JSON object set with jsonencode(): entry_class (the default main " +
					"class of a JAR), default_args, labels and documentation_url. Values Streamtime fills in are not shown as changes."},
			"artifact_kind":  schema.StringAttribute{Computed: true, PlanModifiers: keepStr, Description: "Kind of artifact the project accepts, from its runtime."},
			"latest_version": schema.StringAttribute{Computed: true, Description: "Most recently uploaded version, if any."},
			"store": schema.SingleNestedAttribute{Optional: true,
				Description: "Your own artifact store for the project's releases. Removing it disconnects the store; " +
					"registered releases stay listed but cannot be pulled until a store is connected again.",
				Attributes: map[string]schema.Attribute{
					"store_type": schema.StringAttribute{Required: true, Description: "Kind of store: oras (an OCI registry) or jfrog."},
					"url":        schema.StringAttribute{Required: true, Description: "The store's https address, for example https://ghcr.io."},
					"ca_certificate": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
						Description: "CA certificate (PEM) to trust when the store does not use a public certificate."},
					"authentication": schema.StringAttribute{Optional: true, Computed: true, PlanModifiers: keepStr,
						Description: "How Flink instances sign in to the store: none (the default), basic or token."},
					"username": schema.StringAttribute{Optional: true, Sensitive: true,
						Description: "Username, for basic sign-in. Streamtime never returns it, so a change made outside Terraform is not shown."},
					"password": schema.StringAttribute{Optional: true, Sensitive: true,
						Description: "Password, for basic sign-in. Use a read-only, expiring credential limited to this project's artifacts. " +
							"Streamtime never returns it."},
					"token": schema.StringAttribute{Optional: true, Sensitive: true,
						Description: "Access token, for token sign-in. Use a read-only, expiring token limited to this project's artifacts. " +
							"Streamtime never returns it."},
					"has_credential": schema.BoolAttribute{Computed: true,
						PlanModifiers: []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()},
						Description:   "Whether Streamtime holds a credential for the store."},
				}},
		},
	}
}

func (r *flinkProjectResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func flinkProjectPath(id string) string { return "flink-projects/" + url.PathEscape(id) + "/" }

func (r *flinkProjectResource) body(m flinkProjectModel) (map[string]any, error) {
	b := map[string]any{
		"identifier": m.Identifier.ValueString(),
		"name":       m.Name.ValueString(),
		// Sent as null when unset, which clears them.
		"description":       optString(m.Description),
		"cluster":           m.Cluster.ValueString(),
		"flink_unit_budget": m.FlinkUnitBudget.ValueInt64(),
	}
	if p := optString(m.Runtime); p != nil {
		b["runtime"] = *p
	}
	meta, err := jsonObject(m.Metadata, "metadata")
	if err != nil {
		return nil, err
	}
	if meta != nil {
		b["metadata"] = meta
	}
	return b, nil
}

func (m *flinkProjectModel) apply(a flinkProjectWire) {
	m.ID = types.StringValue(a.Identifier)
	m.Identifier = types.StringValue(a.Identifier)
	m.Name = types.StringValue(a.Name)
	m.Description = stringOrNull(a.Description)
	m.Runtime = types.StringValue(a.Runtime)
	m.BuildTool = stringOrNull(a.BuildTool)
	m.Cluster = stringOrNull(a.Cluster)
	m.FlinkUnitBudget = types.Int64PointerValue(a.FlinkUnitBudget)
	m.Metadata = keptOrServerJSON(m.Metadata, a.Metadata, false)
	m.ArtifactKind = types.StringValue(a.ArtifactKind)
	m.LatestVersion = stringOrNull(a.LatestVersion)
}

func storePath(id string) string { return flinkProjectPath(id) + "store/" }

func storeBody(s *flinkProjectStore) map[string]any {
	b := map[string]any{"store_type": s.StoreType.ValueString(), "url": s.URL.ValueString()}
	for k, v := range map[string]types.String{"ca_certificate": s.CACertificate, "authentication": s.Authentication,
		"username": s.Username, "password": s.Password, "token": s.Token} {
		if p := optString(v); p != nil {
			b[k] = *p
		}
	}
	return b
}

// applyStore copies the server's store into state. The credential is never
// returned, so the configured one is kept.
func applyStore(kept *flinkProjectStore, a *flinkProjectStoreWire) *flinkProjectStore {
	if a == nil {
		return nil
	}
	s := &flinkProjectStore{Username: types.StringNull(), Password: types.StringNull(), Token: types.StringNull()}
	if kept != nil {
		s.Username, s.Password, s.Token = kept.Username, kept.Password, kept.Token
	}
	s.StoreType = types.StringValue(a.StoreType)
	s.URL = types.StringValue(a.URL)
	s.CACertificate = types.StringValue(a.CACertificate)
	s.Authentication = types.StringValue(a.Authentication)
	s.HasCredential = types.BoolValue(a.HasCredential)
	return s
}

// readStore reads the project's store connection; nil when none is connected.
func (r *flinkProjectResource) readStore(ctx context.Context, id string) (*flinkProjectStoreWire, error) {
	var out flinkProjectStoreWire
	if err := r.c.Do(ctx, "GET", storePath(id), nil, &out); err != nil {
		if isGone(err) {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}

func (r *flinkProjectResource) putStore(ctx context.Context, id string, s *flinkProjectStore) (*flinkProjectStoreWire, error) {
	var out flinkProjectStoreWire
	if err := r.c.Do(ctx, "PUT", storePath(id), storeBody(s), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *flinkProjectResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m flinkProjectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the Flink project", err.Error())
		return
	}
	var out flinkProjectWire
	if err := r.c.Do(ctx, "POST", "flink-projects/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the Flink project", err.Error())
		return
	}
	planned := m.Store
	m.apply(out)
	m.Store = nil
	// The project and its one-time key are recorded before the store is connected,
	// so a failure below does not lose the key.
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	if resp.Diagnostics.HasError() || planned == nil {
		return
	}
	st, err := r.putStore(ctx, out.Identifier, planned)
	if err != nil {
		resp.Diagnostics.AddError("The Flink project was created, but its store was not connected", err.Error())
		return
	}
	m.Store = applyStore(planned, st)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *flinkProjectResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m flinkProjectModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	id := m.ID.ValueString()
	var out flinkProjectWire
	if err := r.c.Do(ctx, "GET", flinkProjectPath(id), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the Flink project", err.Error())
		return
	}
	st, err := r.readStore(ctx, id)
	if err != nil {
		resp.Diagnostics.AddError("Could not read the Flink project's store", err.Error())
		return
	}
	m.apply(out)
	m.Store = applyStore(m.Store, st)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *flinkProjectResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m, prior flinkProjectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	id := prior.ID.ValueString()
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the Flink project", err.Error())
		return
	}
	var out flinkProjectWire
	if err := r.c.Do(ctx, "PUT", flinkProjectPath(id), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the Flink project", err.Error())
		return
	}
	planned := m.Store
	m.apply(out)
	m.Store = prior.Store
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	switch {
	case planned == nil && prior.Store != nil:
		if err := deleteIgnoringGone(ctx, r.c, storePath(id)); err != nil {
			resp.Diagnostics.AddError("Could not disconnect the Flink project's store", err.Error())
			return
		}
		m.Store = nil
	case planned != nil && !storeEqual(planned, prior.Store):
		st, err := r.putStore(ctx, id, planned)
		if err != nil {
			resp.Diagnostics.AddError("Could not update the Flink project's store", err.Error())
			return
		}
		m.Store = applyStore(planned, st)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// storeEqual reports whether the planned store is the one in state, ignoring what
// only the server computes.
func storeEqual(planned, prior *flinkProjectStore) bool {
	if prior == nil {
		return false
	}
	a, b := *planned, *prior
	a.HasCredential, b.HasCredential = types.BoolNull(), types.BoolNull()
	return reflect.DeepEqual(a, b)
}

func (r *flinkProjectResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m flinkProjectModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// Deleting the project also removes its versions, its CI key and its store
	// connection. Streamtime refuses while something runs a version, naming what.
	if err := deleteIgnoringGone(ctx, r.c, flinkProjectPath(m.ID.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the Flink project", err.Error())
	}
}

func (r *flinkProjectResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
