package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// The organization and catalog resources are plain synchronous objects: create,
// read, update and delete over one collection, no deployment to wait for. They
// share this engine instead of a hand-written file each, so they all handle
// server defaults, credentials and "already gone" the same way. A resource is a
// list of fields plus a few hooks.

type fieldKind int

const (
	kString fieldKind = iota
	kBool
	kInt
	kStrings
	kJSON
)

// field is one attribute of an object resource and the API key it maps to.
type field struct {
	name string // Terraform attribute
	api  string // key in the API body; defaults to name
	desc string
	kind fieldKind

	required bool
	optional bool // neither required nor optional: set by the server
	computed bool // with optional: the server fills it in when it is not configured

	sensitive bool
	replace   bool // changing it creates a new object
	// hidden: the server never returns the value (a credential), so the configured
	// value is kept.
	hidden bool
	// once: the server returns the value only in the response that creates the
	// object (a generated key or secret), so it is kept from then on.
	once          bool
	createOnly    bool
	unordered     bool
	clearOnRemove bool
	// masked: a JSON setting in which the server replaces secret values with ********.
	masked bool
	// same reports whether two strings mean the same to the server, which normalises
	// some values (a trailing slash).
	same func(configured, server string) bool
}

func (f field) apiName() string {
	if f.api != "" {
		return f.api
	}
	return f.name
}

func (f field) writable() bool { return f.required || f.optional }

// objectSpec describes one resource.
type objectSpec struct {
	typeName   string // streamtime_member
	noun       string // "member", for messages
	desc       string
	collection string // org-scoped path of the collection, with a trailing slash
	createPath string
	key        string // attribute that addresses the object in its URL
	fields     []field

	// update is the HTTP method that changes an object: PUT or PATCH. Empty means
	// the API cannot change it, so every writable attribute replaces the object.
	update string

	// singleton is the ID of an object that always exists (the organization's mail
	// settings). Create and delete only start and stop managing it.
	singleton string

	// prepare adjusts a request body before it is sent.
	prepare func(ctx context.Context, c *client.Client, body map[string]any) error
	// reshape adjusts a response before it is read into the state.
	reshape func(obj map[string]json.RawMessage)
	// verify runs before a read, update or delete, and stops when the object the
	// URL reached is not the one in the state.
	verify func(ctx context.Context, state tfsdk.State, obj map[string]json.RawMessage) error
}

type objectResource struct {
	spec objectSpec
	c    *client.Client
}

func newObjectResource(spec objectSpec) func() resource.Resource {
	return func() resource.Resource { return &objectResource{spec: spec} }
}

func (r *objectResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + strings.TrimPrefix(r.spec.typeName, "streamtime")
}

func (r *objectResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func (r *objectResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	attrs := map[string]schema.Attribute{}
	if r.spec.key != "id" {
		attrs["id"] = schema.StringAttribute{
			Computed:      true,
			Description:   "ID of the " + r.spec.noun + ". Use it to import an existing one.",
			PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
		}
	}
	for _, f := range r.spec.fields {
		attrs[f.name] = r.attribute(f)
	}
	resp.Schema = schema.Schema{Description: r.spec.desc, Attributes: attrs}
}

func (r *objectResource) attribute(f field) schema.Attribute {
	replace := f.replace || (r.spec.update == "" && f.writable())
	computed := f.computed || !f.writable()
	keep := computed // the server decides it, so a plan without a change keeps the state's value
	desc := f.desc
	switch f.kind {
	case kBool:
		var pm []planmodifier.Bool
		if keep {
			pm = append(pm, boolplanmodifier.UseStateForUnknown())
		}
		if replace {
			pm = append(pm, boolplanmodifier.RequiresReplace())
		}
		return schema.BoolAttribute{Required: f.required, Optional: f.optional, Computed: computed, Description: desc, PlanModifiers: pm}
	case kInt:
		var pm []planmodifier.Int64
		if keep {
			pm = append(pm, int64planmodifier.UseStateForUnknown())
		}
		if replace {
			pm = append(pm, int64planmodifier.RequiresReplace())
		}
		return schema.Int64Attribute{Required: f.required, Optional: f.optional, Computed: computed, Description: desc, PlanModifiers: pm}
	case kStrings:
		var pm []planmodifier.List
		if keep {
			pm = append(pm, listplanmodifier.UseStateForUnknown())
		}
		if replace {
			pm = append(pm, listplanmodifier.RequiresReplace())
		}
		return schema.ListAttribute{ElementType: types.StringType, Required: f.required, Optional: f.optional, Computed: computed,
			Sensitive: f.sensitive, Description: desc, PlanModifiers: pm}
	default:
		var pm []planmodifier.String
		if keep {
			pm = append(pm, stringplanmodifier.UseStateForUnknown())
		}
		if replace {
			pm = append(pm, stringplanmodifier.RequiresReplace())
		}
		return schema.StringAttribute{Required: f.required, Optional: f.optional, Computed: computed,
			Sensitive: f.sensitive, Description: desc, PlanModifiers: pm}
	}
}

// itemPath is the URL of the object with the given ID.
func (r *objectResource) itemPath(id string) string {
	if r.spec.singleton != "" {
		return r.spec.collection
	}
	return r.spec.collection + url.PathEscape(id) + "/"
}

type attrGetter interface {
	GetAttribute(ctx context.Context, p path.Path, target any) diag.Diagnostics
}

// body turns the planned values into an API request body. Null and unknown
// values are left out, so the server keeps its own for them.
func (r *objectResource) body(ctx context.Context, src attrGetter) (map[string]any, diag.Diagnostics) {
	var diags diag.Diagnostics
	out := map[string]any{}
	for _, f := range r.spec.fields {
		if !f.writable() {
			continue
		}
		p := path.Root(f.name)
		switch f.kind {
		case kBool:
			var v types.Bool
			diags.Append(src.GetAttribute(ctx, p, &v)...)
			if !v.IsNull() && !v.IsUnknown() {
				out[f.apiName()] = v.ValueBool()
			}
		case kInt:
			var v types.Int64
			diags.Append(src.GetAttribute(ctx, p, &v)...)
			if !v.IsNull() && !v.IsUnknown() {
				out[f.apiName()] = v.ValueInt64()
			}
		case kStrings:
			var v types.List
			diags.Append(src.GetAttribute(ctx, p, &v)...)
			if !v.IsNull() && !v.IsUnknown() {
				var items []string
				diags.Append(v.ElementsAs(ctx, &items, false)...)
				if items == nil {
					items = []string{}
				}
				out[f.apiName()] = items
			}
		case kJSON:
			var v types.String
			diags.Append(src.GetAttribute(ctx, p, &v)...)
			obj, err := jsonObject(v, f.name)
			if err != nil {
				diags.AddAttributeError(p, "Invalid "+f.name, err.Error())
			} else if obj != nil {
				out[f.apiName()] = obj
			}
		default:
			var v types.String
			diags.Append(src.GetAttribute(ctx, p, &v)...)
			if !v.IsNull() && !v.IsUnknown() {
				out[f.apiName()] = v.ValueString()
			}
		}
	}
	return out, diags
}

// send writes the object: POST to create, or the update method to change it.
// A singleton is written over its current settings, so a setting the
// configuration leaves out keeps its value.
func (r *objectResource) send(ctx context.Context, method, path string, body map[string]any) (map[string]json.RawMessage, error) {
	if r.spec.prepare != nil {
		if err := r.spec.prepare(ctx, r.c, body); err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if err := r.c.Do(ctx, method, path, json.RawMessage(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *objectResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	body, diags := r.body(ctx, req.Plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	method, target := "POST", r.spec.collection
	if r.spec.createPath != "" {
		target = r.spec.createPath
	}
	if r.spec.singleton != "" {
		method, target = "PUT", r.spec.collection
	}
	obj, err := r.send(ctx, method, target, body)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the "+r.spec.noun, err.Error())
		return
	}
	resp.State.Raw = req.Plan.Raw
	if r.spec.createPath != "" {
		obj = map[string]json.RawMessage{}
		for _, field := range r.spec.fields {
			if value, present := body[field.apiName()]; present {
				obj[field.apiName()], _ = json.Marshal(value)
			}
		}
	}
	resp.Diagnostics.Append(r.apply(ctx, &resp.State, obj, true)...)
	if !resp.Diagnostics.HasError() && r.spec.createPath != "" {
		var identifier types.String
		resp.Diagnostics.Append(resp.State.GetAttribute(ctx, path.Root("id"), &identifier)...)
		var current map[string]json.RawMessage
		if err := r.c.Do(ctx, "GET", r.itemPath(identifier.ValueString()), nil, &current); err != nil {
			resp.Diagnostics.AddError("Could not read the created "+r.spec.noun, err.Error())
			return
		}
		resp.Diagnostics.Append(r.apply(ctx, &resp.State, current, false)...)
	}
}

func (r *objectResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var id types.String
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root(r.idAttr()), &id)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var obj map[string]json.RawMessage
	if err := r.c.Do(ctx, "GET", r.itemPath(id.ValueString()), nil, &obj); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the "+r.spec.noun, err.Error())
		return
	}
	if r.spec.verify != nil {
		if err := r.spec.verify(ctx, req.State, obj); err != nil {
			resp.Diagnostics.AddError("Could not read the "+r.spec.noun, err.Error())
			return
		}
	}
	resp.State = req.State
	resp.Diagnostics.Append(r.apply(ctx, &resp.State, obj, false)...)
}

func (r *objectResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	if r.spec.update == "" {
		resp.Diagnostics.AddError("Could not update the "+r.spec.noun,
			"Streamtime cannot change a "+r.spec.noun+" in place. Replace it instead.")
		return
	}
	var id types.String
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root(r.idAttr()), &id)...)
	body, diags := r.body(ctx, req.Plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	if r.spec.verify != nil {
		var cur map[string]json.RawMessage
		if err := r.c.Do(ctx, "GET", r.itemPath(id.ValueString()), nil, &cur); err != nil {
			resp.Diagnostics.AddError("Could not update the "+r.spec.noun, err.Error())
			return
		}
		if err := r.spec.verify(ctx, req.State, cur); err != nil {
			resp.Diagnostics.AddError("Could not update the "+r.spec.noun, err.Error())
			return
		}
	}
	for _, field := range r.spec.fields {
		if field.createOnly {
			delete(body, field.apiName())
		}
		if field.clearOnRemove {
			var planned, previous types.String
			resp.Diagnostics.Append(req.Plan.GetAttribute(ctx, path.Root(field.name), &planned)...)
			resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root(field.name), &previous)...)
			if planned.IsNull() && !previous.IsNull() && !previous.IsUnknown() {
				body[field.apiName()] = nil
			}
		}
	}
	if resp.Diagnostics.HasError() {
		return
	}
	obj, err := r.send(ctx, r.spec.update, r.itemPath(id.ValueString()), body)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the "+r.spec.noun, err.Error())
		return
	}
	resp.State.Raw = req.Plan.Raw
	resp.Diagnostics.Append(r.apply(ctx, &resp.State, obj, false)...)
}

func (r *objectResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	if r.spec.singleton != "" {
		resp.Diagnostics.AddWarning("The "+r.spec.noun+" settings stay as they are",
			"Streamtime keeps one set of "+r.spec.noun+" settings for the organization. Terraform stops managing them, and the current values remain.")
		return
	}
	var id types.String
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root(r.idAttr()), &id)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if r.spec.verify != nil {
		var cur map[string]json.RawMessage
		if err := r.c.Do(ctx, "GET", r.itemPath(id.ValueString()), nil, &cur); err != nil {
			if isGone(err) {
				return
			}
			resp.Diagnostics.AddError("Could not delete the "+r.spec.noun, err.Error())
			return
		}
		if err := r.spec.verify(ctx, req.State, cur); err != nil {
			resp.Diagnostics.AddError("Could not delete the "+r.spec.noun, err.Error())
			return
		}
	}
	if err := deleteIgnoringGone(ctx, r.c, r.itemPath(id.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the "+r.spec.noun, err.Error())
	}
}

func (r *objectResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := req.ID
	if r.spec.singleton != "" {
		id = r.spec.singleton
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root(r.idAttr()), id)...)
}

// idAttr is the attribute that holds the ID: "id", which is the key itself for
// objects the server identifies by UUID.
func (r *objectResource) idAttr() string { return "id" }

// apply reads the server's object into the state. The state already holds the
// planned values (after a write) or the prior ones (after a read).
func (r *objectResource) apply(ctx context.Context, st *tfsdk.State, obj map[string]json.RawMessage, created bool) diag.Diagnostics {
	var diags diag.Diagnostics
	if r.spec.reshape != nil {
		r.spec.reshape(obj)
	}
	for _, f := range r.spec.fields {
		diags.Append(r.applyField(ctx, st, f, obj, created)...)
	}
	if diags.HasError() {
		return diags
	}
	switch {
	case r.spec.singleton != "":
		diags.Append(st.SetAttribute(ctx, path.Root("id"), r.spec.singleton)...)
	case r.spec.key != "id":
		var k types.String
		diags.Append(st.GetAttribute(ctx, path.Root(r.spec.key), &k)...)
		diags.Append(st.SetAttribute(ctx, path.Root("id"), k)...)
	}
	return diags
}

func (r *objectResource) applyField(ctx context.Context, st *tfsdk.State, f field, obj map[string]json.RawMessage, created bool) diag.Diagnostics {
	p := path.Root(f.name)
	raw, present := obj[f.apiName()]
	explicitNull := present && string(bytes.TrimSpace(raw)) == "null"
	isNull := !present || explicitNull
	// settle is what a value becomes when the server gave none: null if the server
	// said null, and otherwise what the state already holds (null if still unknown).
	settle := func(unknown bool, null attr.Value) diag.Diagnostics {
		if explicitNull {
			return st.SetAttribute(ctx, p, null)
		}
		return unknownToNull(ctx, st, p, unknown, null)
	}

	switch f.kind {
	case kBool:
		var prior types.Bool
		d := st.GetAttribute(ctx, p, &prior)
		if d.HasError() {
			return d
		}
		var v bool
		switch {
		case f.hidden || isNull || json.Unmarshal(raw, &v) != nil:
			return settle(prior.IsUnknown(), types.BoolNull())
		}
		return st.SetAttribute(ctx, p, types.BoolValue(v))
	case kInt:
		var prior types.Int64
		d := st.GetAttribute(ctx, p, &prior)
		if d.HasError() {
			return d
		}
		var v int64
		switch {
		case f.hidden || isNull || json.Unmarshal(raw, &v) != nil:
			return settle(prior.IsUnknown(), types.Int64Null())
		}
		return st.SetAttribute(ctx, p, types.Int64Value(v))
	case kStrings:
		var prior types.List
		d := st.GetAttribute(ctx, p, &prior)
		if d.HasError() {
			return d
		}
		var v []string
		if f.hidden || isNull || json.Unmarshal(raw, &v) != nil {
			return settle(prior.IsUnknown(), types.ListNull(types.StringType))
		}
		if v == nil {
			v = []string{}
		}
		if f.unordered && !prior.IsNull() && !prior.IsUnknown() {
			var configured []string
			if diagnostics := prior.ElementsAs(ctx, &configured, false); diagnostics.HasError() {
				return diagnostics
			}
			slices.Sort(configured)
			serverValues := slices.Clone(v)
			slices.Sort(serverValues)
			if slices.Equal(configured, serverValues) {
				return nil
			}
		}
		lv, d := types.ListValueFrom(ctx, types.StringType, v)
		if d.HasError() {
			return d
		}
		return st.SetAttribute(ctx, p, lv)
	case kJSON:
		var prior types.String
		d := st.GetAttribute(ctx, p, &prior)
		if d.HasError() {
			return d
		}
		if f.hidden {
			return unknownToNull(ctx, st, p, prior.IsUnknown(), types.StringNull())
		}
		server := raw
		if f.masked && !prior.IsNull() && !prior.IsUnknown() {
			server = unmask(prior.ValueString(), raw)
		}
		return st.SetAttribute(ctx, p, keptOrServerJSON(prior, server, f.masked))
	default:
		var prior types.String
		d := st.GetAttribute(ctx, p, &prior)
		if d.HasError() {
			return d
		}
		if f.hidden || (f.once && !created) {
			return unknownToNull(ctx, st, p, prior.IsUnknown(), types.StringNull())
		}
		var v string
		if isNull || json.Unmarshal(raw, &v) != nil {
			return settle(prior.IsUnknown(), types.StringNull())
		}
		if f.once && v == "" {
			return unknownToNull(ctx, st, p, prior.IsUnknown(), types.StringNull())
		}
		if !prior.IsNull() && !prior.IsUnknown() && f.same != nil && f.same(prior.ValueString(), v) {
			return nil // keep what the configuration says
		}
		// An optional setting nobody configured stays null when the server holds an empty one.
		if prior.IsNull() && !f.computed && f.optional && v == "" {
			return nil
		}
		return st.SetAttribute(ctx, p, types.StringValue(v))
	}
}

// unknownToNull settles a value the server did not give: a planned-unknown one
// becomes null, anything else keeps what the state holds.
func unknownToNull(ctx context.Context, st *tfsdk.State, p path.Path, unknown bool, null attr.Value) diag.Diagnostics {
	if unknown {
		return st.SetAttribute(ctx, p, null)
	}
	return nil
}

// unmask puts the configured value back wherever the server shows ********, so a
// credential inside a JSON setting is not read as drift.
func unmask(configured string, server json.RawMessage) json.RawMessage {
	cv, err1 := decodeJSON([]byte(configured))
	sv, err2 := decodeJSON(server)
	if err1 != nil || err2 != nil {
		return server
	}
	out, err := json.Marshal(unmaskValue(cv, sv))
	if err != nil {
		return server
	}
	return out
}

func unmaskValue(configured, server any) any {
	switch s := server.(type) {
	case map[string]any:
		c, _ := configured.(map[string]any)
		for k, v := range s {
			s[k] = unmaskValue(c[k], v)
		}
		return s
	case string:
		if s == "********" {
			if c, ok := configured.(string); ok {
				return c
			}
		}
	}
	return server
}

// sameURL treats a trailing slash as no difference, as the server strips it.
func sameURL(configured, server string) bool {
	return strings.TrimRight(configured, "/") == strings.TrimRight(server, "/")
}

// sameText ignores surrounding whitespace, which the server trims.
func sameText(configured, server string) bool {
	return strings.TrimSpace(configured) == strings.TrimSpace(server)
}
