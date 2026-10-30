// Package provider is the Terraform provider: schema, configuration and the
// server version check.
package provider

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/config"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/resources"
	"github.com/streamtime-ai/terraform-provider-streamtime/internal/version"
)

type streamtimeProvider struct{ version string }

// New returns a provider factory for the given release version.
func New(version string) func() provider.Provider {
	return func() provider.Provider { return &streamtimeProvider{version: version} }
}

type model struct {
	APIURL             types.String `tfsdk:"api_url"`
	Organization       types.String `tfsdk:"organization"`
	APIKey             types.String `tfsdk:"api_key"`
	Token              types.String `tfsdk:"token"`
	CACertFile         types.String `tfsdk:"ca_cert_file"`
	InsecureSkipVerify types.Bool   `tfsdk:"insecure_skip_verify"`
}

func (p *streamtimeProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "streamtime"
	resp.Version = p.version
}

func (p *streamtimeProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manage Streamtime resources. Every value can be left out and set through the environment or the Streamtime CLI login instead.",
		Attributes: map[string]schema.Attribute{
			"api_url":              schema.StringAttribute{Optional: true, Description: "Address of your Streamtime installation, for example https://api.example.com. Environment: STREAMTIME_API_URL. There is no default."},
			"organization":         schema.StringAttribute{Optional: true, Description: "Organization identifier. Environment: STREAMTIME_ORG."},
			"api_key":              schema.StringAttribute{Optional: true, Sensitive: true, Description: "API key. Environment: STREAMTIME_API_KEY. Use this in CI and for long runs."},
			"token":                schema.StringAttribute{Optional: true, Sensitive: true, Description: "Login token. Environment: STREAMTIME_TOKEN. It expires; run 'streamtime login' again when it does. Set either api_key or token, not both."},
			"ca_cert_file":         schema.StringAttribute{Optional: true, Description: "Path to a PEM file with the certificate authority that signed the Streamtime certificate."},
			"insecure_skip_verify": schema.BoolAttribute{Optional: true, Description: "Skip TLS verification. Environment: STREAMTIME_VERIFY_SSL=false. Only for throwaway environments."},
		},
	}
}

func (p *streamtimeProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var m model
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	block := config.Block{
		APIURL: m.APIURL.ValueString(), Organization: m.Organization.ValueString(),
		APIKey: m.APIKey.ValueString(), Token: m.Token.ValueString(), CACertFile: m.CACertFile.ValueString(),
	}
	if !m.InsecureSkipVerify.IsNull() {
		v := m.InsecureSkipVerify.ValueBool()
		block.InsecureSkipVerify = &v
	}
	settings, err := config.Resolve(block, os.Getenv)
	if err != nil {
		resp.Diagnostics.AddError("Streamtime provider is not configured", err.Error())
		return
	}
	c, err := client.New(settings)
	if err != nil {
		resp.Diagnostics.AddError("Streamtime provider is not configured", err.Error())
		return
	}
	checkServerVersion(ctx, c, p.version, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.ResourceData = c
	resp.DataSourceData = c
}

// checkServerVersion compares the provider with the server. Provider newer is
// an error, older a warning; a version that cannot be compared only warns.
func checkServerVersion(ctx context.Context, c *client.Client, providerVersion string, d *diag.Diagnostics) {
	var info struct {
		Version string `json:"version"`
	}
	if err := c.GetPublic(ctx, "setup/info/", &info); err != nil {
		d.AddError("Could not read the Streamtime version", err.Error())
		return
	}
	r := version.Check(providerVersion, info.Version)
	switch r.Outcome {
	case version.ProviderNewer:
		d.AddError("Provider is newer than your Streamtime installation", r.Message)
	case version.ProviderOlder, version.Uncomparable:
		d.AddWarning("Provider and Streamtime versions differ", r.Message)
	}
}

func (p *streamtimeProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{resources.NewSecret, resources.NewBaseDomain}
}

func (p *streamtimeProvider) DataSources(context.Context) []func() datasource.DataSource {
	return nil
}
