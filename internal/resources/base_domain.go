package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

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

// NewBaseDomain returns the streamtime_base_domain resource.
func NewBaseDomain() resource.Resource { return &baseDomainResource{} }

type baseDomainResource struct{ c *client.Client }

type baseDomainModel struct {
	ID                     types.String    `tfsdk:"id"`
	Domain                 types.String    `tfsdk:"domain"`
	AutoCreateCertificates types.Bool      `tfsdk:"auto_create_certificates"`
	DNSProviderConfig      *dnsConfigModel `tfsdk:"dns_provider_config"`
	TLSCert                types.String    `tfsdk:"tls_cert"`
	TLSKey                 types.String    `tfsdk:"tls_key"`
	CertificateExpiry      types.String    `tfsdk:"certificate_expiry"`
}

// baseDomainResponse is the server's BaseDomain. Its DNS settings are read on their
// own, so a legacy row whose settings are not an object reads as no settings
// instead of failing the whole read. certificate_info is an object although the
// spec types it as a string; the provider does not use it, so it is not decoded.
type baseDomainResponse struct {
	streamtimeapi.BaseDomain
	DnsProviderConfig json.RawMessage `json:"dns_provider_config"`
	CertificateInfo   json.RawMessage `json:"certificate_info"`
}

func (r baseDomainResponse) dnsProvider() *streamtimeapi.DNSProviderConfigSchema_DnsProvider {
	var c streamtimeapi.DNSProviderConfigSchema
	if json.Unmarshal(r.DnsProviderConfig, &c) != nil {
		return nil
	}
	return c.DnsProvider
}

// dnsConfigModel and dnsProviderModel are the typed form of the backend's
// DNSProviderConfigSchema: {"dns_provider": {"provider_type": ..., <fields of that provider>}}.
type dnsConfigModel struct {
	DNSProvider *dnsProviderModel `tfsdk:"dns_provider"`
}

type dnsProviderModel struct {
	ProviderType    types.String `tfsdk:"provider_type"`
	HostedZoneID    types.String `tfsdk:"hosted_zone_id"`
	AWSAccessKey    types.String `tfsdk:"aws_access_key"`
	AWSAccessSecret types.String `tfsdk:"aws_access_secret"`
	AWSRegion       types.String `tfsdk:"aws_region"`
	Subdomain       types.String `tfsdk:"subdomain"`
	ZoneRecordID    types.String `tfsdk:"zone_record_id"`
	ZoneID          types.String `tfsdk:"zone_id"`
	APIToken        types.String `tfsdk:"api_token"`
	APIKey          types.String `tfsdk:"api_key"`
	APISecret       types.String `tfsdk:"api_secret"`
	ZoneApex        types.String `tfsdk:"zone_apex"`
	CertID          types.String `tfsdk:"cert_id"`
}

// dnsSettings names the settings each provider_type takes. zone_apex and cert_id are
// set by Streamtime, so the state may hold them from an earlier provider: they are
// left out for a provider that does not take them. Any other setting outside the
// list is a configuration error, which the API would also reject.
var dnsSettings = map[string][]string{
	"none":       {},
	"route53":    {"aws_access_key", "aws_access_secret", "aws_region", "hosted_zone_id", "zone_apex"},
	"stdns":      {"subdomain", "zone_record_id", "cert_id"},
	"cloudflare": {"api_token", "zone_id", "zone_apex"},
	"godaddy":    {"api_key", "api_secret", "zone_apex"},
}

func (p *dnsProviderModel) fields() map[string]types.String {
	return map[string]types.String{
		"hosted_zone_id": p.HostedZoneID, "aws_access_key": p.AWSAccessKey, "aws_access_secret": p.AWSAccessSecret,
		"aws_region": p.AWSRegion, "subdomain": p.Subdomain, "zone_record_id": p.ZoneRecordID, "zone_id": p.ZoneID,
		"api_token": p.APIToken, "api_key": p.APIKey, "api_secret": p.APISecret, "zone_apex": p.ZoneApex, "cert_id": p.CertID,
	}
}

// toAPI builds the request's DNS settings with the generated type for the chosen provider.
func (c *dnsConfigModel) toAPI() (streamtimeapi.DNSProviderConfigSchema, error) {
	var out streamtimeapi.DNSProviderConfigSchema
	if c == nil || c.DNSProvider == nil {
		return out, nil
	}
	p := c.DNSProvider
	pt := p.ProviderType.ValueString()
	kind := strings.ToLower(pt)
	if allowed, known := dnsSettings[kind]; known {
		for name, v := range p.fields() {
			if v.IsNull() || v.IsUnknown() || name == "zone_apex" || name == "cert_id" {
				continue
			}
			ok := false
			for _, a := range allowed {
				ok = ok || a == name
			}
			if !ok {
				return out, fmt.Errorf("%s is not a setting of provider_type %q. Remove it, or choose the provider it belongs to.", name, pt)
			}
		}
	}
	var u streamtimeapi.DNSProviderConfigSchema_DnsProvider
	var err error
	switch kind {
	case "route53":
		err = u.FromRoute53DNSProviderSchema(streamtimeapi.Route53DNSProviderSchema{ProviderType: pt,
			AwsAccessKey: p.AWSAccessKey.ValueString(), AwsAccessSecret: p.AWSAccessSecret.ValueString(),
			AwsRegion: p.AWSRegion.ValueString(), HostedZoneId: optString(p.HostedZoneID), ZoneApex: optString(p.ZoneApex)})
	case "stdns":
		err = u.FromSTDNSDNSProviderSchema(streamtimeapi.STDNSDNSProviderSchema{ProviderType: pt,
			Subdomain: p.Subdomain.ValueString(), ZoneRecordId: p.ZoneRecordID.ValueString(), CertId: optString(p.CertID)})
	case "cloudflare":
		err = u.FromCloudflareDNSProviderSchema(streamtimeapi.CloudflareDNSProviderSchema{ProviderType: pt,
			ApiToken: p.APIToken.ValueString(), ZoneId: p.ZoneID.ValueString(), ZoneApex: optString(p.ZoneApex)})
	case "godaddy":
		err = u.FromGodaddyDNSProviderSchema(streamtimeapi.GodaddyDNSProviderSchema{ProviderType: pt,
			ApiKey: p.APIKey.ValueString(), ApiSecret: p.APISecret.ValueString(), ZoneApex: optString(p.ZoneApex)})
	default:
		// none, or a type this provider does not know: the server names the valid ones.
		err = u.FromNoDNSProviderSchema(streamtimeapi.NoDNSProviderSchema{ProviderType: pt})
	}
	out.DnsProvider = &u
	return out, err
}

// serverDNS reads the server's DNS settings with the generated type its provider_type names.
func serverDNS(u *streamtimeapi.DNSProviderConfigSchema_DnsProvider) *dnsProviderModel {
	if u == nil {
		return nil
	}
	head, err := u.AsNoDNSProviderSchema()
	if err != nil {
		return nil
	}
	s := &dnsProviderModel{ProviderType: types.StringValue(head.ProviderType)}
	switch strings.ToLower(head.ProviderType) {
	case "route53":
		if v, err := u.AsRoute53DNSProviderSchema(); err == nil {
			s.AWSAccessKey, s.AWSAccessSecret, s.AWSRegion = fromServer(&v.AwsAccessKey), fromServer(&v.AwsAccessSecret), fromServer(&v.AwsRegion)
			s.HostedZoneID, s.ZoneApex = fromServer(v.HostedZoneId), fromServer(v.ZoneApex)
		}
	case "stdns":
		if v, err := u.AsSTDNSDNSProviderSchema(); err == nil {
			s.Subdomain, s.ZoneRecordID, s.CertID = fromServer(&v.Subdomain), fromServer(&v.ZoneRecordId), fromServer(v.CertId)
		}
	case "cloudflare":
		if v, err := u.AsCloudflareDNSProviderSchema(); err == nil {
			s.APIToken, s.ZoneID, s.ZoneApex = fromServer(&v.ApiToken), fromServer(&v.ZoneId), fromServer(v.ZoneApex)
		}
	case "godaddy":
		if v, err := u.AsGodaddyDNSProviderSchema(); err == nil {
			s.APIKey, s.APISecret, s.ZoneApex = fromServer(&v.ApiKey), fromServer(&v.ApiSecret), fromServer(v.ZoneApex)
		}
	}
	return s
}

func fromServer(v *string) types.String {
	if v == nil || *v == "" {
		return types.StringNull()
	}
	return types.StringValue(*v)
}

// serverOrKept is for server-assigned fields: the server's value wins; without one,
// a known planned value stays and an unknown one becomes null.
func serverOrKept(server, kept types.String) types.String {
	if !server.IsNull() {
		return server
	}
	if kept.IsUnknown() {
		return types.StringNull()
	}
	return kept
}

// mergeDNS builds the state's view of the DNS provider. Fields the user sets are kept
// as configured, because the server may redact credentials or normalise them; after an
// import there is nothing configured, so the server's values are adopted.
func mergeDNS(cur, a *dnsProviderModel) *dnsProviderModel {
	if a == nil {
		return cur
	}
	imported := cur == nil
	if imported {
		cur = &dnsProviderModel{}
	}
	pick := func(kept, server types.String) types.String {
		if imported {
			return server
		}
		return kept
	}
	out := &dnsProviderModel{
		HostedZoneID: pick(cur.HostedZoneID, a.HostedZoneID), AWSAccessKey: pick(cur.AWSAccessKey, a.AWSAccessKey),
		AWSAccessSecret: pick(cur.AWSAccessSecret, a.AWSAccessSecret), AWSRegion: pick(cur.AWSRegion, a.AWSRegion),
		Subdomain: pick(cur.Subdomain, a.Subdomain), ZoneRecordID: pick(cur.ZoneRecordID, a.ZoneRecordID),
		ZoneID: pick(cur.ZoneID, a.ZoneID), APIToken: pick(cur.APIToken, a.APIToken), APIKey: pick(cur.APIKey, a.APIKey),
		APISecret: pick(cur.APISecret, a.APISecret),
		ZoneApex:  serverOrKept(a.ZoneApex, cur.ZoneApex), CertID: serverOrKept(a.CertID, cur.CertID),
	}
	// The backend accepts "None" and stores "none"; either spelling is the same choice.
	out.ProviderType = cur.ProviderType
	if imported || !strings.EqualFold(cur.ProviderType.ValueString(), a.ProviderType.ValueString()) {
		out.ProviderType = a.ProviderType
	}
	return out
}

func (r *baseDomainResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_base_domain"
}

func (r *baseDomainResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A base domain that Streamtime serves workloads under.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{Computed: true, Description: "Same as domain. Use the domain to import.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
			"domain": schema.StringAttribute{Required: true, Description: "The domain name. Changing it creates a new base domain.",
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()}},
			"auto_create_certificates": schema.BoolAttribute{Optional: true, Computed: true,
				Description:   "Let Streamtime create and renew the certificate for this domain.",
				PlanModifiers: []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()}},
			"dns_provider_config": schema.SingleNestedAttribute{Required: true, Sensitive: true,
				Description: "How Streamtime proves ownership of the domain. May hold credentials.",
				Attributes: map[string]schema.Attribute{
					"dns_provider": schema.SingleNestedAttribute{Required: true,
						Description: "The DNS provider and its settings. Set only the fields that belong to the chosen provider_type.",
						Attributes: map[string]schema.Attribute{
							"provider_type":     schema.StringAttribute{Required: true, Description: "One of none, route53, stdns, cloudflare or godaddy. Use none when you supply tls_cert and tls_key yourself."},
							"hosted_zone_id":    schema.StringAttribute{Optional: true, Description: "route53: ID of the hosted zone that holds the records."},
							"aws_access_key":    schema.StringAttribute{Optional: true, Description: "route53: access key ID with permission to manage records in the zone."},
							"aws_access_secret": schema.StringAttribute{Optional: true, Description: "route53: secret access key paired with aws_access_key."},
							"aws_region":        schema.StringAttribute{Optional: true, Description: "route53: AWS region."},
							"subdomain":         schema.StringAttribute{Optional: true, Description: "stdns: the subdomain registered with Streamtime DNS."},
							"zone_record_id":    schema.StringAttribute{Optional: true, Description: "stdns: ID of the zone record."},
							"zone_id":           schema.StringAttribute{Optional: true, Description: "cloudflare: ID of the zone for the domain."},
							"api_token":         schema.StringAttribute{Optional: true, Description: "cloudflare: API token with permission to edit DNS."},
							"api_key":           schema.StringAttribute{Optional: true, Description: "godaddy: API key."},
							"api_secret":        schema.StringAttribute{Optional: true, Description: "godaddy: API secret."},
							"zone_apex": schema.StringAttribute{Optional: true, Computed: true,
								Description:   "The verified registered domain of the zone. Streamtime sets it once zone access is verified.",
								PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
							"cert_id": schema.StringAttribute{Optional: true, Computed: true,
								Description:   "stdns: legacy certificate ID. Streamtime sets it when one exists.",
								PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()}},
						},
					},
				},
			},
			"tls_cert":           schema.StringAttribute{Optional: true, Description: "PEM certificate to use instead of an automatic one."},
			"tls_key":            schema.StringAttribute{Optional: true, Sensitive: true, Description: "PEM private key for tls_cert."},
			"certificate_expiry": schema.StringAttribute{Computed: true, Description: "When the current certificate expires."},
		},
	}
}

func (r *baseDomainResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = configure(req, &resp.Diagnostics)
}

func domainPath(domain string) string { return "base-domains/" + url.PathEscape(domain) + "/" }

func optString(s types.String) *string {
	if s.IsNull() || s.IsUnknown() {
		return nil
	}
	v := s.ValueString()
	return &v
}

func (r *baseDomainResource) body(m baseDomainModel) (json.RawMessage, error) {
	dns, err := m.DNSProviderConfig.toAPI()
	if err != nil {
		return nil, err
	}
	b := streamtimeapi.BaseDomain{Domain: m.Domain.ValueString(), DnsProviderConfig: dns,
		TlsCert: optString(m.TLSCert), TlsKey: optString(m.TLSKey)}
	if !m.AutoCreateCertificates.IsNull() && !m.AutoCreateCertificates.IsUnknown() {
		v := m.AutoCreateCertificates.ValueBool()
		b.AutoCreateCertificates = &v
	}
	return withoutNulls(b)
}

// apply copies the server's view into state. The key and DNS settings are
// kept as planned when the server does not echo them.
func (m *baseDomainModel) apply(a baseDomainResponse) {
	m.ID = types.StringValue(a.Domain)
	m.Domain = types.StringValue(a.Domain)
	m.CertificateExpiry = types.StringValue("")
	if a.CertificateExpiry != nil {
		m.CertificateExpiry = types.StringValue(*a.CertificateExpiry)
	}
	if a.AutoCreateCertificates != nil {
		m.AutoCreateCertificates = types.BoolValue(*a.AutoCreateCertificates)
	} else if m.AutoCreateCertificates.IsUnknown() {
		m.AutoCreateCertificates = types.BoolValue(false)
	}
	if server := serverDNS(a.dnsProvider()); server != nil {
		if m.DNSProviderConfig == nil {
			m.DNSProviderConfig = &dnsConfigModel{}
		}
		m.DNSProviderConfig.DNSProvider = mergeDNS(m.DNSProviderConfig.DNSProvider, server)
	}
	// tls_cert and tls_key are only tracked when the configuration sets them.
	// An automatic certificate the server reports must not appear in state
	// for a null plan, or Terraform fails with an inconsistent result.
	// The server trims the certificate's trailing newline, so a certificate that
	// matches once whitespace is ignored is kept as configured; a different one is
	// real drift and is adopted. The key is never adopted: the server does not return
	// it in a form that round-trips.
	if a.TlsCert != nil && *a.TlsCert != "" && !m.TLSCert.IsNull() &&
		strings.TrimSpace(*a.TlsCert) != strings.TrimSpace(m.TLSCert.ValueString()) {
		m.TLSCert = types.StringValue(*a.TlsCert)
	}
}

func (r *baseDomainResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var m baseDomainModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not create the base domain", err.Error())
		return
	}
	var out baseDomainResponse
	if err := r.c.Do(ctx, "POST", "base-domains/", body, &out); err != nil {
		resp.Diagnostics.AddError("Could not create the base domain", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *baseDomainResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var m baseDomainModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out baseDomainResponse
	if err := r.c.Do(ctx, "GET", domainPath(m.ID.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Could not read the base domain", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *baseDomainResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var m baseDomainModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body, err := r.body(m)
	if err != nil {
		resp.Diagnostics.AddError("Could not update the base domain", err.Error())
		return
	}
	var out baseDomainResponse
	if err := r.c.Do(ctx, "PUT", domainPath(m.Domain.ValueString()), body, &out); err != nil {
		resp.Diagnostics.AddError("Could not update the base domain", err.Error())
		return
	}
	m.apply(out)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

func (r *baseDomainResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var m baseDomainModel
	resp.Diagnostics.Append(req.State.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := deleteIgnoringGone(ctx, r.c, domainPath(m.ID.ValueString())); err != nil {
		resp.Diagnostics.AddError("Could not delete the base domain", err.Error())
	}
}

func (r *baseDomainResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
