package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	dschema "github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// configureData stores the provider's client on a data source.
func configureData(req datasource.ConfigureRequest, d *diag.Diagnostics) *client.Client {
	if req.ProviderData == nil {
		return nil
	}
	c, ok := req.ProviderData.(*client.Client)
	if !ok {
		d.AddError("Unexpected provider data", fmt.Sprintf("Expected the Streamtime client, got %T.", req.ProviderData))
		return nil
	}
	return c
}

// canonicalJSON renders a server JSON value for a read-only attribute.
func canonicalJSON(raw json.RawMessage) types.String {
	return keptOrServerJSON(types.StringNull(), raw, false)
}

// ---- streamtime_fleet -------------------------------------------------------

// NewFleetDataSource returns the streamtime_fleet data source.
func NewFleetDataSource() datasource.DataSource { return &fleetDataSource{} }

type fleetDataSource struct{ c *client.Client }

type fleetDataModel struct {
	Identifier          types.String `tfsdk:"identifier"`
	FleetProvider       types.String `tfsdk:"fleet_provider"`
	Domain              types.String `tfsdk:"domain"`
	Network             types.String `tfsdk:"network"`
	TenancyMode         types.String `tfsdk:"tenancy_mode"`
	MaxTenants          types.Int64  `tfsdk:"max_tenants"`
	MaxKafkaUnits       types.Int64  `tfsdk:"max_kafka_units"`
	Autoscale           types.Bool   `tfsdk:"autoscale"`
	Workloads           types.String `tfsdk:"workloads"`
	PlacementConfig     types.String `tfsdk:"placement_config"`
	Status              types.String `tfsdk:"status"`
	ReconciliationState types.String `tfsdk:"reconciliation_state"`
	ConfigurationState  types.String `tfsdk:"configuration_state"`
}

func (d *fleetDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_fleet"
}

func (d *fleetDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Reads a fleet, for example one created outside Terraform, to place workloads on it.",
		Attributes: map[string]dschema.Attribute{
			"identifier":           dschema.StringAttribute{Required: true, Description: "The fleet's identifier."},
			"fleet_provider":       dschema.StringAttribute{Computed: true, Description: "Where the fleet runs."},
			"domain":               dschema.StringAttribute{Computed: true, Description: "Base domain of the fleet's endpoints."},
			"network":              dschema.StringAttribute{Computed: true, Description: "Identifier of the fleet's network, if any."},
			"tenancy_mode":         dschema.StringAttribute{Computed: true, Description: "Shared, Isolated or Dedicated."},
			"max_tenants":          dschema.Int64Attribute{Computed: true, Description: "Most workloads that can share the fleet."},
			"max_kafka_units":      dschema.Int64Attribute{Computed: true, Description: "Cluster capacity the fleet can hold, in capacity units."},
			"autoscale":            dschema.BoolAttribute{Computed: true, Description: "Whether node pools grow and shrink with load."},
			"workloads":            dschema.StringAttribute{Computed: true, Description: "Workload categories the fleet hosts, as JSON."},
			"placement_config":     dschema.StringAttribute{Computed: true, Description: "Placement settings, as JSON."},
			"status":               dschema.StringAttribute{Computed: true, Description: "Health of the fleet, for example Healthy."},
			"reconciliation_state": dschema.StringAttribute{Computed: true, Description: "Progress of the last change."},
			"configuration_state":  dschema.StringAttribute{Computed: true, Description: "Whether the fleet's settings are complete."},
		},
	}
}

func (d *fleetDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.c = configureData(req, &resp.Diagnostics)
}

func (d *fleetDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m fleetDataModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out fleetWire
	if err := d.c.Do(ctx, "GET", fleetPath(m.Identifier.ValueString()), nil, &out); err != nil {
		if isGone(err) {
			err = fmt.Errorf("fleet %q does not exist in this organization", m.Identifier.ValueString())
		}
		resp.Diagnostics.AddError("Could not read the fleet", err.Error())
		return
	}
	m.Network = types.StringNull()
	if out.Network != nil && *out.Network != "" {
		n, err := networkIdentifier(ctx, d.c, *out.Network, "")
		if err != nil {
			resp.Diagnostics.AddError("Could not read the fleet's network", err.Error())
			return
		}
		m.Network = n
	}
	m.Identifier = types.StringValue(out.Identifier)
	m.FleetProvider, m.Domain = types.StringValue(out.Provider), types.StringValue(out.Domain)
	m.TenancyMode = stringOrNull(out.TenancyMode)
	m.MaxTenants, m.MaxKafkaUnits = types.Int64PointerValue(out.MaxTenants), types.Int64PointerValue(out.MaxKafkaUnits)
	m.Autoscale = types.BoolPointerValue(out.Autoscale)
	m.Workloads, m.PlacementConfig = canonicalJSON(out.Workloads), canonicalJSON(out.PlacementConfig)
	m.Status, m.ReconciliationState = stringOrNull(out.Status), stringOrNull(out.ReconciliationState)
	m.ConfigurationState = stringOrNull(out.ConfigurationState)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// ---- streamtime_fleet_configuration_profiles --------------------------------

// NewFleetConfigurationProfilesDataSource returns streamtime_fleet_configuration_profiles.
func NewFleetConfigurationProfilesDataSource() datasource.DataSource {
	return &fleetProfilesDataSource{}
}

type fleetProfilesDataSource struct{ c *client.Client }

type fleetProfileSummary struct {
	Identifier      types.String `tfsdk:"identifier"`
	DisplayName     types.String `tfsdk:"display_name"`
	Description     types.String `tfsdk:"description"`
	Enabled         types.Bool   `tfsdk:"enabled"`
	DisabledReasons types.List   `tfsdk:"disabled_reasons"`
}

type fleetProfilesModel struct {
	Profiles []fleetProfileSummary `tfsdk:"profiles"`
}

// fleetProfileWire is one entry of GET fleet/configuration-profiles/
// (FleetConfigurationProfileSummary in the spec).
type fleetProfileWire struct {
	Identifier      string   `json:"identifier"`
	DisplayName     string   `json:"display_name"`
	Description     string   `json:"description"`
	Enabled         bool     `json:"enabled"`
	DisabledReasons []string `json:"disabled_reasons"`
}

func (d *fleetProfilesDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_fleet_configuration_profiles"
}

func (d *fleetProfilesDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Lists the places a fleet can run (fleet providers) and whether this organization can use each one.",
		Attributes: map[string]dschema.Attribute{
			"profiles": dschema.ListNestedAttribute{Computed: true, Description: "One entry per fleet provider.",
				NestedObject: dschema.NestedAttributeObject{Attributes: map[string]dschema.Attribute{
					"identifier":   dschema.StringAttribute{Computed: true, Description: "Value for a fleet's fleet_provider."},
					"display_name": dschema.StringAttribute{Computed: true, Description: "Name shown in Streamtime."},
					"description":  dschema.StringAttribute{Computed: true, Description: "What the provider does."},
					"enabled":      dschema.BoolAttribute{Computed: true, Description: "Whether this organization can create fleets with it."},
					"disabled_reasons": dschema.ListAttribute{Computed: true, ElementType: types.StringType,
						Description: "Why it cannot be used, when it is not enabled."},
				}}},
		},
	}
}

func (d *fleetProfilesDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.c = configureData(req, &resp.Diagnostics)
}

func (d *fleetProfilesDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	var out []fleetProfileWire
	if err := d.c.Do(ctx, "GET", "fleet/configuration-profiles/", nil, &out); err != nil {
		resp.Diagnostics.AddError("Could not list fleet providers", err.Error())
		return
	}
	m := fleetProfilesModel{Profiles: []fleetProfileSummary{}}
	for _, p := range out {
		reasons, diags := types.ListValueFrom(ctx, types.StringType, append([]string{}, p.DisabledReasons...))
		resp.Diagnostics.Append(diags...)
		m.Profiles = append(m.Profiles, fleetProfileSummary{
			Identifier: types.StringValue(p.Identifier), DisplayName: types.StringValue(p.DisplayName),
			Description: types.StringValue(p.Description), Enabled: types.BoolValue(p.Enabled), DisabledReasons: reasons,
		})
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// ---- streamtime_fleet_configuration_profile ---------------------------------

// NewFleetConfigurationProfileDataSource returns streamtime_fleet_configuration_profile.
func NewFleetConfigurationProfileDataSource() datasource.DataSource {
	return &fleetProfileDataSource{}
}

type fleetProfileDataSource struct{ c *client.Client }

type fleetProfileModel struct {
	FleetProvider        types.String `tfsdk:"fleet_provider"`
	PlacementSchema      types.String `tfsdk:"placement_schema"`
	AdvancedConfigSchema types.String `tfsdk:"advanced_config_schema"`
}

// fleetProfileDetailWire is GET fleet/configuration-profiles/{provider}/
// (FleetConfigurationProfile in the spec).
type fleetProfileDetailWire struct {
	Provider       string          `json:"provider"`
	Placement      json.RawMessage `json:"placement"`
	AdvancedConfig json.RawMessage `json:"advanced_config"`
}

func (d *fleetProfileDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_fleet_configuration_profile"
}

func (d *fleetProfileDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Describes the placement and advanced settings one fleet provider takes, to write a fleet's " +
			"placement_config and advanced_config. The cloud account must exist first: some choices come from it.",
		Attributes: map[string]dschema.Attribute{
			"fleet_provider": dschema.StringAttribute{Required: true, Description: "The fleet provider, for example aws."},
			"placement_schema": dschema.StringAttribute{Computed: true,
				Description: "The placement settings' schema, as JSON (use jsondecode())."},
			"advanced_config_schema": dschema.StringAttribute{Computed: true,
				Description: "The advanced settings' schema for an empty placement, as JSON. For some providers it depends on " +
					"the placement and holds only a detail message."},
		},
	}
}

func (d *fleetProfileDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.c = configureData(req, &resp.Diagnostics)
}

func (d *fleetProfileDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m fleetProfileModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out fleetProfileDetailWire
	if err := d.c.Do(ctx, "GET", "fleet/configuration-profiles/"+url.PathEscape(m.FleetProvider.ValueString())+"/", nil, &out); err != nil {
		resp.Diagnostics.AddError("Could not read the fleet provider's settings", err.Error())
		return
	}
	m.PlacementSchema, m.AdvancedConfigSchema = canonicalJSON(out.Placement), canonicalJSON(out.AdvancedConfig)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// ---- streamtime_fleet_node_pools --------------------------------------------

// NewFleetNodePoolsDataSource returns streamtime_fleet_node_pools.
func NewFleetNodePoolsDataSource() datasource.DataSource { return &nodePoolsDataSource{} }

type nodePoolsDataSource struct{ c *client.Client }

type nodePoolModel struct {
	Size     types.Int64  `tfsdk:"size"`
	Metadata types.String `tfsdk:"metadata"`
}

type nodePoolsModel struct {
	Fleet     types.String    `tfsdk:"fleet"`
	NodePools []nodePoolModel `tfsdk:"node_pools"`
}

func (d *nodePoolsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_fleet_node_pools"
}

func (d *nodePoolsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Lists a fleet's node pools. Streamtime manages node pools with the fleet; they are read-only here.",
		Attributes: map[string]dschema.Attribute{
			"fleet": dschema.StringAttribute{Required: true, Description: "The fleet's identifier."},
			"node_pools": dschema.ListNestedAttribute{Computed: true, Description: "The fleet's node pools.",
				NestedObject: dschema.NestedAttributeObject{Attributes: map[string]dschema.Attribute{
					"size":     dschema.Int64Attribute{Computed: true, Description: "Number of nodes."},
					"metadata": dschema.StringAttribute{Computed: true, Description: "Details of the pool, as JSON."},
				}}},
		},
	}
}

func (d *nodePoolsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.c = configureData(req, &resp.Diagnostics)
}

func (d *nodePoolsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m nodePoolsModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	items, err := listAll(ctx, d.c, fleetPath(m.Fleet.ValueString())+"node-pools/")
	if err != nil {
		if isGone(err) {
			err = fmt.Errorf("fleet %q does not exist in this organization", m.Fleet.ValueString())
		}
		resp.Diagnostics.AddError("Could not list the fleet's node pools", err.Error())
		return
	}
	m.NodePools = []nodePoolModel{}
	for _, raw := range items {
		var p struct {
			Size     int64           `json:"size"`
			Metadata json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			resp.Diagnostics.AddError("Could not read a node pool", err.Error())
			return
		}
		m.NodePools = append(m.NodePools, nodePoolModel{Size: types.Int64Value(p.Size), Metadata: canonicalJSON(p.Metadata)})
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}

// ---- streamtime_kafka_connection_configuration ------------------------------

// NewKafkaConnectionConfigurationDataSource returns streamtime_kafka_connection_configuration.
func NewKafkaConnectionConfigurationDataSource() datasource.DataSource {
	return &kafkaConnectionDataSource{}
}

type kafkaConnectionDataSource struct{ c *client.Client }

type kafkaConnectionModel struct {
	Kafka                 types.String `tfsdk:"kafka"`
	ClientProperties      types.String `tfsdk:"client_properties"`
	CACertificate         types.String `tfsdk:"ca_certificate"`
	CAFilename            types.String `tfsdk:"ca_filename"`
	ControlCenterUsername types.String `tfsdk:"control_center_username"`
	ControlCenterPassword types.String `tfsdk:"control_center_password"`
}

func (d *kafkaConnectionDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_kafka_connection_configuration"
}

func (d *kafkaConnectionDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = dschema.Schema{
		Description: "Reads what a client needs to connect to a Confluent Platform cluster on your own virtual machines " +
			"(workload cpansible): client properties, the CA certificate and the Control Center login. " +
			"Other Kafka providers are not supported. The values include credentials and are stored in the Terraform state; " +
			"protect the state accordingly.",
		Attributes: map[string]dschema.Attribute{
			"kafka": dschema.StringAttribute{Required: true, Description: "Identifier of the Kafka cluster."},
			"client_properties": dschema.StringAttribute{Computed: true, Sensitive: true,
				Description: "Kafka client properties (bootstrap servers, security settings and credentials), in .properties format."},
			"ca_certificate":          dschema.StringAttribute{Computed: true, Description: "PEM CA certificate the client trusts."},
			"ca_filename":             dschema.StringAttribute{Computed: true, Description: "File name client_properties expects the CA certificate at."},
			"control_center_username": dschema.StringAttribute{Computed: true, Description: "Control Center user name, if Control Center is installed."},
			"control_center_password": dschema.StringAttribute{Computed: true, Sensitive: true,
				Description: "Control Center password, if Control Center is installed."},
		},
	}
}

func (d *kafkaConnectionDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	d.c = configureData(req, &resp.Diagnostics)
}

func (d *kafkaConnectionDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var m kafkaConnectionModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var out struct {
		ClientProperties      string  `json:"client_properties"`
		CACertificate         string  `json:"ca_certificate"`
		CAFilename            string  `json:"ca_filename"`
		ControlCenterUsername *string `json:"control_center_username"`
		ControlCenterPassword *string `json:"control_center_password"`
	}
	kafka := m.Kafka.ValueString()
	if err := d.c.Do(ctx, "GET", kafkaPath(kafka)+"connection-configuration/", nil, &out); err != nil {
		if isGone(err) {
			err = fmt.Errorf("Kafka cluster %q does not exist in this organization", kafka)
		}
		resp.Diagnostics.AddError("Could not read the Kafka connection configuration", err.Error())
		return
	}
	m.ClientProperties = types.StringValue(out.ClientProperties)
	m.CACertificate, m.CAFilename = types.StringValue(out.CACertificate), types.StringValue(out.CAFilename)
	m.ControlCenterUsername, m.ControlCenterPassword = stringOrNull(out.ControlCenterUsername), stringOrNull(out.ControlCenterPassword)
	resp.Diagnostics.Append(resp.State.Set(ctx, m)...)
}
