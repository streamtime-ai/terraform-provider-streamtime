# A fleet on your own Kubernetes cluster (bring your own Kubernetes). Apply registers
# it; then install the Agent on the cluster from the fleet's page in Streamtime.
resource "streamtime_fleet" "byok" {
  identifier     = "prod"
  fleet_provider = "byok"
  domain         = streamtime_base_domain.example.domain

  workloads = jsonencode({
    streaming = {
      enabled   = true
      providers = ["strimzi"]
    }
  })

  # Cloud fleets also take placement_config and, for some providers, advanced_config.
  # Their fields depend on the fleet provider: read them from
  # data.streamtime_fleet_configuration_profile.<name>.placement_schema.

  timeouts = {
    create = "90m"
    delete = "60m"
  }
}
