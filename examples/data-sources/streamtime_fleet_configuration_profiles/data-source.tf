data "streamtime_fleet_configuration_profiles" "all" {}

# Fleet providers this organization can use.
output "fleet_providers" {
  value = [for p in data.streamtime_fleet_configuration_profiles.all.profiles : p.identifier if p.enabled]
}
