data "streamtime_fleet_configuration_profile" "aws" {
  fleet_provider = "aws"
}

# The settings a fleet's placement_config takes on this provider.
output "aws_placement_settings" {
  value = keys(jsondecode(data.streamtime_fleet_configuration_profile.aws.placement_schema).json_schema.properties)
}
