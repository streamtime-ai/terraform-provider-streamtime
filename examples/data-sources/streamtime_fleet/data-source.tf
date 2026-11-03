data "streamtime_fleet" "prod" {
  identifier = "prod"
}

output "fleet_status" {
  value = data.streamtime_fleet.prod.status
}
