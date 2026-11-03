data "streamtime_fleet_node_pools" "prod" {
  fleet = "prod"
}

output "node_count" {
  value = sum([for p in data.streamtime_fleet_node_pools.prod.node_pools : p.size])
}
