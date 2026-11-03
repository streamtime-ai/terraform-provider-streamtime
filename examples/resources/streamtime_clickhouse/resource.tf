# A ClickHouse database on an existing fleet. Apply waits until it is ready,
# and until a change is running.
resource "streamtime_clickhouse" "events" {
  identifier     = "events"
  fleet          = streamtime_fleet.byok.identifier
  retention_days = 14

  tags = {
    team = "analytics"
  }

  # Shards, replicas, resources and ClickHouse settings.
  advanced_configuration = jsonencode({
    shards   = 1
    replicas = 2
  })

  timeouts = {
    create = "30m"
    update = "30m"
    delete = "30m"
  }
}

output "clickhouse_http_url" {
  value = streamtime_clickhouse.events.http_url
}
