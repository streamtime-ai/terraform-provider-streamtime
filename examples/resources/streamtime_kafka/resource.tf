# A Kafka cluster on an existing fleet. Apply waits until the cluster is ready.
resource "streamtime_kafka" "orders" {
  identifier  = "orders"
  workload    = "strimzi"
  fleet       = streamtime_fleet.byok.identifier
  kafka_units = 3

  tags = {
    team = "payments"
  }

  # Component versions, values and authentication. The fields depend on the workload.
  advanced_config = jsonencode({})

  timeouts = {
    create = "60m"
    delete = "60m"
  }
}

output "bootstrap_servers" {
  value = jsondecode(streamtime_kafka.orders.spec).bootstrapServers
}
