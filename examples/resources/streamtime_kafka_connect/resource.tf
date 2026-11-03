# Kafka Connect for an existing Kafka cluster, to run connectors. Apply waits until it is ready.
resource "streamtime_kafka_connect" "connect" {
  identifier       = "orders-connect"
  kafka_id         = streamtime_kafka.orders.identifier
  connect_provider = "skc"

  # Optional. Every setting has a default; for example, the connector plugin archives to install.
  advanced_config = jsonencode({})

  timeouts = {
    create = "60m"
    delete = "60m"
  }
}
