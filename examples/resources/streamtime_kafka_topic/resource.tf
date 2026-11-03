resource "streamtime_kafka_topic" "payments" {
  kafka              = streamtime_kafka.orders.identifier
  name               = "payments"
  partitions         = 6
  replication_factor = 3

  config = {
    "retention.ms"   = "604800000"
    "cleanup.policy" = "delete"
  }
}
