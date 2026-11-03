# Copy topics from one Streamtime Kafka cluster to another. Apply waits until the replicator is ready.
resource "streamtime_kafka_replicator" "mirror" {
  identifier           = "east-to-west"
  replicator_provider  = "MirrorMaker2"
  source_kafka_id      = streamtime_kafka.east.identifier
  destination_kafka_id = streamtime_kafka.west.identifier

  # The credentials for both clusters are stored as secrets that must already exist.
  source_secret_path      = "builtin:///replicators/east-to-west/source-secret"
  destination_secret_path = "builtin:///replicators/east-to-west/destination-secret"

  replication_mode         = "active-passive"
  lag_alert_threshold      = 1000
  lag_alert_threshold_unit = "messages"

  topic_whitelist = "orders.*,payments"

  timeouts = {
    create = "60m"
    delete = "60m"
  }
}

# To copy from a cluster outside Streamtime, leave out source_kafka_id and describe how to reach it:
#   source_connection_details = jsonencode({
#     bootstrap_servers = "kafka.example.com:9093"
#     security_protocol = "SASL_SSL"
#     sasl_mechanism    = "PLAIN"
#   })
