resource "streamtime_kafka_schema" "payments_value" {
  kafka       = streamtime_kafka.orders.identifier
  subject     = "payments-value"
  schema_type = "avro"
  schema      = file("${path.module}/payment.avsc")

  # Optional: the subject's own level. Leave it out to use the registry's default.
  compatibility = "BACKWARD"

  # Destroy soft-deletes the subject unless this is true.
  hard_delete = false
}
