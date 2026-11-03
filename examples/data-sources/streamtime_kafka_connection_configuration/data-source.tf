# Confluent Platform on your own virtual machines (workload cpansible) only.
data "streamtime_kafka_connection_configuration" "vm" {
  kafka = "vm-cluster"
}

output "client_properties" {
  value     = data.streamtime_kafka_connection_configuration.vm.client_properties
  sensitive = true
}
