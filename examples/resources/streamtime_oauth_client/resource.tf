resource "streamtime_oauth_client" "ingest" {
  identifier     = "ingest"
  name           = "Ingest service"
  kafka_clusters = ["orders"]
}

output "ingest_client_secret" {
  value     = streamtime_oauth_client.ingest.client_secret
  sensitive = true
}
