# A virtual Kafka cluster: its own endpoint and sign-in for a share of a Kafka cluster.
# Apply waits until it is ready, and a change to its capacity is deployed in place.
resource "streamtime_virtual_kafka" "team_a" {
  identifier  = "team-a"
  kafka_id    = streamtime_kafka.orders.identifier
  kafka_units = 1

  # The sign-in provider clients use.
  advanced_config = jsonencode({
    oauth = {
      client_id         = "team-a"
      client_secret     = var.oauth_client_secret
      issuer            = "https://login.example.com"
      jwks_endpoint     = "https://login.example.com/jwks"
      authorization_uri = "https://login.example.com/authorize"
      token_uri         = "https://login.example.com/token"
      audience          = "team-a"
    }
  })

  timeouts = {
    create = "60m"
    update = "60m"
    delete = "60m"
  }
}

output "virtual_kafka_endpoints" {
  value     = jsondecode(streamtime_virtual_kafka.team_a.resources).endpoints
  sensitive = true
}
