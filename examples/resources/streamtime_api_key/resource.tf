resource "streamtime_api_key" "ci" {
  name = "ci-pipeline"
  role = "Agent"
}

# Streamtime shows the key value only when it creates the key.
output "ci_api_key" {
  value     = streamtime_api_key.ci.key
  sensitive = true
}
