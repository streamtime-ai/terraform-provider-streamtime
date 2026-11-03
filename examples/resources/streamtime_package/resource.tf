resource "streamtime_package" "kafka" {
  registry_url   = "https://charts.example.com"
  repository     = "kafka"
  tag            = "31.0.0"
  support_status = "supported"
}
