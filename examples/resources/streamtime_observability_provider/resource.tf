resource "streamtime_observability_provider" "datadog" {
  identifier = "datadog"
  vendor     = "datadog"

  configuration = jsonencode({
    api_key = var.datadog_api_key
    site    = "datadoghq.com"
  })
}

variable "datadog_api_key" {
  type      = string
  sensitive = true
}
