resource "streamtime_alert_channel" "ops" {
  identifier = "ops"
  type       = "slack"

  receiver_config = jsonencode({
    api_url = var.slack_webhook_url
    channel = "#operations"
  })
}

variable "slack_webhook_url" {
  type      = string
  sensitive = true
}
