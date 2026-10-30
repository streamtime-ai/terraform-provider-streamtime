terraform {
  required_providers {
    streamtime = {
      source = "streamtime-ai/streamtime"
    }
  }
}

# Every value can come from the environment (STREAMTIME_API_URL,
# STREAMTIME_ORG, STREAMTIME_API_KEY) or your Streamtime CLI login instead.
provider "streamtime" {
  api_url      = "https://api.example.com"
  organization = "acme"
  api_key      = var.streamtime_api_key
}

variable "streamtime_api_key" {
  type      = string
  sensitive = true
}
