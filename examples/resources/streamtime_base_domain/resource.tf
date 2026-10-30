resource "streamtime_base_domain" "example" {
  domain                   = "example.com"
  auto_create_certificates = true

  dns_provider_config = {
    dns_provider = {
      provider_type = "cloudflare"
      zone_id       = var.cloudflare_zone_id
      api_token     = var.cloudflare_api_token
    }
  }
}

variable "cloudflare_zone_id" {
  type = string
}

variable "cloudflare_api_token" {
  type      = string
  sensitive = true
}
