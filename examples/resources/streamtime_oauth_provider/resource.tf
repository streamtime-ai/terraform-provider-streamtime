resource "streamtime_oauth_provider" "sso" {
  identifier             = "sso"
  issuer                 = "https://accounts.example.com"
  client_id              = "streamtime"
  jwks_endpoint_uri      = "https://accounts.example.com/keys"
  token_endpoint_uri     = "https://accounts.example.com/token"
  authorize_endpoint_uri = "https://accounts.example.com/authorize"

  client_secret = var.oidc_client_secret
}

variable "oidc_client_secret" {
  type      = string
  sensitive = true
}
