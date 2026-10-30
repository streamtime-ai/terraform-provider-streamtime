resource "streamtime_secret" "db_password" {
  path    = "apps/db-password"
  content = var.db_password
}

variable "db_password" {
  type      = string
  sensitive = true
}
