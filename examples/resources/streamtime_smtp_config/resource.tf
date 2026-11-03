resource "streamtime_smtp_config" "mail" {
  enabled       = true
  host          = "smtp.example.com"
  port          = 587
  encryption    = "starttls"
  username      = "streamtime"
  password      = var.smtp_password
  from_email    = "streamtime@example.com"
  from_name     = "Streamtime"
  link_base_url = "https://streamtime.example.com"
}

variable "smtp_password" {
  type      = string
  sensitive = true
}
