resource "streamtime_cloud_account" "aws" {
  identifier     = "aws-prod"
  cloud_provider = "aws"

  configuration = jsonencode({
    aws_access_key    = var.aws_access_key
    aws_access_secret = var.aws_access_secret
  })
}

variable "aws_access_key" {
  type = string
}

variable "aws_access_secret" {
  type      = string
  sensitive = true
}
