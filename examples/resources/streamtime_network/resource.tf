# Streamtime creates the VPC in the cloud account and fills in network_id.
resource "streamtime_network" "prod" {
  identifier    = "prod-vpc"
  name          = "prod-vpc"
  cloud_account = streamtime_cloud_account.aws.identifier
  region        = "eu-west-1"
  cidr          = "10.20.0.0/16"
}

# Or use a VPC you already have: set network_id and nothing is created.
resource "streamtime_network" "existing" {
  identifier    = "shared-vpc"
  name          = "shared-vpc"
  cloud_account = streamtime_cloud_account.aws.identifier
  network_id    = "vpc-0123456789abcdef0"
}
