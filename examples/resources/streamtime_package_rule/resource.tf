resource "streamtime_package_rule" "kafka" {
  name               = "kafka-3x"
  repository         = "kafka"
  version_constraint = ">=3.6.0 <4.0.0"
  target_status      = "supported"
  default_version    = "3.8.0"
}
