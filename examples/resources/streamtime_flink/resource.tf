# A single-job Confluent Flink cluster that runs a version of a Flink project.
# Apache Flink runs in a job workspace (mode = "session") and sizes itself from the Flink units.
# Apply waits until it is ready, and until a change is running.
resource "streamtime_flink" "etl" {
  identifier     = "etl"
  flink_provider = "cmf"
  mode           = "application"
  fleet       = streamtime_fleet.byok.identifier
  flink_units = 2

  project_version = {
    project = streamtime_flink_project.etl.identifier
    version = "1.0.0"
  }

  # Confluent Flink only: parallelism, resources and Flink settings.
  advanced_configuration = jsonencode({
    parallelism = 2
  })

  timeouts = {
    create = "30m"
    update = "30m"
    delete = "30m"
  }
}

output "flink_ui" {
  value = streamtime_flink.etl.ui_url
}
