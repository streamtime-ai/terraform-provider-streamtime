# A single-job Flink cluster that runs a version of a Flink project.
# Apply waits until it is ready, and until a change is running.
resource "streamtime_flink" "etl" {
  identifier  = "etl"
  mode        = "application"
  fleet       = streamtime_fleet.byok.identifier
  flink_units = 2

  project_version = {
    project = streamtime_flink_project.etl.identifier
    version = "1.0.0"
  }

  # Parallelism, resources and Flink settings.
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
