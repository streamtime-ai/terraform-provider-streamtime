# A Flink project with its own artifact store. Your CI system uploads versions to it
# with the project's CI key, which you create when you set up CI (the project page, or
# `streamtime flink-project rotate-key etl`). Terraform does not manage the key.
resource "streamtime_flink_project" "etl" {
  identifier        = "etl"
  name              = "ETL jobs"
  runtime           = "java"
  cluster           = streamtime_flink.shared.identifier # a Job Workspace
  flink_unit_budget = 2

  metadata = jsonencode({
    entry_class = "com.acme.etl.Main"
    labels      = { team = "data" }
  })

  store = {
    store_type     = "oras"
    url            = "https://ghcr.io"
    authentication = "token"
    # A read-only, expiring token limited to this project's artifacts.
    token = var.ghcr_read_token
  }
}

variable "ghcr_read_token" {
  type      = string
  sensitive = true
}
