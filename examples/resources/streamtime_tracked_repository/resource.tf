resource "streamtime_tracked_repository" "agent" {
  repository   = "streamtime/streamtime-agent"
  repo_type    = "oci"
  registry_url = "docker.io"
}

resource "streamtime_tracked_repository" "kafka" {
  repository      = "kafka"
  repo_type       = "helm"
  helm_repository = streamtime_helm_repository.charts.name
}
