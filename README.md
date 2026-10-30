# Terraform provider for Streamtime

Manage Streamtime resources with Terraform.

```hcl
terraform {
  required_providers {
    streamtime = {
      source = "streamtime-ai/streamtime"
    }
  }
}

provider "streamtime" {
  api_url      = "https://api.example.com"
  organization = "acme"
  api_key      = var.streamtime_api_key
}
```

Every setting can come from the environment instead
(`STREAMTIME_API_URL`, `STREAMTIME_ORG`, `STREAMTIME_API_KEY` or
`STREAMTIME_TOKEN`) or from your Streamtime CLI login. There is no default
address. Settings in the provider block win over the environment, which wins
over the CLI login.

Use an API key in CI and for long runs. A login token expires; when it does,
run `streamtime login` again.

The provider checks your Streamtime version when it starts. Use a provider
release that is not newer than your Streamtime installation.

## Development

```bash
go test ./...    # unit tests (Go 1.25)
```

Provider releases carry the same version as the Streamtime release they
were built with. Report bugs in this repository's issues.

Licensed under MPL-2.0.
