package provider_test

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAcc_OrganizationAndCatalog(t *testing.T) {
	cases := []struct {
		name       string
		collection string
		config     string
		attribute  string
		first      string
		second     string
		ignore     []string
		requires   []string
		variables  map[string]string
	}{
		{
			name: "member", collection: "members/", attribute: "role", first: "Administrator", second: "Cluster Administrator",
			config: `email = "%[1]s@example.com"
role = "%[2]s"
initial_password = "Acceptance-Only-Password-123"`,
			ignore: []string{"initial_password"},
		},
		{
			name: "api_key", collection: "api-keys/", attribute: "name", first: "first", second: "second",
			config: `name = "%[1]s-%[2]s"
role = "Agent"`, ignore: []string{"key"},
		},
		{
			name: "alert_channel", collection: "alert-channels/", attribute: "type", first: "webhook", second: "webhook",
			config: `identifier = "%[1]s"
type = "webhook"
receiver_config = jsonencode({receiver_type = "webhook", url = "https://example.com/%[2]s"})`,
		},
		{
			name: "helm_repository", collection: "helm-repositories/", attribute: "base_url", first: "https://example.com/first", second: "https://example.com/second",
			config: `name = "%[1]s"
base_url = "%[2]s"`,
		},
		{
			name: "tracked_repository", collection: "tracked-repositories/", attribute: "template_values", first: `{"default":{"replicas":1}}`, second: `{"default":{"replicas":2}}`,
			config: `repository = "%[1]s"
repo_type = "oci"
registry_url = "registry.example.com"
template_values = %[2]q`,
		},
		{
			name: "package", collection: "packages/", attribute: "support_status", first: "supported", second: "deprecated",
			config: `repository = "%[1]s"
registry_url = "registry.example.com"
tag = "1.0.0"
support_status = "%[2]s"`,
		},
		{
			name: "package_rule", collection: "package-rules/", attribute: "description", first: "first", second: "second",
			config: `name = "%[1]s"
repository = "%[1]s"
description = "%[2]s"`,
		},
		{
			name: "oauth_client", collection: "oauth-clients/", attribute: "name", first: "first",
			config: `identifier = "%[1]s"
name = "%[2]s"`, ignore: []string{"client_secret"}, requires: []string{"STREAMTIME_ACC_BUILTIN_IDENTITY"},
		},
		{
			name: "oauth_provider", collection: "oauth-providers/", attribute: "client_id", first: "configured",
			config: `identifier = "%[1]s"
issuer = var.acceptance_issuer
client_id = var.acceptance_client_id
client_secret = var.acceptance_client_secret
jwks_endpoint_uri = var.acceptance_jwks
token_endpoint_uri = var.acceptance_token_endpoint
authorize_endpoint_uri = var.acceptance_authorize_endpoint`,
			ignore: []string{"client_secret"}, variables: map[string]string{
				"acceptance_issuer": "STREAMTIME_ACC_OIDC_ISSUER", "acceptance_client_id": "STREAMTIME_ACC_OIDC_CLIENT_ID",
				"acceptance_client_secret": "STREAMTIME_ACC_OIDC_CLIENT_SECRET",
				"acceptance_jwks":          "STREAMTIME_ACC_OIDC_JWKS_URL", "acceptance_token_endpoint": "STREAMTIME_ACC_OIDC_TOKEN_URL",
				"acceptance_authorize_endpoint": "STREAMTIME_ACC_OIDC_AUTHORIZE_URL"},
		},
		{
			name: "observability_provider", collection: "observability-providers/", attribute: "vendor", first: "datadog",
			config: `identifier = "%[1]s"
vendor = "%[2]s"
configuration = jsonencode({api_key = var.acceptance_datadog_key, site = "datadoghq.com"})`,
			ignore: []string{"configuration"}, variables: map[string]string{"acceptance_datadog_key": "STREAMTIME_ACC_DATADOG_API_KEY"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, variable := range testCase.requires {
				if os.Getenv(variable) == "" {
					t.Skipf("%s is required for this acceptance case", variable)
				}
			}
			variableDeclarations := ""
			for variable, environment := range testCase.variables {
				value := os.Getenv(environment)
				if value == "" {
					t.Skipf("%s is required for this acceptance case", environment)
				}
				t.Setenv("TF_VAR_"+variable, value)
				variableDeclarations += "variable \"" + variable + "\" {\n type = string\n sensitive = true\n}\n"
			}
			identifier := "tfacc" + acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)
			resourceType := "streamtime_" + testCase.name
			resourceName := resourceType + ".test"
			configuration := func(value string) string {
				return variableDeclarations + "provider \"streamtime\" {}\nresource \"" + resourceType + "\" \"test\" {\n" +
					fmt.Sprintf(testCase.config, identifier, value) + "\n}\n"
			}
			steps := []resource.TestStep{{Config: configuration(testCase.first),
				Check: resource.ComposeAggregateTestCheckFunc(resource.TestCheckResourceAttrSet(resourceName, "id"),
					resource.TestCheckResourceAttrSet(resourceName, testCase.attribute))}}
			if testCase.second != "" {
				steps = append(steps, resource.TestStep{Config: configuration(testCase.second)})
			}
			steps = append(steps, resource.TestStep{ResourceName: resourceName, ImportState: true,
				ImportStateVerify: true, ImportStateVerifyIgnore: testCase.ignore})
			resource.Test(t, resource.TestCase{
				PreCheck: func() { preCheck(t) }, ProtoV6ProviderFactories: protoV6Factories,
				CheckDestroy: destroyed(resourceType, testCase.collection, "id"), Steps: steps,
			})
		})
	}
}

func TestAcc_SMTPConfig(t *testing.T) {
	if os.Getenv("STREAMTIME_ACC_SMTP") != "1" {
		t.Skip("set STREAMTIME_ACC_SMTP=1 only for a disposable organization's mail settings")
	}
	configuration := func(port int) string {
		return fmt.Sprintf(`provider "streamtime" {}
resource "streamtime_smtp_config" "test" {
  enabled = false
  host = "smtp.example.com"
  port = %d
  encryption = "starttls"
}`, port)
	}
	resource.Test(t, resource.TestCase{
		PreCheck: func() { preCheck(t) }, ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy: func(_ *terraform.State) error {
			status, err := apiStatus("smtp-config/")
			if err != nil {
				return err
			}
			if status != http.StatusOK {
				return fmt.Errorf("mail settings should remain after destroy, got status %d", status)
			}
			return nil
		},
		Steps: []resource.TestStep{
			{Config: configuration(587), Check: resource.TestCheckResourceAttr("streamtime_smtp_config.test", "port", "587")},
			{Config: configuration(2525), Check: resource.TestCheckResourceAttr("streamtime_smtp_config.test", "port", "2525")},
			{ResourceName: "streamtime_smtp_config.test", ImportState: true, ImportStateVerify: true},
		},
	})
}
