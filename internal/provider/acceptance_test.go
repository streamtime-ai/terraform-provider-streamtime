package provider_test

// Acceptance suite: the provider driven by a real Terraform CLI against a running
// Streamtime backend. It is skipped unless TF_ACC=1 and runs through
// ./run-acceptance.sh, never ./run-tests.sh.
//
// Every step is a full apply followed by a plan that must be empty, so a server
// default, a normalised value or a reordered field that the provider does not
// absorb fails here instead of in a customer's pipeline.

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/provider"
)

var protoV6Factories = map[string]func() (tfprotov6.ProviderServer, error){
	"streamtime": providerserver.NewProtocol6WithError(provider.New("acceptance")()),
}

// preCheck fails early and names what is missing, instead of a confusing provider error.
func preCheck(t *testing.T) {
	t.Helper()
	for _, v := range []string{"STREAMTIME_API_URL", "STREAMTIME_ORG"} {
		if os.Getenv(v) == "" {
			t.Fatalf("%s must be set to run the acceptance suite", v)
		}
	}
	if os.Getenv("STREAMTIME_API_KEY") == "" && os.Getenv("STREAMTIME_TOKEN") == "" {
		t.Fatal("STREAMTIME_API_KEY or STREAMTIME_TOKEN must be set to run the acceptance suite")
	}
}

// apiClient trusts the backend the way the provider does: STREAMTIME_VERIFY_SSL set to
// 0, false, no or off skips certificate verification (internal/config/config.go).
func apiClient() *http.Client {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("STREAMTIME_VERIFY_SSL"))) {
	case "0", "false", "no", "off":
		return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // explicit opt-in, as in the provider
	}
	return http.DefaultClient
}

// apiStatus asks the backend directly, outside the provider, so destroy is checked
// against the server's own view.
func apiStatus(path string) (int, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(os.Getenv("STREAMTIME_API_URL"), "/")+"/organizations/"+os.Getenv("STREAMTIME_ORG")+"/"+path, nil)
	if err != nil {
		return 0, err
	}
	if key := os.Getenv("STREAMTIME_API_KEY"); key != "" {
		req.Header.Set("X-Streamtime-Key", key)
		req.Header.Set("X-Streamtime-Org", os.Getenv("STREAMTIME_ORG"))
	} else {
		req.Header.Set("Authorization", "Bearer "+os.Getenv("STREAMTIME_TOKEN"))
	}
	resp, err := apiClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// destroyed checks that every object of the resource type is gone from the server.
func destroyed(resourceType, urlPrefix, idAttr string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		for _, rs := range s.RootModule().Resources {
			if rs.Type != resourceType {
				continue
			}
			code, err := apiStatus(urlPrefix + rs.Primary.Attributes[idAttr] + "/")
			if err != nil {
				return err
			}
			if code != http.StatusNotFound {
				return fmt.Errorf("%s %s still answers %d after destroy", resourceType, rs.Primary.ID, code)
			}
		}
		return nil
	}
}

// TestAcc_Secret: create, empty plan, update in place, import by id, destroy.
// The secret value is never returned by the server, so the import step ignores it.
func TestAcc_Secret(t *testing.T) {
	path := "tfacc/" + acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)
	cfg := func(content string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_secret" "t" {
  path    = %q
  content = %q
}
`, path, content)
	}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy:             destroyed("streamtime_secret", "secrets/", "id"),
		Steps: []resource.TestStep{
			{
				Config: cfg("first"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_secret.t", "path", path),
					resource.TestCheckResourceAttrSet("streamtime_secret.t", "id"),
					resource.TestCheckResourceAttrSet("streamtime_secret.t", "secret_provider"),
					resource.TestCheckResourceAttrSet("streamtime_secret.t", "uri"),
				),
			},
			{
				Config: cfg("second"),
				Check:  resource.TestCheckResourceAttr("streamtime_secret.t", "content", "second"),
			},
			{
				ResourceName:            "streamtime_secret.t",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"content"},
			},
		},
	})
}

// selfSigned returns a certificate and key in PEM form for the domain and its wildcard.
func selfSigned(t *testing.T, domain string) (certPEM, keyPEM string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain, "*." + domain},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}

// TestAcc_BaseDomain: create with the explicit "none" DNS provider and a supplied
// certificate, empty plan, in-place update with a renewed certificate, import by domain,
// destroy. It also proves the DNS config is accepted in the shape the backend's
// serializer demands.
func TestAcc_BaseDomain(t *testing.T) {
	domain := "tfacc-" + strings.ToLower(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)) + ".example.com"
	cfg := func(cert, key string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_base_domain" "t" {
  domain                   = %q
  auto_create_certificates = false
  tls_cert                 = %q
  tls_key                  = %q

  dns_provider_config = {
    dns_provider = {
      provider_type = "none"
    }
  }
}
`, domain, cert, key)
	}
	cert1, key1 := selfSigned(t, domain)
	cert2, key2 := selfSigned(t, domain)
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy:             destroyed("streamtime_base_domain", "base-domains/", "domain"),
		Steps: []resource.TestStep{
			{
				Config: cfg(cert1, key1),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_base_domain.t", "id", domain),
					resource.TestCheckResourceAttr("streamtime_base_domain.t", "auto_create_certificates", "false"),
					resource.TestCheckResourceAttr("streamtime_base_domain.t", "dns_provider_config.dns_provider.provider_type", "none"),
				),
			},
			{
				Config: cfg(cert2, key2),
				Check:  resource.TestCheckResourceAttr("streamtime_base_domain.t", "tls_cert", cert2),
			},
			{
				ResourceName:      "streamtime_base_domain.t",
				ImportState:       true,
				ImportStateVerify: true,
				// The key is write-only, and tls_cert is tracked only when configured (the server
				// also reports automatic certificates), so an import starts without either.
				ImportStateVerifyIgnore: []string{"tls_cert", "tls_key"},
			},
		},
	})
}

// TestAcc_CloudAccountAndNetwork: a cloud account and a bring-your-own network in it
// (network_id set, so nothing is created in a cloud). Create, empty plan, in-place
// update of each, import by identifier, destroy. Credentials are never read back,
// so the import step ignores them. Fleets are asynchronous and are verified by hand
// (#1801 human row 3).
func TestAcc_CloudAccountAndNetwork(t *testing.T) {
	id := "tfacc-" + strings.ToLower(acctest.RandStringFromCharSet(8, acctest.CharSetAlpha))
	cfg := func(secret, name string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_cloud_account" "t" {
  identifier     = %[1]q
  cloud_provider = "aws"
  configuration = jsonencode({
    aws_access_key    = "AKIATFACCEXAMPLE0000"
    aws_access_secret = %[2]q
  })
}

resource "streamtime_network" "t" {
  identifier    = %[1]q
  name          = %[3]q
  cloud_account = streamtime_cloud_account.t.identifier
  network_id    = "vpc-0123456789abcdef0"
}
`, id, secret, name)
	}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy: resource.ComposeAggregateTestCheckFunc(
			destroyed("streamtime_network", "networks/", "identifier"),
			destroyed("streamtime_cloud_account", "bootstrap-providers/", "identifier"),
		),
		Steps: []resource.TestStep{
			{
				Config: cfg("tfacc-secret-tfacc-secret-tfacc-secret-1", "first"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_cloud_account.t", "id", id),
					resource.TestCheckResourceAttr("streamtime_network.t", "network_id", "vpc-0123456789abcdef0"),
					resource.TestCheckResourceAttr("streamtime_network.t", "provisioned_by_streamtime", "false"),
				),
			},
			{
				Config: cfg("tfacc-secret-tfacc-secret-tfacc-secret-2", "second"),
				Check:  resource.TestCheckResourceAttr("streamtime_network.t", "name", "second"),
			},
			{
				ResourceName:            "streamtime_cloud_account.t",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"configuration"},
			},
			{
				ResourceName:      "streamtime_network.t",
				ImportState:       true,
				ImportStateVerify: true,
				// Whether Streamtime created the network is known only to the run that created it.
				ImportStateVerifyIgnore: []string{"provisioned_by_streamtime", "timeouts"},
			},
		},
	})
}

// TestAcc_KafkaTopic: create, empty plan, more partitions and a changed setting in
// place, import by <kafka>/<name>, destroy. Topics need a running cluster, so it
// runs only when STREAMTIME_ACC_KAFKA names one (a disposable cluster: the test
// creates and deletes a tfacc topic on it). The cluster itself is asynchronous and
// is verified by hand (plan #1801, human row 3).
func TestAcc_KafkaTopic(t *testing.T) {
	kafka := os.Getenv("STREAMTIME_ACC_KAFKA")
	if kafka == "" {
		t.Skip("set STREAMTIME_ACC_KAFKA to the identifier of a running Kafka cluster to run this test")
	}
	name := "tfacc-" + strings.ToLower(acctest.RandStringFromCharSet(8, acctest.CharSetAlpha))
	cfg := func(partitions int, retention string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_kafka_topic" "t" {
  kafka              = %[1]q
  name               = %[2]q
  partitions         = %[3]d
  replication_factor = 1
  config = {
    "retention.ms" = %[4]q
  }
}
`, kafka, name, partitions, retention)
	}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy:             destroyed("streamtime_kafka_topic", "kafka/"+kafka+"/topics/", "name"),
		Steps: []resource.TestStep{
			{
				Config: cfg(1, "3600000"),
				Check:  resource.TestCheckResourceAttr("streamtime_kafka_topic.t", "id", kafka+"/"+name),
			},
			{
				Config: cfg(2, "7200000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_kafka_topic.t", "partitions", "2"),
					resource.TestCheckResourceAttr("streamtime_kafka_topic.t", "config.retention.ms", "7200000"),
				),
			},
			{
				ResourceName:      "streamtime_kafka_topic.t",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

// TestAcc_KafkaSchema: register with a compatibility level, empty plan, a changed
// schema registers version 2 in place, import by <kafka>/<subject>, destroy with
// hard_delete. Needs a running cluster with a schema registry and a recent Agent,
// named by STREAMTIME_ACC_SCHEMA_KAFKA (disposable: the test registers and
// permanently removes a tfacc subject on it).
func TestAcc_KafkaSchema(t *testing.T) {
	kafka := os.Getenv("STREAMTIME_ACC_SCHEMA_KAFKA")
	if kafka == "" {
		t.Skip("set STREAMTIME_ACC_SCHEMA_KAFKA to the identifier of a running Kafka cluster with a schema registry to run this test")
	}
	subject := "tfacc-" + strings.ToLower(acctest.RandStringFromCharSet(8, acctest.CharSetAlpha)) + "-value"
	cfg := func(fields string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_kafka_schema" "t" {
  kafka         = %[1]q
  subject       = %[2]q
  schema_type   = "avro"
  compatibility = "BACKWARD"
  hard_delete   = true
  schema = jsonencode({
    type   = "record"
    name   = "Payment"
    fields = [%[3]s]
  })
}
`, kafka, subject, fields)
	}
	id := `{ name = "id", type = "string" }`
	amount := `{ name = "amount", type = "double", default = 0 }`
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy: func(*terraform.State) error {
			code, err := apiStatus("kafka/" + kafka + "/schemas/subjects/" + subject + "/versions/?deleted=true")
			if err == nil && code != http.StatusNotFound {
				err = fmt.Errorf("subject %s still answers %d after a hard delete", subject, code)
			}
			return err
		},
		Steps: []resource.TestStep{
			{
				Config: cfg(id),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_kafka_schema.t", "id", kafka+"/"+subject),
					resource.TestCheckResourceAttr("streamtime_kafka_schema.t", "version", "1"),
				),
			},
			{
				Config: cfg(id + ", " + amount),
				Check:  resource.TestCheckResourceAttr("streamtime_kafka_schema.t", "version", "2"),
			},
			{
				ResourceName:      "streamtime_kafka_schema.t",
				ImportState:       true,
				ImportStateVerify: true,
				// The registry stores its own formatting of the schema, and hard_delete
				// is a choice about destroy that only the configuration holds.
				ImportStateVerifyIgnore: []string{"schema", "hard_delete"},
			},
		},
	})
}

// TestAcc_FlinkProject: create with a token-authenticated store, empty plan, an
// in-place change of the name and the store's address, import by identifier,
// destroy. The CI key and the store token are never returned again, so the import
// cannot verify them.
func TestAcc_FlinkProject(t *testing.T) {
	id := "tfacc-" + strings.ToLower(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	cfg := func(name, storeURL string) string {
		return fmt.Sprintf(`
provider "streamtime" {}

resource "streamtime_flink_project" "t" {
  identifier = %q
  name       = %q
  runtime    = "java"

  store = {
    store_type     = "oras"
    url            = %q
    authentication = "token"
    token          = "tfacc-read-only"
  }
}
`, id, name, storeURL)
	}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: protoV6Factories,
		CheckDestroy:             destroyed("streamtime_flink_project", "flink-projects/", "id"),
		Steps: []resource.TestStep{
			{
				Config: cfg("first", "https://ghcr.io"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_flink_project.t", "id", id),
					resource.TestCheckResourceAttr("streamtime_flink_project.t", "store.has_credential", "true"),
				),
			},
			{
				Config: cfg("second", "https://registry.example.com"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("streamtime_flink_project.t", "name", "second"),
					resource.TestCheckResourceAttr("streamtime_flink_project.t", "store.url", "https://registry.example.com"),
				),
			},
			{
				ResourceName:            "streamtime_flink_project.t",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"store.token"},
			},
		},
	})
}
