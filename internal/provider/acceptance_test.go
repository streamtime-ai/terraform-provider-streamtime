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
