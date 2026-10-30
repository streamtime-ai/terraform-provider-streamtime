// Package version implements the directional provider/server version rule.
package version

import (
	"fmt"
	"strings"

	goversion "github.com/hashicorp/go-version"
)

// Outcome is the result of comparing the provider with the server.
type Outcome int

const (
	// Equal: stay silent.
	Equal Outcome = iota
	// ProviderOlder: warn the user to upgrade the provider.
	ProviderOlder
	// ProviderNewer: error, the provider may call endpoints the server lacks.
	ProviderNewer
	// Uncomparable: either side is not a SemVer release; warn, never error.
	Uncomparable
)

// Result carries the outcome and a plain-language message.
type Result struct {
	Outcome Outcome
	Message string
}

// parseRelease returns a version only for a plain SemVer release. Prereleases
// (including 0.0.0-nightly.*), "latest", "dev" and custom tags are not releases.
func parseRelease(s string) (*goversion.Version, bool) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	v, err := goversion.NewSemver(s)
	if err != nil || v.Prerelease() != "" {
		return nil, false
	}
	return v, true
}

// Check applies the rule: provider newer than server is an error, older is a
// warning, equal is silent, and anything that is not a SemVer release warns.
func Check(provider, server string) Result {
	p, pok := parseRelease(provider)
	s, sok := parseRelease(server)
	if !pok || !sok {
		return Result{Uncomparable, fmt.Sprintf(
			"Could not compare the provider version (%s) with the server version (%s) because one of them is not a release version. "+
				"Pin the server to a release version to enable this check.", provider, server)}
	}
	switch p.Compare(s) {
	case 0:
		return Result{Outcome: Equal}
	case 1:
		return Result{ProviderNewer, fmt.Sprintf(
			"This provider (%s) is newer than the Streamtime server (%s) and may use features the server does not have. "+
				"Use provider version %s, or upgrade the server.", p, s, s)}
	default:
		return Result{ProviderOlder, fmt.Sprintf(
			"This provider (%s) is older than the Streamtime server (%s). Upgrade the provider to get the latest resources and fixes.", p, s)}
	}
}
