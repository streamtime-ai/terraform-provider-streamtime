package version

import "testing"

func TestVersionCheck(t *testing.T) {
	cases := []struct {
		name, provider, server string
		want                   Outcome
	}{
		{"equal", "1.4.0", "1.4.0", Equal},
		{"v prefix equal", "v1.4.0", "1.4.0", Equal},
		{"provider newer", "1.5.0", "1.4.0", ProviderNewer},
		{"provider older", "1.3.0", "1.4.0", ProviderOlder},
		{"server latest", "1.4.0", "latest", Uncomparable},
		{"server dev", "1.4.0", "dev", Uncomparable},
		{"server nightly", "1.4.0", "0.0.0-nightly.20261027.40531336156", Uncomparable},
		{"provider nightly", "0.0.0-nightly.20261027.1", "1.4.0", Uncomparable},
		{"provider dev build", "dev", "1.4.0", Uncomparable},
		{"server prerelease", "1.4.0", "1.5.0-rc.1", Uncomparable},
		{"custom tag", "1.4.0", "my-build-7", Uncomparable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Check(c.provider, c.server); got.Outcome != c.want {
				t.Fatalf("Check(%q,%q)=%v want %v (%s)", c.provider, c.server, got.Outcome, c.want, got.Message)
			}
		})
	}
}

func TestVersionCheck_NewerNamesMatchingVersion(t *testing.T) {
	r := Check("1.5.0", "1.4.0")
	if r.Outcome != ProviderNewer || !contains(r.Message, "1.4.0") {
		t.Fatalf("message must name the matching provider version: %q", r.Message)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
