package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// Organization and catalog resources (slice 6 of #1801). They are built on
// objectSpec in objects.go.

// NewMember returns the streamtime_member resource.
var NewMember = newObjectResource(objectSpec{
	typeName:   "streamtime_member",
	noun:       "member",
	desc:       "A person who belongs to the organization, with the role they hold. Create invites the person; their identity must also exist at the organization's sign-in provider. For built-in sign-in, supply an initial password for a new person.",
	collection: "members/",
	createPath: "members/invite/",
	key:        "email",
	update:     "PUT",
	fields: []field{
		{name: "email", kind: kString, required: true, replace: true,
			desc: "The member's email address. It is also the ID you use to import them. Changing it adds a different member."},
		{name: "role", kind: kString, optional: true, computed: true,
			desc: "The role the member holds, for example Administrator. Leave it out to leave the member's role unset."},
		{name: "initial_password", api: "password", kind: kString, optional: true, sensitive: true, hidden: true, replace: true, createOnly: true,
			desc: "Initial password for a new person using built-in sign-in. It is sent only when inviting the person, never changes an existing password, and is retained in sensitive Terraform state."},
	},
})

// NewAPIKey returns the streamtime_api_key resource.
var NewAPIKey = newObjectResource(objectSpec{
	typeName:   "streamtime_api_key",
	noun:       "API key",
	desc:       "An API key for scripts and automation. Streamtime shows the key value only when it creates the key, so it is kept in the Terraform state, marked sensitive.",
	collection: "api-keys/",
	key:        "id",
	update:     "PUT",
	fields: []field{
		{name: "id", kind: kString, desc: "ID of the API key. Use it to import an existing key."},
		{name: "name", kind: kString, required: true, desc: "A name that tells you what the key is for. Up to 50 characters."},
		{name: "role", kind: kString, required: true,
			desc: "The role the key acts with, for example Agent or Administrator. A name and role pair can be used only once."},
		{name: "key", kind: kString, once: true, sensitive: true,
			desc: "The key value. Streamtime returns it only when the key is created, so it is empty for a key you imported. Create a new key if you need the value of an imported one."},
	},
})

func trailingSlashSame(configured, server string) bool { return sameURL(configured, server) }

// NewOAuthProvider returns the streamtime_oauth_provider resource.
var NewOAuthProvider = newObjectResource(objectSpec{
	typeName:   "streamtime_oauth_provider",
	noun:       "sign-in provider",
	desc:       "The identity provider people sign in with. Supply the issuer, signing-key, token and sign-in addresses from its discovery document, and the client you registered there. Streamtime validates those addresses against discovery.",
	collection: "oauth-providers/",
	key:        "identifier",
	update:     "PUT",
	fields: []field{
		{name: "identifier", kind: kString, required: true, replace: true,
			desc: "A short name for the provider, up to 24 lowercase letters, numbers and dashes. Changing it creates a new provider."},
		{name: "issuer", kind: kString, required: true, same: trailingSlashSame,
			desc: "The issuer address of your identity provider, for example https://accounts.example.com."},
		{name: "client_id", kind: kString, required: true, desc: "The client ID you registered for Streamtime at your identity provider."},
		{name: "client_secret", kind: kString, required: true, sensitive: true, hidden: true,
			desc: "The client secret you registered for Streamtime at your identity provider."},
		{name: "jwks_endpoint_uri", kind: kString, required: true, same: trailingSlashSame,
			desc: "Where the provider publishes its signing keys: jwks_uri in its discovery document."},
		{name: "token_endpoint_uri", kind: kString, required: true, same: trailingSlashSame,
			desc: "The provider's token address: token_endpoint in its discovery document."},
		{name: "authorize_endpoint_uri", kind: kString, required: true, same: trailingSlashSame,
			desc: "The provider's sign-in page address: authorization_endpoint in its discovery document."},
		{name: "userinfo_endpoint_uri", kind: kString, optional: true, computed: true, same: trailingSlashSame,
			desc: "The provider's user information address, when it has one."},
		{name: "audience", kind: kString, optional: true, clearOnRemove: true, desc: "The audience the provider puts in its tokens, when it uses one."},
		{name: "scope", kind: kString, optional: true, computed: true, desc: "The scopes Streamtime asks for when people sign in."},
		{name: "sub_claim_name", kind: kString, optional: true, computed: true, desc: "The token claim that holds the person's unique ID."},
		{name: "email_claim_name", kind: kString, optional: true, computed: true, desc: "The token claim that holds the person's email address."},
		{name: "groups_claim_name", kind: kString, optional: true, computed: true, desc: "The token claim that holds the person's groups."},
		{name: "groups_claim_scope", kind: kString, optional: true, clearOnRemove: true, desc: "The scope that makes the provider include the groups claim."},
		{name: "tls_trusted_cert", kind: kString, optional: true, clearOnRemove: true, same: sameText,
			desc: "The certificate authority bundle to trust when the provider uses a private certificate, in PEM format."},
		{name: "refresh_token", kind: kBool, optional: true, computed: true,
			desc: "Whether people stay signed in by refreshing their session."},
	},
})

// NewOAuthClient returns the streamtime_oauth_client resource.
var NewOAuthClient = newObjectResource(objectSpec{
	typeName:   "streamtime_oauth_client",
	noun:       "OAuth client",
	desc:       "A client that applications use to connect to Kafka clusters with a client ID and secret. Streamtime shows the secret only when it creates the client, so it is kept in the Terraform state, marked sensitive. It needs the organization to use Streamtime's own sign-in.",
	collection: "oauth-clients/",
	key:        "identifier",
	fields: []field{
		{name: "identifier", kind: kString, required: true,
			desc: "A short name for the client, up to 24 lowercase letters, numbers and dashes. Changing it creates a new client."},
		{name: "name", kind: kString, required: true, desc: "A name that tells people what the client is for. Changing it creates a new client."},
		{name: "kafka_clusters", kind: kStrings, optional: true, computed: true, unordered: true,
			desc: "The identifiers of the Kafka clusters the client may use. Changing them creates a new client, because Streamtime cannot change a client's access in place."},
		{name: "client_id", kind: kString, desc: "The client ID applications sign in with."},
		{name: "client_secret", kind: kString, once: true, sensitive: true,
			desc: "The client secret. Streamtime returns it only when the client is created, so it is empty for a client you imported."},
		{name: "secret_last4", kind: kString, desc: "The last four characters of the secret, to tell secrets apart."},
	},
})

// NewAlertChannel returns the streamtime_alert_channel resource.
var NewAlertChannel = newObjectResource(objectSpec{
	typeName:   "streamtime_alert_channel",
	noun:       "alert channel",
	desc:       "A place alerts are sent to: Slack, PagerDuty or a webhook.",
	collection: "alert-channels/",
	key:        "identifier",
	update:     "PUT",
	fields: []field{
		{name: "identifier", kind: kString, required: true, replace: true,
			desc: "A short name for the channel, up to 24 lowercase letters, numbers and dashes. Changing it creates a new channel."},
		{name: "type", kind: kString, required: true,
			desc: "Where alerts go: slack, pagerduty or webhook."},
		{name: "receiver_config", kind: kJSON, required: true, sensitive: true,
			desc: "How to reach the destination, set with jsonencode({...}). Slack needs channel and api_url; webhooks need url; PagerDuty needs routing_key. The receiver_type is filled in from type when you leave it out."},
	},
	prepare: func(_ context.Context, _ *client.Client, body map[string]any) error {
		if cfg, ok := body["receiver_config"].(map[string]any); ok {
			if _, has := cfg["receiver_type"]; !has {
				if t, ok := body["type"].(string); ok {
					cfg["receiver_type"] = t
				}
			}
		}
		return nil
	},
})

// NewObservabilityProvider returns the streamtime_observability_provider resource.
var NewObservabilityProvider = newObjectResource(objectSpec{
	typeName:   "streamtime_observability_provider",
	noun:       "observability provider",
	desc:       "A monitoring service Streamtime sends metrics and logs to: Datadog, Grafana Cloud or New Relic.",
	collection: "observability-providers/",
	key:        "identifier",
	update:     "PUT",
	fields: []field{
		{name: "identifier", kind: kString, required: true, replace: true,
			desc: "A short name for the provider, up to 24 lowercase letters, numbers and dashes. Changing it creates a new provider."},
		{name: "vendor", kind: kString, required: true, replace: true,
			desc: "The monitoring service: datadog, grafana_cloud or newrelic. Changing it creates a new provider."},
		{name: "configuration", kind: kJSON, required: true, sensitive: true, masked: true,
			desc: "The service's settings, including its keys, set with jsonencode({...}). The settings depend on the vendor."},
	},
})

// NewHelmRepository returns the streamtime_helm_repository resource.
var NewHelmRepository = newObjectResource(objectSpec{
	typeName:   "streamtime_helm_repository",
	noun:       "chart repository",
	desc:       "A Helm chart repository that packages can come from. Managing the catalog needs the catalog permission, normally held by the root organization.",
	collection: "helm-repositories/",
	key:        "name",
	update:     "PUT",
	fields: []field{
		{name: "name", kind: kString, required: true, replace: true,
			desc: "A name for the repository. It is also the ID you use to import it. Changing it creates a new repository."},
		{name: "base_url", kind: kString, required: true, same: trailingSlashSame, desc: "The repository address, for example https://charts.example.com."},
	},
})

// NewTrackedRepository returns the streamtime_tracked_repository resource.
var NewTrackedRepository = newObjectResource(objectSpec{
	typeName:   "streamtime_tracked_repository",
	noun:       "tracked repository",
	desc:       "A repository whose packages Streamtime lists in the catalog. Managing the catalog needs the catalog permission, normally held by the root organization. Streamtime finds a tracked repository by its repository name, so import it by that name; Terraform refuses to read, change or delete one when another registry holds a repository of the same name.",
	collection: "tracked-repositories/",
	key:        "repository",
	update:     "PATCH",
	fields: []field{
		{name: "repository", kind: kString, required: true, replace: true,
			desc: "The repository name, for example streamtime/streamtime-agent. It is also the ID you use to import it. Changing it creates a new tracked repository."},
		{name: "repo_type", kind: kString, required: true, replace: true,
			desc: "oci for a container registry or helm for a chart repository. Changing it creates a new tracked repository."},
		{name: "registry_url", kind: kString, optional: true, replace: true,
			desc: "The container registry address. Required for oci, and not allowed for helm. Changing it creates a new tracked repository."},
		{name: "helm_repository", kind: kString, optional: true, replace: true,
			desc: "The name of the streamtime_helm_repository the charts come from. Required for helm, and not allowed for oci. Changing it creates a new tracked repository."},
		{name: "template_values", kind: kJSON, optional: true, computed: true,
			desc: "Default chart values for the packages in this repository, set with jsonencode({...})."},
	},
	prepare: func(ctx context.Context, c *client.Client, body map[string]any) error {
		name, _ := body["helm_repository"].(string)
		delete(body, "helm_repository")
		if name == "" {
			return nil
		}
		var repo struct {
			ID string `json:"id"`
		}
		if err := c.Do(ctx, "GET", "helm-repositories/"+url.PathEscape(name)+"/", nil, &repo); err != nil {
			return fmt.Errorf("could not find the chart repository %q. Create it first: %w", name, err)
		}
		body["helm_repository_id"] = repo.ID
		return nil
	},
	reshape: func(obj map[string]json.RawMessage) {
		var nested struct {
			Name string `json:"name"`
		}
		if raw, ok := obj["helm_repository"]; ok && json.Unmarshal(raw, &nested) == nil && nested.Name != "" {
			obj["helm_repository"], _ = json.Marshal(nested.Name)
			return
		}
		obj["helm_repository"] = json.RawMessage("null")
	},
	verify: func(ctx context.Context, state tfsdk.State, obj map[string]json.RawMessage) error {
		// The URL names only the repository. When two registries hold one of that
		// name, it can reach the other one, and must not read or delete it.
		text := func(raw json.RawMessage) string {
			var v *string
			_ = json.Unmarshal(raw, &v)
			if v == nil {
				return ""
			}
			return *v
		}
		var nested struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(obj["helm_repository"], &nested)
		differs := func(attr, server string) bool {
			var prior types.String
			if state.GetAttribute(ctx, path.Root(attr), &prior).HasError() || prior.IsNull() || prior.IsUnknown() {
				return false // nothing to compare, as after an import
			}
			return prior.ValueString() != server
		}
		if differs("repo_type", text(obj["repo_type"])) || differs("registry_url", text(obj["registry_url"])) ||
			differs("helm_repository", nested.Name) {
			return fmt.Errorf("another registry holds a repository with the same name, so Streamtime cannot tell them apart. Change it in Streamtime directly")
		}
		return nil
	},
})

// NewPackage returns the streamtime_package resource.
var NewPackage = newObjectResource(objectSpec{
	typeName:   "streamtime_package",
	noun:       "package",
	desc:       "A chart or image version in the package catalog, with how Streamtime treats it. Managing the catalog needs the catalog permission, normally held by the root organization.",
	collection: "packages/",
	key:        "identifier",
	update:     "PATCH",
	fields: []field{
		{name: "identifier", kind: kString, desc: "The package ID Streamtime assigns. Use it to import an existing package."},
		{name: "registry_url", kind: kString, required: true, replace: true, desc: "The registry or chart repository address. Changing it creates a new package."},
		{name: "repository", kind: kString, required: true, replace: true, desc: "The repository name. Changing it creates a new package."},
		{name: "tag", kind: kString, required: true, replace: true, desc: "The version tag. Changing it creates a new package."},
		{name: "artifact_type", kind: kString, optional: true, computed: true,
			desc: "What the package is: container_image, helm_chart or oci_artifact."},
		{name: "support_status", kind: kString, optional: true, computed: true,
			desc: "How Streamtime treats the version: active, supported, deprecated, latest or preview."},
		{name: "preferred_registry", kind: kString, optional: true, computed: true, desc: "The registry to pull the package from when more than one holds it."},
		{name: "support_notes", kind: kString, optional: true, clearOnRemove: true, desc: "Notes for people choosing a version."},
		{name: "template_values", kind: kJSON, optional: true, computed: true, desc: "Chart values applied for this package, set with jsonencode({...})."},
		{name: "default_values", kind: kJSON, optional: true, computed: true, desc: "The package's default chart values, set with jsonencode({...})."},
		{name: "values_schema", kind: kJSON, optional: true, computed: true, desc: "A JSON schema for the chart values, set with jsonencode({...})."},
	},
})

// NewPackageRule returns the streamtime_package_rule resource.
var NewPackageRule = newObjectResource(objectSpec{
	typeName:   "streamtime_package_rule",
	noun:       "package rule",
	desc:       "A rule that decides which versions of a repository are supported and which is the default. Managing the catalog needs the catalog permission, normally held by the root organization.",
	collection: "package-rules/",
	key:        "name",
	update:     "PATCH",
	fields: []field{
		{name: "name", kind: kString, required: true, replace: true,
			desc: "A name for the rule. It is also the ID you use to import it. Changing it creates a new rule."},
		{name: "repository", kind: kString, required: true, desc: "The repository the rule applies to."},
		{name: "description", kind: kString, optional: true, computed: true, desc: "What the rule is for."},
		{name: "registry_url", kind: kString, optional: true, computed: true, desc: "Limits the rule to one registry."},
		{name: "version_constraint", kind: kString, optional: true, computed: true, desc: "Versions the rule matches, for example >=3.6.0 <4.0.0."},
		{name: "version_list", kind: kStrings, optional: true, computed: true, desc: "Exact versions the rule matches."},
		{name: "max_age_days", kind: kInt, optional: true, computed: true, desc: "Only versions published in the last this many days match."},
		{name: "target_status", kind: kString, optional: true, computed: true,
			desc: "The status matching versions get: active, supported, deprecated, latest or preview."},
		{name: "unmatch_status", kind: kString, optional: true, computed: true,
			desc: "The status versions that do not match get: active, supported, deprecated, latest or preview."},
		{name: "preferred_registry", kind: kString, optional: true, computed: true, desc: "The registry to pull matching versions from."},
		{name: "allowed_registries", kind: kStrings, optional: true, computed: true, desc: "The registries matching versions may come from."},
		{name: "default_version", kind: kString, optional: true, computed: true, desc: "The version offered by default."},
		{name: "priority", kind: kInt, optional: true, computed: true, desc: "Which rule wins when more than one matches. A higher number wins."},
		{name: "enabled", kind: kBool, optional: true, computed: true, desc: "Whether the rule is applied."},
	},
})

// NewSMTPConfig returns the streamtime_smtp_config resource.
var NewSMTPConfig = newObjectResource(objectSpec{
	typeName:   "streamtime_smtp_config",
	noun:       "email",
	desc:       "The mail server Streamtime sends status, alert and report emails through. An organization has one set of these settings. Destroying the resource stops Terraform managing them and leaves the current values in place.",
	collection: "smtp-config/",
	key:        "id",
	singleton:  "smtp-config",
	update:     "PUT",
	fields: []field{
		{name: "id", kind: kString, desc: "Always smtp-config. Use it to import the settings."},
		{name: "enabled", kind: kBool, optional: true, computed: true, desc: "Whether Streamtime sends email."},
		{name: "host", kind: kString, optional: true, computed: true, desc: "The mail server address."},
		{name: "port", kind: kInt, optional: true, computed: true, desc: "The mail server port."},
		{name: "encryption", kind: kString, optional: true, computed: true, desc: "How the connection is protected: starttls, ssl or none."},
		{name: "username", kind: kString, optional: true, computed: true, desc: "The account Streamtime signs in to the mail server with."},
		{name: "password", kind: kString, optional: true, sensitive: true, hidden: true,
			desc: "The mail server password. Streamtime never shows it, so Terraform cannot tell if it was changed elsewhere."},
		{name: "from_email", kind: kString, optional: true, computed: true, desc: "The address emails come from."},
		{name: "from_name", kind: kString, optional: true, computed: true, desc: "The name emails come from."},
		{name: "link_base_url", kind: kString, optional: true, computed: true, same: trailingSlashSame,
			desc: "The Streamtime address that links in emails point to."},
		{name: "timeout_seconds", kind: kInt, optional: true, computed: true, desc: "How long to wait for the mail server."},
		{name: "status_emails_enabled", kind: kBool, optional: true, computed: true, desc: "Whether status emails are sent."},
		{name: "alert_emails_enabled", kind: kBool, optional: true, computed: true, desc: "Whether alert emails are sent."},
		{name: "weekly_report_enabled", kind: kBool, optional: true, computed: true, desc: "Whether the weekly report is sent."},
	},
})
