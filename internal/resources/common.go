// Package resources holds the Terraform resources. Each talks to Streamtime
// only through the org-scoped client.
package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"

	"github.com/streamtime-ai/terraform-provider-streamtime/internal/client"
)

// configure stores the provider's client on a resource.
func configure(req resource.ConfigureRequest, d *diag.Diagnostics) *client.Client {
	if req.ProviderData == nil {
		return nil
	}
	c, ok := req.ProviderData.(*client.Client)
	if !ok {
		d.AddError("Unexpected provider data", fmt.Sprintf("Expected the Streamtime client, got %T.", req.ProviderData))
		return nil
	}
	return c
}

// isGone reports whether err means the object no longer exists.
func isGone(err error) bool { return errors.Is(err, client.ErrNotFound) }

// deleteIgnoringGone deletes a path; an object that is already gone is success.
func deleteIgnoringGone(ctx context.Context, c *client.Client, path string) error {
	if err := c.Do(ctx, "DELETE", path, nil, nil); err != nil && !isGone(err) {
		return err
	}
	return nil
}

// withoutNulls encodes v as JSON and leaves out every object key whose value is null.
// The generated API types send null for an unset nullable field, and the API reads
// null as "clear it": a PUT would erase an automatic certificate or a vault secret's
// metadata. Unset fields are left out instead, so the server keeps what it has.
func withoutNulls(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var x any
	if err := dec.Decode(&x); err != nil {
		return nil, err
	}
	return json.Marshal(dropNulls(x))
}

func dropNulls(x any) any {
	switch t := x.(type) {
	case map[string]any:
		for k, v := range t {
			if v == nil {
				delete(t, k)
			} else {
				t[k] = dropNulls(v)
			}
		}
	case []any:
		for i := range t {
			t[i] = dropNulls(t[i])
		}
	}
	return x
}
