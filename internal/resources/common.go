// Package resources holds the Terraform resources. Each talks to Streamtime
// only through the org-scoped client.
package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"

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

// pollBounds is how often an asynchronous resource is polled: from initial,
// backing off to max, until the Terraform timeout.
type pollBounds struct{ initial, max time.Duration }

// progress is what an asynchronous resource reports about its last change.
type progress struct {
	state, status, lifecycle string
	observedGeneration       int64
}

// reconciling is a server object that reports progress: a fleet or a workload.
type reconciling interface{ progress() progress }

// waitGeneration polls path until Streamtime has deployed the given generation,
// leaving the last read in out. A failed deployment is an error naming the object.
func waitGeneration[T reconciling](ctx context.Context, c *client.Client, path, noun string, generation int64, poll pollBounds, out *T) error {
	return client.WaitForStatus(ctx, poll.initial, poll.max, func(ctx context.Context) (bool, error) {
		if err := c.Do(ctx, "GET", path, nil, out); err != nil {
			return false, err
		}
		p := (*out).progress()
		switch p.state {
		case "failed":
			return false, fmt.Errorf("Streamtime could not deploy the %s (status %s). See its operations in Streamtime for the cause, fix it, then apply again", noun, p.status)
		case "succeeded":
			return p.observedGeneration >= generation, nil
		}
		return false, nil
	})
}

// reconcileAndWait asks Streamtime to deploy the saved generation of the object at
// path (op is create or update, for the idempotency key), then waits until it has.
func reconcileAndWait[T reconciling](ctx context.Context, c *client.Client, path, noun, op, identifier string, generation int64, poll pollBounds, out *T) error {
	headers := map[string]string{"Idempotency-Key": idempotencyKey(op, identifier, generation)}
	if err := c.DoWithHeaders(ctx, "POST", path+"reconcile/", headers, map[string]any{"generation": generation}, nil); err != nil {
		return fmt.Errorf("Streamtime could not start deploying the %s: %w", noun, err)
	}
	return waitGeneration(ctx, c, path, noun, generation, poll, out)
}

// deploySaved makes sure the saved generation of the object at path is running: it
// waits for a deployment already under way, and otherwise starts one. A change that
// failed to deploy, or was saved but never deployed, is deployed again this way.
func deploySaved[T reconciling](ctx context.Context, c *client.Client, path, noun, identifier string, generation int64, poll pollBounds, out *T) error {
	p := (*out).progress()
	switch {
	case p.state == "succeeded" && p.observedGeneration >= generation:
		return nil
	case p.state == "queued" || p.state == "running":
		return waitGeneration(ctx, c, path, noun, generation, poll, out)
	}
	return reconcileAndWait(ctx, c, path, noun, "update", identifier, generation, poll, out)
}

// planRedeploy is ModifyPlan for an object whose saved settings Streamtime deploys.
// When its last change was saved but is not running (it failed, timed out, or never
// started), the configuration still matches the state, so Terraform would plan
// nothing. It plans an update instead, marking what the deployment changes as
// unknown, so the next apply deploys the saved settings.
func planRedeploy(ctx context.Context, req resource.ModifyPlanRequest, resp *resource.ModifyPlanResponse, noun string, deployed []string) {
	if req.State.Raw.IsNull() || req.Plan.Raw.IsNull() {
		return
	}
	var id, state, status types.String
	var generation, observed types.Int64
	for name, dst := range map[string]any{"id": &id, "reconciliation_state": &state, "status": &status,
		"generation": &generation, "observed_generation": &observed} {
		resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root(name), dst)...)
	}
	if resp.Diagnostics.HasError() || generation.IsNull() || observed.IsNull() {
		return
	}
	if observed.ValueInt64() >= generation.ValueInt64() || state.ValueString() == "queued" || state.ValueString() == "running" {
		return
	}
	for _, name := range deployed {
		resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root(name), types.StringUnknown())...)
	}
	for _, name := range []string{"generation", "observed_generation"} {
		resp.Diagnostics.Append(resp.Plan.SetAttribute(ctx, path.Root(name), types.Int64Unknown())...)
	}
	resp.Diagnostics.AddWarning("The last change to the "+noun+" is not running",
		"The settings of "+noun+" "+id.ValueString()+" are saved, but Streamtime has not deployed them (status "+
			status.ValueString()+"). Apply deploys them again.")
}

// waitGone polls path after a delete until the object is gone (404). A teardown
// that reports failure is an error naming the object.
func waitGone[T reconciling](ctx context.Context, c *client.Client, path, noun string, poll pollBounds) error {
	return client.WaitForStatus(ctx, poll.initial, poll.max, func(ctx context.Context) (bool, error) {
		var out T
		if err := c.Do(ctx, "GET", path, nil, &out); err != nil {
			if isGone(err) {
				return true, nil
			}
			return false, err
		}
		if p := out.progress(); p.state == "failed" && p.lifecycle == "absent" {
			return false, fmt.Errorf("Streamtime could not remove the %s (status %s). See its operations in Streamtime, fix the cause, then run destroy again", noun, p.status)
		}
		return false, nil
	})
}
