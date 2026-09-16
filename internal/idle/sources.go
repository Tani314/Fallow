package idle

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
)

// AnnotationLastActivity is where an external traffic source records the last
// time this workload served a request. A service mesh sidecar, an ingress
// controller, or a nightly Prometheus query can all write it.
const AnnotationLastActivity = "fallow.dev/last-activity"

// LastActivity reports idle when the last-activity annotation is older than
// Window.
//
// Production mapping: in a real deployment this annotation is maintained by
// whatever already knows about traffic -- an Envoy access-log pipeline, an
// ingress metrics scraper, a Prometheus recording rule. Keeping the signal
// on the object rather than querying a metrics backend inline means the
// controller stays fast and has no hard dependency on a metrics stack.
type LastActivity struct {
	// Window is how long without activity counts as idle.
	Window time.Duration
}

// Name identifies this source.
func (LastActivity) Name() string { return "last-activity" }

// Evaluate reads the annotation and compares it against Window.
func (l LastActivity) Evaluate(_ context.Context, d *appsv1.Deployment, now time.Time) (Signal, error) {
	raw, ok := d.Annotations[AnnotationLastActivity]
	if !ok {
		// Unknown is not idle. Without instrumentation Fallow refuses to
		// guess, which means onboarding a namespace cannot accidentally
		// reclaim workloads nobody has wired traffic signals for yet.
		return Signal{
			Name:   l.Name(),
			Idle:   false,
			Detail: "no " + AnnotationLastActivity + " annotation; activity unknown",
		}, nil
	}

	last, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return Signal{}, fmt.Errorf("parsing %s=%q: %w", AnnotationLastActivity, raw, err)
	}

	quiet := now.Sub(last)
	if quiet >= l.Window {
		return Signal{
			Name:   l.Name(),
			Idle:   true,
			Detail: fmt.Sprintf("no activity for %s (window %s)", round(quiet), l.Window),
		}, nil
	}
	return Signal{
		Name:   l.Name(),
		Idle:   false,
		Detail: fmt.Sprintf("active %s ago (window %s)", round(quiet), l.Window),
	}, nil
}

// MetricsProvider supplies resource usage for a workload. Implemented in
// production by a metrics.k8s.io client; implemented in tests by a stub.
type MetricsProvider interface {
	// CPUMilliCores returns summed CPU usage across the workload's pods.
	CPUMilliCores(ctx context.Context, namespace, name string) (int64, error)
}

// CPUBelow reports idle when a workload's CPU sits under Threshold.
//
// Production mapping: back this with metrics-server via the metrics.k8s.io
// API, or with a Prometheus range query if you need a percentile over a
// window rather than an instantaneous read.
type CPUBelow struct {
	// Threshold in millicores. Usage at or above this counts as busy.
	Threshold int64
	// Provider supplies the reading.
	Provider MetricsProvider
}

// Name identifies this source.
func (CPUBelow) Name() string { return "cpu" }

// Evaluate reads CPU usage and compares it against Threshold.
func (c CPUBelow) Evaluate(ctx context.Context, d *appsv1.Deployment, _ time.Time) (Signal, error) {
	if c.Provider == nil {
		return Signal{}, fmt.Errorf("no metrics provider configured")
	}
	milli, err := c.Provider.CPUMilliCores(ctx, d.Namespace, d.Name)
	if err != nil {
		return Signal{}, fmt.Errorf("reading cpu for %s/%s: %w", d.Namespace, d.Name, err)
	}
	if milli < c.Threshold {
		return Signal{
			Name:   c.Name(),
			Idle:   true,
			Detail: fmt.Sprintf("%dm < %dm threshold", milli, c.Threshold),
		}, nil
	}
	return Signal{
		Name:   c.Name(),
		Idle:   false,
		Detail: fmt.Sprintf("%dm >= %dm threshold", milli, c.Threshold),
	}, nil
}

// RolloutQuiet reports idle when the workload has not been deployed to
// recently.
//
// A Deployment someone shipped an hour ago is under active development even
// if it is serving no traffic yet, so this signal keeps Fallow from
// reclaiming work in progress.
type RolloutQuiet struct {
	// Window is how long since the last rollout counts as quiet.
	Window time.Duration
}

// Name identifies this source.
func (RolloutQuiet) Name() string { return "rollout-quiet" }

// Evaluate finds the most recent rollout and compares it against Window.
func (r RolloutQuiet) Evaluate(_ context.Context, d *appsv1.Deployment, now time.Time) (Signal, error) {
	last := d.CreationTimestamp.Time
	for _, c := range d.Status.Conditions {
		if c.LastUpdateTime.Time.After(last) {
			last = c.LastUpdateTime.Time
		}
	}
	if last.IsZero() {
		return Signal{
			Name:   r.Name(),
			Idle:   false,
			Detail: "no rollout history; treating as active",
		}, nil
	}

	since := now.Sub(last)
	if since >= r.Window {
		return Signal{
			Name:   r.Name(),
			Idle:   true,
			Detail: fmt.Sprintf("last rollout %s ago (window %s)", round(since), r.Window),
		}, nil
	}
	return Signal{
		Name:   r.Name(),
		Idle:   false,
		Detail: fmt.Sprintf("rolled out %s ago (window %s)", round(since), r.Window),
	}, nil
}

// StaticSource returns a fixed verdict. It exists so the demo and unit tests
// can drive the escalation ladder deterministically, without a metrics stack
// or wall-clock waits.
type StaticSource struct {
	SourceName string
	IsIdle     bool
	Because    string
}

// Name identifies this source.
func (s StaticSource) Name() string {
	if s.SourceName == "" {
		return "static"
	}
	return s.SourceName
}

// Evaluate returns the configured fixed verdict.
func (s StaticSource) Evaluate(_ context.Context, _ *appsv1.Deployment, _ time.Time) (Signal, error) {
	return Signal{Name: s.Name(), Idle: s.IsIdle, Detail: s.Because}, nil
}

// round trims sub-second noise so durations read cleanly in events.
func round(d time.Duration) time.Duration { return d.Round(time.Second) }
