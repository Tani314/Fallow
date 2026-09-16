// Package idle decides whether a workload is doing anything worth keeping
// alive.
//
// Idleness is deliberately not hard-coded. Every organisation measures "in
// use" differently -- request rate, CPU, queue depth, a business-hours
// calendar -- so the controller depends only on the Detector interface and
// the signals are assembled at wiring time. Swapping in a Prometheus-backed
// signal changes nothing in the reconciler.
package idle

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
)

// Clock lets tests advance time without sleeping. Production wiring uses
// RealClock; tests use a fake so a 72-hour escalation runs in microseconds.
type Clock interface {
	Now() time.Time
}

// RealClock reads the wall clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// Signal is one input to an idleness decision, carrying the reasoning as
// well as the verdict. The explanation matters: an operator who finds their
// Deployment scaled to zero needs to know which signal caused it.
type Signal struct {
	// Name identifies the source that produced this signal.
	Name string
	// Idle is this source's verdict.
	Idle bool
	// Detail explains the verdict in human terms.
	Detail string
}

// Verdict is the combined result across all signals.
type Verdict struct {
	// Idle is the overall decision.
	Idle bool
	// Signals is the per-source breakdown behind the decision.
	Signals []Signal
	// Reason summarises the verdict for events and status.
	Reason string
}

// Source produces a single Signal for a workload.
type Source interface {
	// Name identifies this source in signal breakdowns.
	Name() string
	// Evaluate reports whether this source considers the workload idle.
	Evaluate(ctx context.Context, d *appsv1.Deployment, now time.Time) (Signal, error)
}

// Detector combines sources into an overall idleness verdict.
type Detector interface {
	// Probe evaluates the workload against every configured source.
	Probe(ctx context.Context, d *appsv1.Deployment) (Verdict, error)
}

// AllOf is the default Detector: a workload counts as idle only when every
// source agrees it is idle.
//
// The conservative AND is the point. Reclamation is disruptive, so a single
// dissenting signal -- one source still seeing traffic -- is enough to keep
// a workload alive. A metrics scrape failure is treated the same way: an
// erroring source votes "busy", never "idle", so a broken signal pipeline
// can never cause a mass scale-down.
type AllOf struct {
	Sources []Source
	Clock   Clock
}

// NewAllOf builds a conservative composite detector over the given sources.
func NewAllOf(clock Clock, sources ...Source) *AllOf {
	if clock == nil {
		clock = RealClock{}
	}
	return &AllOf{Sources: sources, Clock: clock}
}

// Probe evaluates every source and returns the combined verdict.
func (a *AllOf) Probe(ctx context.Context, d *appsv1.Deployment) (Verdict, error) {
	if len(a.Sources) == 0 {
		// No signals configured means no evidence of idleness. Returning
		// "busy" keeps an unconfigured controller inert rather than
		// treating silence as permission to reclaim.
		return Verdict{Idle: false, Reason: "no idle signals configured"}, nil
	}

	now := a.Clock.Now()
	v := Verdict{Idle: true, Signals: make([]Signal, 0, len(a.Sources))}
	var busy []string

	for _, src := range a.Sources {
		sig, err := src.Evaluate(ctx, d, now)
		if err != nil {
			// Fail safe: an unreadable signal counts as busy.
			sig = Signal{
				Name:   src.Name(),
				Idle:   false,
				Detail: fmt.Sprintf("signal unavailable (%v); treating as active", err),
			}
		}
		v.Signals = append(v.Signals, sig)
		if !sig.Idle {
			v.Idle = false
			busy = append(busy, fmt.Sprintf("%s: %s", sig.Name, sig.Detail))
		}
	}

	if v.Idle {
		parts := make([]string, 0, len(v.Signals))
		for _, s := range v.Signals {
			parts = append(parts, fmt.Sprintf("%s: %s", s.Name, s.Detail))
		}
		v.Reason = "all signals idle (" + strings.Join(parts, "; ") + ")"
	} else {
		v.Reason = "active (" + strings.Join(busy, "; ") + ")"
	}
	return v, nil
}
