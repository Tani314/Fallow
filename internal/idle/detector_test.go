package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var reference = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

type fixedClock struct{ t time.Time }

func (f fixedClock) Now() time.Time { return f.t }

// erroringSource stands in for a metrics backend that is down.
type erroringSource struct{ name string }

func (e erroringSource) Name() string { return e.name }
func (e erroringSource) Evaluate(context.Context, *appsv1.Deployment, time.Time) (Signal, error) {
	return Signal{}, errors.New("scrape timed out")
}

func deployment(annotations map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "api", Annotations: annotations},
	}
}

// TestAllOfRequiresUnanimity checks the conservative AND: one source still
// seeing life is enough to keep a workload alive.
func TestAllOfRequiresUnanimity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sources []Source
		want    bool
	}{
		{
			name:    "every source idle",
			sources: []Source{StaticSource{SourceName: "a", IsIdle: true}, StaticSource{SourceName: "b", IsIdle: true}},
			want:    true,
		},
		{
			name:    "one dissenter",
			sources: []Source{StaticSource{SourceName: "a", IsIdle: true}, StaticSource{SourceName: "b", IsIdle: false}},
			want:    false,
		},
		{
			name:    "no sources configured",
			sources: nil,
			want:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := NewAllOf(fixedClock{reference}, tc.sources...).Probe(context.Background(), deployment(nil))
			if err != nil {
				t.Fatalf("Probe: %v", err)
			}
			if v.Idle != tc.want {
				t.Fatalf("Idle = %v, want %v (reason: %s)", v.Idle, tc.want, v.Reason)
			}
		})
	}
}

// TestBrokenSignalVotesBusy is the property that keeps a metrics outage from
// becoming a cluster-wide scale-down.
func TestBrokenSignalVotesBusy(t *testing.T) {
	d := NewAllOf(fixedClock{reference},
		StaticSource{SourceName: "traffic", IsIdle: true},
		erroringSource{name: "cpu"},
	)

	v, err := d.Probe(context.Background(), deployment(nil))
	if err != nil {
		t.Fatalf("Probe returned an error instead of voting busy: %v", err)
	}
	if v.Idle {
		t.Fatal("a broken signal was counted as idle")
	}
	if !strings.Contains(v.Reason, "cpu") || !strings.Contains(v.Reason, "unavailable") {
		t.Fatalf("reason %q does not explain which signal failed", v.Reason)
	}
}

// TestVerdictExplainsItself checks that every source's reasoning survives
// into the verdict, since that is what an operator reads after finding a
// workload scaled to zero.
func TestVerdictExplainsItself(t *testing.T) {
	v, err := NewAllOf(fixedClock{reference},
		StaticSource{SourceName: "traffic", IsIdle: true, Because: "no requests in 30d"},
		StaticSource{SourceName: "cpu", IsIdle: true, Because: "2m < 50m threshold"},
	).Probe(context.Background(), deployment(nil))
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if len(v.Signals) != 2 {
		t.Fatalf("got %d signals, want 2", len(v.Signals))
	}
	for _, want := range []string{"no requests in 30d", "2m < 50m threshold"} {
		if !strings.Contains(v.Reason, want) {
			t.Errorf("reason %q is missing %q", v.Reason, want)
		}
	}
}

func TestLastActivity(t *testing.T) {
	source := LastActivity{Window: 24 * time.Hour}

	for _, tc := range []struct {
		name      string
		value     string
		present   bool
		wantIdle  bool
		wantError bool
	}{
		{name: "no annotation means unknown, not idle", present: false, wantIdle: false},
		{name: "quiet longer than the window", present: true, value: reference.Add(-48 * time.Hour).Format(time.RFC3339), wantIdle: true},
		{name: "active inside the window", present: true, value: reference.Add(-time.Hour).Format(time.RFC3339), wantIdle: false},
		{name: "unparseable timestamp errors", present: true, value: "last tuesday", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var annotations map[string]string
			if tc.present {
				annotations = map[string]string{AnnotationLastActivity: tc.value}
			}

			sig, err := source.Evaluate(context.Background(), deployment(annotations), reference)
			if tc.wantError {
				if err == nil {
					t.Fatal("want an error so the composite detector can vote busy")
				}
				return
			}
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if sig.Idle != tc.wantIdle {
				t.Fatalf("Idle = %v, want %v (%s)", sig.Idle, tc.wantIdle, sig.Detail)
			}
		})
	}
}

// TestRolloutQuiet checks that work in progress is protected: a Deployment
// shipped an hour ago is not idle just because nobody has called it yet.
func TestRolloutQuiet(t *testing.T) {
	source := RolloutQuiet{Window: 7 * 24 * time.Hour}

	recent := deployment(nil)
	recent.CreationTimestamp = metav1.NewTime(reference.Add(-30 * 24 * time.Hour))
	recent.Status.Conditions = []appsv1.DeploymentCondition{
		{Type: appsv1.DeploymentProgressing, LastUpdateTime: metav1.NewTime(reference.Add(-time.Hour))},
	}
	sig, err := source.Evaluate(context.Background(), recent, reference)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if sig.Idle {
		t.Fatalf("a workload rolled out an hour ago was called idle: %s", sig.Detail)
	}

	old := deployment(nil)
	old.CreationTimestamp = metav1.NewTime(reference.Add(-90 * 24 * time.Hour))
	sig, err = source.Evaluate(context.Background(), old, reference)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !sig.Idle {
		t.Fatalf("a workload untouched for 90 days was called active: %s", sig.Detail)
	}
}

// stubMetrics is a metrics backend that always reports the same usage.
type stubMetrics struct {
	milli int64
	err   error
}

func (s stubMetrics) CPUMilliCores(context.Context, string, string) (int64, error) {
	return s.milli, s.err
}

func TestCPUBelow(t *testing.T) {
	idleSig, err := CPUBelow{Threshold: 50, Provider: stubMetrics{milli: 2}}.
		Evaluate(context.Background(), deployment(nil), reference)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if !idleSig.Idle {
		t.Fatalf("2m should be under a 50m threshold: %s", idleSig.Detail)
	}

	busySig, err := CPUBelow{Threshold: 50, Provider: stubMetrics{milli: 400}}.
		Evaluate(context.Background(), deployment(nil), reference)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if busySig.Idle {
		t.Fatalf("400m should be over a 50m threshold: %s", busySig.Detail)
	}

	if _, err := (CPUBelow{Threshold: 50}).Evaluate(context.Background(), deployment(nil), reference); err == nil {
		t.Fatal("a missing metrics provider must error rather than report idle")
	}
}
