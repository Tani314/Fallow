package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/archive"
	"github.com/Tani314/Fallow/internal/idle"
)

// testResyncPeriod is longer than any deadline the tests configure, so a
// requeue reflects the next stage deadline rather than the polling floor.
const testResyncPeriod = 12 * time.Hour

// fakeClock lets a test walk a three-day escalation in a few microseconds.
type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.t = f.t.Add(d) }

// harness is a reconciler wired to an in-memory cluster.
type harness struct {
	t        *testing.T
	client   client.Client
	clock    *fakeClock
	detector *switchableDetector
	r        *ReclaimPolicyReconciler
	events   chan string
}

// switchableDetector reports whatever the test last told it to, so a test
// can flip a workload from idle to busy mid-escalation.
type switchableDetector struct{ idle bool }

func (s *switchableDetector) Probe(context.Context, *appsv1.Deployment) (idle.Verdict, error) {
	if s.idle {
		return idle.Verdict{Idle: true, Reason: "test: idle"}, nil
	}
	return idle.Verdict{Idle: false, Reason: "test: active"}, nil
}

func newHarness(t *testing.T, objects ...client.Object) *harness {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("building scheme: %v", err)
	}
	if err := fallowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding fallow types: %v", err)
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&fallowv1alpha1.ReclaimPolicy{}).
		Build()

	clock := &fakeClock{t: time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)}
	detector := &switchableDetector{idle: true}
	recorder := record.NewFakeRecorder(1000)

	return &harness{
		t:        t,
		client:   c,
		clock:    clock,
		detector: detector,
		events:   recorder.Events,
		r: &ReclaimPolicyReconciler{
			Client:       c,
			Scheme:       scheme,
			Recorder:     recorder,
			Detector:     detector,
			Archiver:     archive.NewConfigMapArchiver(c),
			Clock:        clock,
			ResyncPeriod: testResyncPeriod,
		},
	}
}

// reconcile runs one pass and fails the test if it errors.
func (h *harness) reconcile(name string) ctrl.Result {
	h.t.Helper()
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
	if err != nil {
		h.t.Fatalf("reconcile %s: %v", name, err)
	}
	return res
}

func (h *harness) deployment(namespace, name string) *appsv1.Deployment {
	h.t.Helper()
	d := &appsv1.Deployment{}
	err := h.client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, d)
	if err != nil {
		h.t.Fatalf("getting deployment %s/%s: %v", namespace, name, err)
	}
	return d
}

func (h *harness) deploymentExists(namespace, name string) bool {
	h.t.Helper()
	err := h.client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &appsv1.Deployment{})
	return err == nil
}

func (h *harness) policy(name string) *fallowv1alpha1.ReclaimPolicy {
	h.t.Helper()
	p := &fallowv1alpha1.ReclaimPolicy{}
	if err := h.client.Get(context.Background(), types.NamespacedName{Name: name}, p); err != nil {
		h.t.Fatalf("getting policy %s: %v", name, err)
	}
	return p
}

// targetFor returns the status entry for one workload.
func (h *harness) targetFor(policyName, namespace, name string) fallowv1alpha1.TargetStatus {
	h.t.Helper()
	for _, target := range h.policy(policyName).Status.Targets {
		if target.Namespace == namespace && target.Name == name {
			return target
		}
	}
	h.t.Fatalf("no status target for %s/%s", namespace, name)
	return fallowv1alpha1.TargetStatus{}
}

func namespaceWithLabels(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func deployment(namespace, name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: map[string]string{"app": name}},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

// fullLadder is the policy used by most tests: notify after 1h idle, scale
// down 2h later, delete 24h after that.
func fullLadder(name string) *fallowv1alpha1.ReclaimPolicy {
	return &fallowv1alpha1.ReclaimPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: fallowv1alpha1.ReclaimPolicySpec{
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"fallow": "enabled"}},
			IdleAfter:         metav1.Duration{Duration: time.Hour},
			Stages: fallowv1alpha1.ReclaimStages{
				Notify:      &fallowv1alpha1.NotifyStage{},
				ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: 2 * time.Hour}},
				Delete:      &fallowv1alpha1.DeleteStage{After: metav1.Duration{Duration: 24 * time.Hour}},
			},
		},
	}
}

// escalate seeds the idle clock with one pass, jumps past every deadline,
// then gives the ladder the passes it needs to climb -- one rung each.
func (h *harness) escalate(name string, passes int) {
	h.t.Helper()
	h.reconcile(name)
	h.clock.Advance(100 * time.Hour)
	for i := 0; i < passes; i++ {
		h.reconcile(name)
	}
}

func replicasOf(t *testing.T, d *appsv1.Deployment) int32 {
	t.Helper()
	if d.Spec.Replicas == nil {
		t.Fatalf("deployment %s/%s has nil replicas", d.Namespace, d.Name)
	}
	return *d.Spec.Replicas
}

// failingArchiver stands in for a cluster where the archive cannot be
// written -- no quota, no RBAC, a wedged API server.
type failingArchiver struct{}

func (failingArchiver) Archive(context.Context, *appsv1.Deployment) (string, error) {
	return "", errArchiveUnavailable
}

func (failingArchiver) Restore(context.Context, string, string) (*appsv1.Deployment, error) {
	return nil, errArchiveUnavailable
}

var errArchiveUnavailable = errors.New("archive storage unavailable")

func reconcileFor(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name}}
}
