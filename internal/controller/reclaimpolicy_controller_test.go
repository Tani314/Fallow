package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/archive"
)

// TestLadderEscalatesInOrder walks a workload the whole way down and checks
// that each rung does exactly what it claims to.
func TestLadderEscalatesInOrder(t *testing.T) {
	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 3),
		fullLadder("reclaim-dev"),
	)

	// First pass: the workload is idle but has not been idle long enough.
	// All it earns is a timestamp.
	res := h.reconcile("reclaim-dev")
	if got := h.targetFor("reclaim-dev", "team-a", "api").Stage; got != fallowv1alpha1.StageActive {
		t.Fatalf("stage before idleAfter = %s, want Active", got)
	}
	if res.RequeueAfter != time.Hour {
		t.Fatalf("requeue = %s, want 1h (the notify deadline)", res.RequeueAfter)
	}
	if _, ok := h.deployment("team-a", "api").Annotations[fallowv1alpha1.AnnotationIdleSince]; !ok {
		t.Fatal("idle-since annotation was not recorded on the workload")
	}

	// After idleAfter: notified, but nothing about the workload changes.
	h.clock.Advance(time.Hour)
	h.reconcile("reclaim-dev")
	d := h.deployment("team-a", "api")
	if got := d.Annotations[fallowv1alpha1.AnnotationStage]; got != string(fallowv1alpha1.StageNotified) {
		t.Fatalf("stage annotation = %q, want Notified", got)
	}
	if got := replicasOf(t, d); got != 3 {
		t.Fatalf("replicas after notify = %d, want 3 (notify must not disrupt)", got)
	}

	// Two hours later: scaled to zero, with the old size preserved.
	h.clock.Advance(2 * time.Hour)
	h.reconcile("reclaim-dev")
	d = h.deployment("team-a", "api")
	if got := replicasOf(t, d); got != 0 {
		t.Fatalf("replicas after scale stage = %d, want 0", got)
	}
	if got := d.Annotations[fallowv1alpha1.AnnotationOriginalReplicas]; got != "3" {
		t.Fatalf("original-replicas annotation = %q, want \"3\"", got)
	}
	target := h.targetFor("reclaim-dev", "team-a", "api")
	if target.OriginalReplicas == nil || *target.OriginalReplicas != 3 {
		t.Fatalf("status original replicas = %v, want 3", target.OriginalReplicas)
	}
	if h.policy("reclaim-dev").Status.ReclaimedWorkloads != 1 {
		t.Fatalf("reclaimed count = %d, want 1", h.policy("reclaim-dev").Status.ReclaimedWorkloads)
	}

	// A day later: deleted, with the manifest recoverable.
	h.clock.Advance(24 * time.Hour)
	h.reconcile("reclaim-dev")
	if h.deploymentExists("team-a", "api") {
		t.Fatal("deployment still exists after the delete stage")
	}

	cm := &corev1.ConfigMap{}
	err := h.client.Get(context.Background(), types.NamespacedName{
		Namespace: "team-a", Name: archive.ArchiveName("api"),
	}, cm)
	if err != nil {
		t.Fatalf("archive ConfigMap was not written: %v", err)
	}

	restored := &appsv1.Deployment{}
	if err := yaml.Unmarshal([]byte(cm.Data[archive.KeyManifest]), restored); err != nil {
		t.Fatalf("archived manifest does not parse: %v", err)
	}
	if got := replicasOf(t, restored); got != 3 {
		t.Fatalf("archived replicas = %d, want 3 (the size before reclamation)", got)
	}
	if _, ok := restored.Annotations[fallowv1alpha1.AnnotationStage]; ok {
		t.Fatal("archived manifest still carries Fallow's escalation bookkeeping")
	}
}

// TestDeletedWorkloadStaysInStatus checks the audit trail outlives the
// workload: the record of what was reclaimed, and where the manifest went,
// is the only route back.
func TestDeletedWorkloadStaysInStatus(t *testing.T) {
	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 2),
		fullLadder("reclaim-dev"),
	)

	h.reconcile("reclaim-dev")
	h.clock.Advance(27 * time.Hour)
	for i := 0; i < 3; i++ { // one rung per pass
		h.reconcile("reclaim-dev")
	}
	if h.deploymentExists("team-a", "api") {
		t.Fatal("deployment survived three escalation passes")
	}

	h.clock.Advance(time.Hour)
	h.reconcile("reclaim-dev")

	target := h.targetFor("reclaim-dev", "team-a", "api")
	if target.Stage != fallowv1alpha1.StageDeleted {
		t.Fatalf("stage after deletion = %s, want Deleted", target.Stage)
	}
	if !strings.Contains(target.Reason, archive.ArchiveName("api")) {
		t.Fatalf("status reason %q does not name the archive", target.Reason)
	}
	if got := h.policy("reclaim-dev").Status.ObservedWorkloads; got != 0 {
		t.Fatalf("observed workloads = %d, want 0 (the workload is gone)", got)
	}
}

// TestActivityRestoresWorkload is the promise the whole design rests on:
// a workload that comes back to life gets its replicas back.
func TestActivityRestoresWorkload(t *testing.T) {
	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 4),
		fullLadder("reclaim-dev"),
	)

	h.reconcile("reclaim-dev")
	h.clock.Advance(3 * time.Hour)
	h.reconcile("reclaim-dev") // Notified
	h.reconcile("reclaim-dev") // ScaledToZero
	if got := replicasOf(t, h.deployment("team-a", "api")); got != 0 {
		t.Fatalf("setup failed: replicas = %d, want 0", got)
	}

	h.detector.idle = false
	h.reconcile("reclaim-dev")

	d := h.deployment("team-a", "api")
	if got := replicasOf(t, d); got != 4 {
		t.Fatalf("replicas after restore = %d, want 4", got)
	}
	for _, a := range []string{
		fallowv1alpha1.AnnotationStage,
		fallowv1alpha1.AnnotationIdleSince,
		fallowv1alpha1.AnnotationOriginalReplicas,
		fallowv1alpha1.AnnotationPolicy,
	} {
		if _, ok := d.Annotations[a]; ok {
			t.Errorf("annotation %s survived restore", a)
		}
	}

	target := h.targetFor("reclaim-dev", "team-a", "api")
	if target.Stage != fallowv1alpha1.StageActive {
		t.Fatalf("stage after restore = %s, want Active", target.Stage)
	}
	if target.IdleSince != nil {
		t.Fatalf("idleSince = %v, want nil after restore", target.IdleSince)
	}
}

// TestOutageClimbsOneRungPerPass checks that a controller returning to find
// every deadline blown does not skip straight to deletion.
func TestOutageClimbsOneRungPerPass(t *testing.T) {
	d := deployment("team-a", "api", 5)
	d.Annotations = map[string]string{
		fallowv1alpha1.AnnotationIdleSince: "2026-02-01T09:00:00Z", // ~4 weeks stale
	}

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		d,
		fullLadder("reclaim-dev"),
	)

	res := h.reconcile("reclaim-dev")
	if got := h.targetFor("reclaim-dev", "team-a", "api").Stage; got != fallowv1alpha1.StageNotified {
		t.Fatalf("first pass reached %s, want Notified", got)
	}
	if res.RequeueAfter != climbRequeue {
		t.Fatalf("requeue = %s, want a prompt %s (more rungs pending)", res.RequeueAfter, climbRequeue)
	}

	h.reconcile("reclaim-dev")
	if got := replicasOf(t, h.deployment("team-a", "api")); got != 0 {
		t.Fatalf("second pass left replicas at %d, want 0", got)
	}
	if got := h.targetFor("reclaim-dev", "team-a", "api").OriginalReplicas; got == nil || *got != 5 {
		t.Fatalf("original replicas = %v, want 5 captured on the way past ScaledToZero", got)
	}

	h.reconcile("reclaim-dev")
	if h.deploymentExists("team-a", "api") {
		t.Fatal("third pass should have deleted the workload")
	}
}

// TestDryRunTouchesNothing checks that a dry run reports the full escalation
// while leaving every workload exactly as it found it.
func TestDryRunTouchesNothing(t *testing.T) {
	policy := fullLadder("reclaim-dev")
	policy.Spec.DryRun = true

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 3),
		policy,
	)

	h.reconcile("reclaim-dev")
	h.clock.Advance(3 * time.Hour)
	h.reconcile("reclaim-dev") // would notify
	h.reconcile("reclaim-dev") // would scale to zero

	d := h.deployment("team-a", "api")
	if got := replicasOf(t, d); got != 3 {
		t.Fatalf("dry run changed replicas to %d", got)
	}
	if len(d.Annotations) != 0 {
		t.Fatalf("dry run wrote annotations: %v", d.Annotations)
	}

	// Status still tracks the escalation, which is the whole point: the
	// policy's own status is the dry run's only memory.
	if got := h.targetFor("reclaim-dev", "team-a", "api").Stage; got != fallowv1alpha1.StageScaledToZero {
		t.Fatalf("dry run stage = %s, want ScaledToZero reported", got)
	}

	h.clock.Advance(24 * time.Hour)
	h.reconcile("reclaim-dev")
	if !h.deploymentExists("team-a", "api") {
		t.Fatal("dry run deleted the workload")
	}
	cms := &corev1.ConfigMapList{}
	if err := h.client.List(context.Background(), cms); err != nil {
		t.Fatalf("listing configmaps: %v", err)
	}
	if len(cms.Items) != 0 {
		t.Fatalf("dry run wrote %d archive ConfigMap(s)", len(cms.Items))
	}
}

// TestSafetyRails covers the ways a workload stays out of reach.
func TestSafetyRails(t *testing.T) {
	excluded := deployment("team-a", "opted-out", 1)
	excluded.Annotations = map[string]string{fallowv1alpha1.AnnotationExclude: "true"}

	claimed := deployment("team-a", "claimed-elsewhere", 1)
	claimed.Annotations = map[string]string{fallowv1alpha1.AnnotationPolicy: "some-other-policy"}

	policy := fullLadder("reclaim-dev")
	policy.Spec.ProtectedNamespaces = []string{"team-b"}

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		namespaceWithLabels("team-b", map[string]string{"fallow": "enabled"}),
		namespaceWithLabels("kube-system", map[string]string{"fallow": "enabled"}),
		excluded,
		claimed,
		deployment("team-a", "governed", 1),
		deployment("team-b", "protected", 1),
		deployment("kube-system", "coredns", 2),
		policy,
	)

	h.escalate("reclaim-dev", 3)

	for _, tc := range []struct{ namespace, name, why string }{
		{"team-a", "opted-out", "excluded by annotation"},
		{"team-a", "claimed-elsewhere", "claimed by another policy"},
		{"team-b", "protected", "in a protected namespace"},
		{"kube-system", "coredns", "in a system namespace"},
	} {
		if !h.deploymentExists(tc.namespace, tc.name) {
			t.Errorf("%s/%s was reclaimed despite being %s", tc.namespace, tc.name, tc.why)
		}
	}
	if h.deploymentExists("team-a", "governed") {
		t.Error("the governed workload was not reclaimed")
	}
	if got := h.policy("reclaim-dev").Status.EnrolledNamespaces; got != 1 {
		t.Errorf("enrolled namespaces = %d, want 1 (only team-a is both matched and unprotected)", got)
	}
}

// TestNoNamespaceSelectorIsInert checks that the empty policy does nothing
// and says so.
func TestNoNamespaceSelectorIsInert(t *testing.T) {
	policy := fullLadder("reclaim-dev")
	policy.Spec.NamespaceSelector = nil

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 1),
		policy,
	)

	h.clock.Advance(100 * time.Hour)
	h.reconcile("reclaim-dev")
	h.reconcile("reclaim-dev")

	if !h.deploymentExists("team-a", "api") {
		t.Fatal("a policy with no namespaceSelector reclaimed a workload")
	}
	conditions := h.policy("reclaim-dev").Status.Conditions
	if len(conditions) != 1 || conditions[0].Reason != "NoNamespaceSelector" {
		t.Fatalf("conditions = %+v, want a single NoNamespaceSelector condition", conditions)
	}
}

// TestTruncatedLadderStops checks that leaving a stage out is a real stop,
// not a pause.
func TestTruncatedLadderStops(t *testing.T) {
	policy := fullLadder("notify-only")
	policy.Spec.Stages.ScaleToZero = nil
	policy.Spec.Stages.Delete = nil

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 2),
		policy,
	)

	h.escalate("notify-only", 5)

	d := h.deployment("team-a", "api")
	if got := replicasOf(t, d); got != 2 {
		t.Fatalf("notify-only policy changed replicas to %d", got)
	}
	if got := h.targetFor("notify-only", "team-a", "api").Stage; got != fallowv1alpha1.StageNotified {
		t.Fatalf("stage = %s, want Notified forever", got)
	}
}

// TestWorkloadSelectorNarrows checks the second selector actually filters.
func TestWorkloadSelectorNarrows(t *testing.T) {
	policy := fullLadder("reclaim-dev")
	policy.Spec.WorkloadSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "batch"}}

	batch := deployment("team-a", "batch-job", 1)
	batch.Labels["tier"] = "batch"

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		batch,
		deployment("team-a", "frontend", 1),
		policy,
	)

	h.escalate("reclaim-dev", 3)

	if h.deploymentExists("team-a", "batch-job") {
		t.Error("selected workload was not reclaimed")
	}
	if !h.deploymentExists("team-a", "frontend") {
		t.Error("unselected workload was reclaimed")
	}
}

// TestArchiveFailureBlocksDeletion checks that reversibility is a
// precondition of deletion, not a best-effort extra.
func TestArchiveFailureBlocksDeletion(t *testing.T) {
	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 1),
		fullLadder("reclaim-dev"),
	)
	h.r.Archiver = failingArchiver{}

	h.escalate("reclaim-dev", 2) // Notified, then ScaledToZero

	_, err := h.r.Reconcile(context.Background(), reconcileFor("reclaim-dev"))
	if err == nil {
		t.Fatal("delete stage succeeded despite the archive failing")
	}
	if !h.deploymentExists("team-a", "api") {
		t.Fatal("workload was deleted even though its manifest could not be archived")
	}
}

// TestDeleteWithoutArchiveIsAllowedExplicitly checks the opt-out works, since
// some workloads genuinely are disposable.
func TestDeleteWithoutArchiveIsAllowedExplicitly(t *testing.T) {
	policy := fullLadder("reclaim-dev")
	policy.Spec.Stages.Delete.Archive = ptr(false)

	h := newHarness(t,
		namespaceWithLabels("team-a", map[string]string{"fallow": "enabled"}),
		deployment("team-a", "api", 1),
		policy,
	)

	h.escalate("reclaim-dev", 3)

	if h.deploymentExists("team-a", "api") {
		t.Fatal("workload was not deleted")
	}
	cms := &corev1.ConfigMapList{}
	if err := h.client.List(context.Background(), cms); err != nil {
		t.Fatalf("listing configmaps: %v", err)
	}
	if len(cms.Items) != 0 {
		t.Fatalf("archiving was disabled but %d ConfigMap(s) were written", len(cms.Items))
	}
}
