package archive

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
)

func newClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("building scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// reclaimed is a workload as it looks at the moment of deletion: scaled to
// zero, carrying Fallow's bookkeeping, and full of server-managed fields.
func reclaimed() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "team-a",
			Name:              "api",
			UID:               types.UID("2f7a1c0e-0000-4000-8000-00000000abcd"),
			ResourceVersion:   "8471",
			Generation:        12,
			CreationTimestamp: metav1.NewTime(time.Date(2025, 11, 2, 8, 30, 0, 0, time.UTC)),
			SelfLink:          "/apis/apps/v1/namespaces/team-a/deployments/api",
			Labels:            map[string]string{"app": "api"},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "{...}",
				fallowv1alpha1.AnnotationStage:                     string(fallowv1alpha1.StageScaledToZero),
				fallowv1alpha1.AnnotationIdleSince:                 "2026-02-01T09:00:00Z",
				fallowv1alpha1.AnnotationOriginalReplicas:          "7",
				fallowv1alpha1.AnnotationPolicy:                    "reclaim-dev",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "ConfigMap", Name: "gone", UID: types.UID("dead-beef"),
			}},
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationApply}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(0)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "api", Image: "registry.example.com/api:v4.2.0"},
				}},
			},
		},
		Status: appsv1.DeploymentStatus{Replicas: 0, ObservedGeneration: 12},
	}
}

func ptr[T any](v T) *T { return &v }

// TestSanitizeProducesAnApplyableManifest checks the archived manifest is
// something kubectl apply will accept, at the size the workload was running
// at before Fallow touched it.
func TestSanitizeProducesAnApplyableManifest(t *testing.T) {
	clean := Sanitize(reclaimed())

	if clean.Spec.Replicas == nil || *clean.Spec.Replicas != 7 {
		t.Fatalf("replicas = %v, want the 7 recorded before scale-to-zero", clean.Spec.Replicas)
	}
	if clean.UID != "" || clean.ResourceVersion != "" || clean.Generation != 0 || clean.SelfLink != "" {
		t.Error("server-managed identity fields survived sanitizing")
	}
	if !clean.CreationTimestamp.IsZero() || clean.ManagedFields != nil {
		t.Error("creationTimestamp or managedFields survived sanitizing")
	}
	if clean.OwnerReferences != nil {
		t.Error("owner references survived; a restore would be garbage-collected immediately")
	}
	if clean.Status.ObservedGeneration != 0 {
		t.Error("status survived sanitizing")
	}
	if clean.Kind != "Deployment" || clean.APIVersion != "apps/v1" {
		t.Errorf("TypeMeta = %s/%s, want apps/v1 Deployment so the manifest is self-describing", clean.APIVersion, clean.Kind)
	}

	for _, a := range []string{
		fallowv1alpha1.AnnotationStage,
		fallowv1alpha1.AnnotationIdleSince,
		fallowv1alpha1.AnnotationOriginalReplicas,
		fallowv1alpha1.AnnotationPolicy,
	} {
		if _, ok := clean.Annotations[a]; ok {
			t.Errorf("escalation annotation %s survived; a restore would resume mid-ladder", a)
		}
	}
	if clean.Annotations["kubectl.kubernetes.io/last-applied-configuration"] == "" {
		t.Error("sanitizing removed an annotation that was not Fallow's to remove")
	}

	// The input must not be mutated: the caller still has to delete it.
	original := reclaimed()
	Sanitize(original)
	if original.UID == "" || original.Spec.Replicas == nil || *original.Spec.Replicas != 0 {
		t.Error("Sanitize mutated the workload it was given")
	}
}

// TestArchiveRestoreRoundTrip is the reversibility promise, end to end.
func TestArchiveRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	d := reclaimed()
	c := newClient(t, d)
	archiver := NewConfigMapArchiver(c)

	name, err := archiver.Archive(ctx, d)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if name != ArchiveName("api") {
		t.Fatalf("archive name = %q, want the deterministic %q", name, ArchiveName("api"))
	}

	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "team-a", Name: name}, cm); err != nil {
		t.Fatalf("reading archive: %v", err)
	}
	if cm.Labels[LabelArchive] != "true" || cm.Labels[LabelWorkload] != "api" {
		t.Errorf("archive labels = %v, want it findable by workload", cm.Labels)
	}

	// Deletion is what the archive exists to survive.
	if err := c.Delete(ctx, d); err != nil {
		t.Fatalf("deleting workload: %v", err)
	}

	restored, err := archiver.Restore(ctx, "team-a", name)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Spec.Replicas == nil || *restored.Spec.Replicas != 7 {
		t.Fatalf("restored replicas = %v, want 7", restored.Spec.Replicas)
	}
	if got := restored.Spec.Template.Spec.Containers[0].Image; got != "registry.example.com/api:v4.2.0" {
		t.Fatalf("restored image = %q, want the archived one", got)
	}
	if got := restored.Annotations[fallowv1alpha1.AnnotationArchive]; got != name {
		t.Errorf("restored workload records archive %q, want %q as provenance", got, name)
	}

	live := &appsv1.Deployment{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "team-a", Name: "api"}, live); err != nil {
		t.Fatalf("restored workload is not in the cluster: %v", err)
	}
}

// TestArchiveOverwritesStaleCopy checks a second reclamation cycle replaces
// the first archive rather than failing on the name it already used.
func TestArchiveOverwritesStaleCopy(t *testing.T) {
	ctx := context.Background()
	d := reclaimed()
	c := newClient(t, d)
	archiver := NewConfigMapArchiver(c)

	if _, err := archiver.Archive(ctx, d); err != nil {
		t.Fatalf("first Archive: %v", err)
	}

	d.Annotations[fallowv1alpha1.AnnotationOriginalReplicas] = "2"
	name, err := archiver.Archive(ctx, d)
	if err != nil {
		t.Fatalf("second Archive: %v", err)
	}

	restored, err := archiver.Restore(ctx, "team-a", name)
	if err != nil {
		// Restore fails if the workload still exists; delete first.
		if err := c.Delete(ctx, d); err != nil {
			t.Fatalf("deleting workload: %v", err)
		}
		if restored, err = archiver.Restore(ctx, "team-a", name); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	}
	if restored.Spec.Replicas == nil || *restored.Spec.Replicas != 2 {
		t.Fatalf("restored replicas = %v, want 2 from the newer archive", restored.Spec.Replicas)
	}
}

// TestRestoreRejectsAMalformedArchive checks the failure is explained rather
// than silently producing an empty Deployment.
func TestRestoreRejectsAMalformedArchive(t *testing.T) {
	ctx := context.Background()
	c := newClient(t, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "fallow-archive-broken"},
		Data:       map[string]string{"something-else": "{}"},
	})

	if _, err := NewConfigMapArchiver(c).Restore(ctx, "team-a", "fallow-archive-broken"); err == nil {
		t.Fatal("restoring an archive with no manifest key should fail")
	}
	if _, err := NewConfigMapArchiver(c).Restore(ctx, "team-a", "absent"); err == nil {
		t.Fatal("restoring a missing archive should fail")
	}
}

func TestArchiveNameFitsKubernetesLimits(t *testing.T) {
	long := ArchiveName(string(make([]byte, 400)))
	if len(long) > maxNameLength {
		t.Fatalf("archive name is %d characters, over the %d limit", len(long), maxNameLength)
	}
}
