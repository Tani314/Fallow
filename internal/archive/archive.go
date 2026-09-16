// Package archive makes deletion reversible.
//
// The delete stage is the only irreversible rung on Fallow's ladder, so
// before a workload is removed its manifest is written to a ConfigMap in the
// same namespace. Recovery then needs nothing but kubectl: the ConfigMap
// holds a clean, re-appliable Deployment with the replica count it had
// before Fallow ever touched it.
//
// A ConfigMap is used rather than object storage on purpose. The archive
// lives in the same namespace, under the same RBAC, with the same lifecycle
// as the workload it replaces -- deleting the namespace disposes of the
// archive too, and no external bucket, credential, or retention policy has
// to exist for the feature to work.
package archive

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
)

const (
	// KeyManifest is the ConfigMap key holding the serialised workload.
	KeyManifest = "manifest.yaml"

	// LabelArchive marks ConfigMaps created by Fallow so they can be listed
	// and cleaned up independently of the workloads they describe.
	LabelArchive = "fallow.dev/archive"
	// LabelWorkload records the name of the archived workload.
	LabelWorkload = "fallow.dev/workload"
	// LabelKind records the kind of the archived workload.
	LabelKind = "fallow.dev/workload-kind"

	// archivePrefix namespaces archive ConfigMaps away from user ConfigMaps.
	archivePrefix = "fallow-archive-"

	// maxNameLength is the Kubernetes limit for a ConfigMap name.
	maxNameLength = 253
)

// Archiver stores workload manifests before deletion and returns them later.
type Archiver interface {
	// Archive writes the workload's manifest to durable storage and returns
	// the name of the archive holding it.
	Archive(ctx context.Context, d *appsv1.Deployment) (string, error)
	// Restore reads an archive and recreates the workload it describes.
	Restore(ctx context.Context, namespace, archiveName string) (*appsv1.Deployment, error)
}

// ConfigMapArchiver stores manifests in ConfigMaps alongside the workload.
type ConfigMapArchiver struct {
	Client client.Client
}

// NewConfigMapArchiver builds an Archiver backed by ConfigMaps.
func NewConfigMapArchiver(c client.Client) *ConfigMapArchiver {
	return &ConfigMapArchiver{Client: c}
}

// ArchiveName is the ConfigMap name used for a given workload. It is
// deterministic so re-archiving a workload overwrites its previous archive
// rather than accumulating copies.
func ArchiveName(workload string) string {
	name := archivePrefix + workload
	if len(name) > maxNameLength {
		name = name[:maxNameLength]
	}
	return name
}

// Archive serialises the workload and stores it in a ConfigMap.
//
// The stored manifest is the one an operator would want back: server-managed
// fields are stripped so it can be re-applied as-is, and the replica count
// recorded before scale-to-zero is restored into the spec, so recovering a
// workload returns it to the size it was running at rather than to zero.
func (a *ConfigMapArchiver) Archive(ctx context.Context, d *appsv1.Deployment) (string, error) {
	name := ArchiveName(d.Name)

	clean := Sanitize(d)
	clean.Annotations[fallowv1alpha1.AnnotationArchive] = name

	manifest, err := yaml.Marshal(clean)
	if err != nil {
		return "", fmt.Errorf("serialising %s/%s: %w", d.Namespace, d.Name, err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: d.Namespace,
			Labels: map[string]string{
				LabelArchive:  "true",
				LabelWorkload: d.Name,
				LabelKind:     "Deployment",
			},
		},
		Data: map[string]string{KeyManifest: string(manifest)},
	}

	if err := a.Client.Create(ctx, cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("writing archive %s/%s: %w", d.Namespace, name, err)
		}
		// An archive from an earlier cycle is stale by definition: the
		// workload has been restored and re-idled since. Overwrite it.
		existing := &corev1.ConfigMap{}
		if err := a.Client.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: name}, existing); err != nil {
			return "", fmt.Errorf("reading existing archive %s/%s: %w", d.Namespace, name, err)
		}
		existing.Labels = cm.Labels
		existing.Data = cm.Data
		if err := a.Client.Update(ctx, existing); err != nil {
			return "", fmt.Errorf("replacing archive %s/%s: %w", d.Namespace, name, err)
		}
	}
	return name, nil
}

// Restore reads an archive and recreates the workload it holds.
func (a *ConfigMapArchiver) Restore(ctx context.Context, namespace, archiveName string) (*appsv1.Deployment, error) {
	cm := &corev1.ConfigMap{}
	if err := a.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: archiveName}, cm); err != nil {
		return nil, fmt.Errorf("reading archive %s/%s: %w", namespace, archiveName, err)
	}

	manifest, ok := cm.Data[KeyManifest]
	if !ok {
		return nil, fmt.Errorf("archive %s/%s has no %s key", namespace, archiveName, KeyManifest)
	}

	d := &appsv1.Deployment{}
	if err := yaml.Unmarshal([]byte(manifest), d); err != nil {
		return nil, fmt.Errorf("decoding archive %s/%s: %w", namespace, archiveName, err)
	}

	if err := a.Client.Create(ctx, d); err != nil {
		return nil, fmt.Errorf("recreating %s/%s: %w", d.Namespace, d.Name, err)
	}
	return d, nil
}

// Sanitize returns a copy of the workload stripped of everything the API
// server owns, so the result can be applied to a cluster unchanged.
func Sanitize(d *appsv1.Deployment) *appsv1.Deployment {
	clean := d.DeepCopy()

	clean.Status = appsv1.DeploymentStatus{}
	clean.ResourceVersion = ""
	clean.UID = ""
	clean.Generation = 0
	clean.CreationTimestamp = metav1.Time{}
	clean.DeletionTimestamp = nil
	clean.ManagedFields = nil
	clean.SelfLink = ""
	// Owner references point at UIDs that will not exist after a restore;
	// keeping them would have the garbage collector delete the workload the
	// moment it came back.
	clean.OwnerReferences = nil

	clean.TypeMeta = metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "Deployment"}

	if clean.Annotations == nil {
		clean.Annotations = map[string]string{}
	}

	// The live object is at zero replicas by the time it is archived, so the
	// count worth preserving is the one Fallow saved on the way down.
	if raw, ok := clean.Annotations[fallowv1alpha1.AnnotationOriginalReplicas]; ok {
		if n, err := strconv.ParseInt(raw, 10, 32); err == nil {
			replicas := int32(n)
			clean.Spec.Replicas = &replicas
		}
	}

	// Fallow's bookkeeping describes a lifecycle that ends here. Leaving it
	// on the manifest would restore a workload that believes it is still
	// mid-escalation.
	delete(clean.Annotations, fallowv1alpha1.AnnotationStage)
	delete(clean.Annotations, fallowv1alpha1.AnnotationIdleSince)
	delete(clean.Annotations, fallowv1alpha1.AnnotationOriginalReplicas)
	delete(clean.Annotations, fallowv1alpha1.AnnotationPolicy)

	return clean
}
