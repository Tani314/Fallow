package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Stage is a rung on the escalation ladder. A workload climbs one rung at a
// time and falls straight back to StageActive the moment it looks busy again.
type Stage string

const (
	// StageActive means the workload is in use, or has been restored.
	StageActive Stage = "Active"
	// StageNotified means Fallow has annotated the workload and emitted an
	// event, but has not touched its runtime behaviour.
	StageNotified Stage = "Notified"
	// StageScaledToZero means replicas were set to 0. The original count is
	// preserved so the workload can be restored exactly.
	StageScaledToZero Stage = "ScaledToZero"
	// StageDeleted means the workload was removed after the grace period.
	// If archiving was enabled the manifest is recoverable.
	StageDeleted Stage = "Deleted"
)

// Annotations Fallow writes onto the workloads it manages. They are the
// controller's durable memory: everything needed to reverse an action lives
// on the object itself, so a controller restart loses nothing.
const (
	// AnnotationStage records the current escalation stage.
	AnnotationStage = "fallow.dev/stage"
	// AnnotationIdleSince records when the workload was first seen idle.
	AnnotationIdleSince = "fallow.dev/idle-since"
	// AnnotationOriginalReplicas preserves the replica count from before
	// scale-to-zero, so restoring returns the exact prior size.
	AnnotationOriginalReplicas = "fallow.dev/original-replicas"
	// AnnotationPolicy names the ReclaimPolicy that claimed this workload.
	AnnotationPolicy = "fallow.dev/policy"
	// AnnotationExclude opts a workload out entirely when set to "true".
	AnnotationExclude = "fallow.dev/exclude"
	// AnnotationArchive names the ConfigMap holding a deleted workload's manifest.
	AnnotationArchive = "fallow.dev/archived-in"
)

// NotifyStage annotates the workload and emits a Kubernetes event. It changes
// nothing about how the workload runs -- it exists to give owners a chance to
// react before anything disruptive happens.
type NotifyStage struct {
	// Message is an optional custom line included in the emitted event.
	// +optional
	Message string `json:"message,omitempty"`
}

// ScaleToZeroStage sets replicas to 0 after the workload has sat in
// StageNotified for After. The prior replica count is saved first.
type ScaleToZeroStage struct {
	// After is how long a workload stays notified before being scaled down.
	After metav1.Duration `json:"after"`
}

// DeleteStage removes the workload after it has sat at zero replicas for
// After. This is the only destructive stage, so it is opt-in and archives
// the manifest by default.
type DeleteStage struct {
	// After is the grace period at zero replicas before deletion.
	After metav1.Duration `json:"after"`

	// Archive stores the full workload manifest in a ConfigMap before
	// deleting, which is what keeps this stage reversible. Disabling it
	// makes deletion permanent.
	// +kubebuilder:default=true
	// +optional
	Archive *bool `json:"archive,omitempty"`
}

// ReclaimStages is the escalation ladder. Stages are strictly ordered; a
// workload cannot skip a rung. Omitting a stage truncates the ladder there,
// so a policy with only Notify set will never scale or delete anything.
type ReclaimStages struct {
	// Notify is the first stage. Omitting it disables the policy entirely.
	// +optional
	Notify *NotifyStage `json:"notify,omitempty"`

	// ScaleToZero is the second stage.
	// +optional
	ScaleToZero *ScaleToZeroStage `json:"scaleToZero,omitempty"`

	// Delete is the third and final stage.
	// +optional
	Delete *DeleteStage `json:"delete,omitempty"`
}

// ReclaimPolicySpec defines which workloads a policy governs and how
// aggressively it escalates.
type ReclaimPolicySpec struct {
	// NamespaceSelector enrolls namespaces. A namespace must match for any
	// workload inside it to be considered. A nil selector enrolls nothing --
	// enrollment is deliberately opt-in, so an empty policy is inert rather
	// than cluster-wide.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`

	// WorkloadSelector narrows which workloads inside enrolled namespaces
	// are governed. A nil selector matches every workload in those namespaces.
	// +optional
	WorkloadSelector *metav1.LabelSelector `json:"workloadSelector,omitempty"`

	// IdleAfter is how long a workload must look idle before Fallow begins
	// escalating it.
	IdleAfter metav1.Duration `json:"idleAfter"`

	// Stages is the escalation ladder.
	Stages ReclaimStages `json:"stages"`

	// DryRun reports every action the policy would take, via events and
	// status, without mutating a single workload.
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// ProtectedNamespaces are never touched even if they match the
	// NamespaceSelector. System namespaces are always protected regardless
	// of what is listed here.
	// +optional
	ProtectedNamespaces []string `json:"protectedNamespaces,omitempty"`
}

// TargetStatus is the per-workload record of what Fallow has done and why.
type TargetStatus struct {
	// Namespace of the governed workload.
	Namespace string `json:"namespace"`
	// Name of the governed workload.
	Name string `json:"name"`
	// Kind of the governed workload (currently always Deployment).
	Kind string `json:"kind"`
	// Stage is the rung this workload currently sits on.
	Stage Stage `json:"stage"`

	// IdleSince is when the workload was first observed idle. Cleared when
	// it becomes active again.
	// +optional
	IdleSince *metav1.Time `json:"idleSince,omitempty"`

	// LastTransitionTime is when Stage last changed.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// OriginalReplicas is the replica count captured before scaling to zero.
	// +optional
	OriginalReplicas *int32 `json:"originalReplicas,omitempty"`

	// Reason is a human-readable explanation of the current stage.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// ReclaimPolicyStatus is the observed state of a ReclaimPolicy.
type ReclaimPolicyStatus struct {
	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// EnrolledNamespaces counts namespaces matching the NamespaceSelector.
	// +optional
	EnrolledNamespaces int32 `json:"enrolledNamespaces"`

	// ObservedWorkloads counts workloads this policy governs.
	// +optional
	ObservedWorkloads int32 `json:"observedWorkloads"`

	// ReclaimedWorkloads counts workloads currently scaled to zero or deleted.
	// +optional
	ReclaimedWorkloads int32 `json:"reclaimedWorkloads"`

	// Targets is the per-workload breakdown.
	// +optional
	// +listType=atomic
	Targets []TargetStatus `json:"targets,omitempty"`

	// Conditions follow the standard Kubernetes condition conventions.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=rp
// +kubebuilder:printcolumn:name="Idle After",type=string,JSONPath=`.spec.idleAfter`
// +kubebuilder:printcolumn:name="Namespaces",type=integer,JSONPath=`.status.enrolledNamespaces`
// +kubebuilder:printcolumn:name="Workloads",type=integer,JSONPath=`.status.observedWorkloads`
// +kubebuilder:printcolumn:name="Reclaimed",type=integer,JSONPath=`.status.reclaimedWorkloads`
// +kubebuilder:printcolumn:name="Dry Run",type=boolean,JSONPath=`.spec.dryRun`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ReclaimPolicy declares which idle workloads to reclaim and how far to go.
type ReclaimPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ReclaimPolicySpec   `json:"spec,omitempty"`
	Status ReclaimPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ReclaimPolicyList contains a list of ReclaimPolicy.
type ReclaimPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ReclaimPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ReclaimPolicy{}, &ReclaimPolicyList{})
}
