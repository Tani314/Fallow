package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/archive"
	"github.com/Tani314/Fallow/internal/idle"
)

// DefaultResyncPeriod is how often a policy is re-examined when no deadline
// falls sooner. Idleness is measured in hours or days, so polling is cheap.
const DefaultResyncPeriod = 5 * time.Minute

// conditionReady is the condition summarising whether a policy is working.
const conditionReady = "Ready"

// ReclaimPolicyReconciler reconciles a ReclaimPolicy against the workloads it
// governs.
type ReclaimPolicyReconciler struct {
	client.Client

	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// Detector decides whether a workload is idle. Injected rather than
	// constructed here so the signals can be changed -- or stubbed in tests
	// -- without touching the reconciler.
	Detector idle.Detector

	// Archiver stores manifests before deletion.
	Archiver archive.Archiver

	// Clock is the time source. Tests supply a fake so a three-day
	// escalation runs instantly.
	Clock idle.Clock

	// ResyncPeriod bounds how long the controller will go without looking
	// at a policy.
	ResyncPeriod time.Duration

	// AlwaysProtected names namespaces no policy may ever touch, whatever
	// its selectors say. Fallow's own namespace belongs here.
	AlwaysProtected []string
}

// The controller reads policies and writes their status; it never creates or
// deletes them, and it sets no finalizers, so it asks for neither. Workloads
// need the full set, because deleting them is the last rung of the ladder.
// ConfigMaps are the archive, and list/watch are there because the cached
// client backs every Get with an informer.
// +kubebuilder:rbac:groups=fallow.dev,resources=reclaimpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=fallow.dev,resources=reclaimpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile brings one policy's view of the cluster up to date: it finds the
// workloads the policy governs, moves each one along the ladder as its
// deadlines fall due, and republishes the whole picture in status.
func (r *ReclaimPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	policy := &fallowv1alpha1.ReclaimPolicy{}
	if err := r.Get(ctx, req.NamespacedName, policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status := fallowv1alpha1.ReclaimPolicyStatus{
		ObservedGeneration: policy.Generation,
		Conditions:         policy.Status.Conditions,
	}
	previous := indexTargets(policy.Status.Targets)

	namespaces, err := r.enrolledNamespaces(ctx, policy)
	if err != nil {
		r.setReady(&status, metav1.ConditionFalse, "NamespaceListFailed", err.Error())
		return ctrl.Result{}, errors.Join(err, r.writeStatus(ctx, policy, status))
	}
	status.EnrolledNamespaces = int32(len(namespaces))

	if policy.Spec.NamespaceSelector == nil {
		// Enrollment is opt-in: a policy with no selector governs nothing.
		// Saying so in status is friendlier than a silent no-op, because
		// "nothing happened" is exactly what a misconfigured policy looks
		// like from the outside.
		r.setReady(&status, metav1.ConditionFalse, "NoNamespaceSelector",
			"spec.namespaceSelector is empty, so this policy governs no namespaces")
		return ctrl.Result{RequeueAfter: r.resync()}, r.writeStatus(ctx, policy, status)
	}

	// A nil workload selector means every workload in the enrolled
	// namespaces, per the API contract. LabelSelectorAsSelector reads nil as
	// "match nothing", so the nil case is handled here rather than delegated.
	workloadSelector := labels.Everything()
	if policy.Spec.WorkloadSelector != nil {
		var err error
		if workloadSelector, err = metav1.LabelSelectorAsSelector(policy.Spec.WorkloadSelector); err != nil {
			r.setReady(&status, metav1.ConditionFalse, "InvalidWorkloadSelector", err.Error())
			return ctrl.Result{}, r.writeStatus(ctx, policy, status)
		}
	}

	var (
		targets  []fallowv1alpha1.TargetStatus
		failures []error
		soonest  = r.resync()
	)

	for _, ns := range namespaces {
		deployments := &appsv1.DeploymentList{}
		if err := r.List(ctx, deployments,
			client.InNamespace(ns),
			client.MatchingLabelsSelector{Selector: workloadSelector},
		); err != nil {
			failures = append(failures, fmt.Errorf("listing deployments in %s: %w", ns, err))
			continue
		}

		for i := range deployments.Items {
			d := &deployments.Items[i]
			if skip, why := r.shouldSkip(policy, d); skip {
				logger.V(1).Info("skipping workload", "workload", d.Namespace+"/"+d.Name, "reason", why)
				continue
			}

			target, requeueAfter, err := r.reconcileWorkload(ctx, policy, d, previous[key(d.Namespace, d.Name)])
			if err != nil {
				// One unhappy workload must not stop the others: a policy
				// covering a hundred Deployments should still make progress
				// when one of them is wedged.
				failures = append(failures, err)
				if prev := previous[key(d.Namespace, d.Name)]; prev != nil {
					targets = append(targets, *prev)
				}
				continue
			}
			targets = append(targets, target)
			if requeueAfter > 0 && requeueAfter < soonest {
				soonest = requeueAfter
			}
		}
	}

	// Deleted workloads no longer show up in a List, but the record of what
	// this policy reclaimed -- and which ConfigMap holds the manifest -- is
	// the only trail back to them, so it is carried forward.
	targets = append(targets, carryForwardDeleted(previous, targets, namespaces)...)
	sortTargets(targets)

	status.Targets = targets
	status.ObservedWorkloads = countLive(targets)
	status.ReclaimedWorkloads = countReclaimed(targets)

	if len(failures) > 0 {
		r.setReady(&status, metav1.ConditionFalse, "PartialFailure",
			fmt.Sprintf("%d workload(s) could not be reconciled: %v", len(failures), errors.Join(failures...)))
		return ctrl.Result{}, errors.Join(append(failures, r.writeStatus(ctx, policy, status))...)
	}

	r.setReady(&status, metav1.ConditionTrue, "Reconciled",
		fmt.Sprintf("governing %d workload(s) across %d namespace(s)", status.ObservedWorkloads, status.EnrolledNamespaces))

	if err := r.writeStatus(ctx, policy, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: soonest}, nil
}

// enrolledNamespaces returns the namespaces the policy may act in, with
// protected ones removed.
func (r *ReclaimPolicyReconciler) enrolledNamespaces(ctx context.Context, policy *fallowv1alpha1.ReclaimPolicy) ([]string, error) {
	if policy.Spec.NamespaceSelector == nil {
		return nil, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(policy.Spec.NamespaceSelector)
	if err != nil {
		return nil, fmt.Errorf("parsing namespaceSelector: %w", err)
	}
	if selector.Empty() {
		// An explicitly empty selector matches everything. That is a real
		// choice a cluster admin can make, so it is honoured -- the
		// protected-namespace filter below is what keeps it survivable.
		selector = labels.Everything()
	}

	list := &corev1.NamespaceList{}
	if err := r.List(ctx, list, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("listing namespaces: %w", err)
	}

	names := make([]string, 0, len(list.Items))
	for _, ns := range list.Items {
		if r.isProtected(policy, ns.Name) {
			continue
		}
		names = append(names, ns.Name)
	}
	sort.Strings(names)
	return names, nil
}

// isProtected reports whether a namespace is off limits. Cluster
// infrastructure is protected unconditionally: a label typo on kube-system
// should not be able to scale the control plane's add-ons to zero.
func (r *ReclaimPolicyReconciler) isProtected(policy *fallowv1alpha1.ReclaimPolicy, ns string) bool {
	if strings.HasPrefix(ns, "kube-") {
		return true
	}
	for _, p := range r.AlwaysProtected {
		if p == ns {
			return true
		}
	}
	for _, p := range policy.Spec.ProtectedNamespaces {
		if p == ns {
			return true
		}
	}
	return false
}

// shouldSkip reports workloads this policy must leave alone.
func (r *ReclaimPolicyReconciler) shouldSkip(policy *fallowv1alpha1.ReclaimPolicy, d *appsv1.Deployment) (bool, string) {
	if strings.EqualFold(d.Annotations[fallowv1alpha1.AnnotationExclude], "true") {
		return true, "excluded by " + fallowv1alpha1.AnnotationExclude
	}
	if !d.DeletionTimestamp.IsZero() {
		return true, "already being deleted"
	}
	// Two policies escalating the same workload would fight over its
	// annotations and each undo the other's bookkeeping. First claim wins,
	// and the claim is released when the workload goes back to Active.
	if owner, ok := d.Annotations[fallowv1alpha1.AnnotationPolicy]; ok && owner != policy.Name {
		return true, "claimed by policy " + owner
	}
	return false, ""
}

// writeStatus publishes the recomputed status.
func (r *ReclaimPolicyReconciler) writeStatus(ctx context.Context, policy *fallowv1alpha1.ReclaimPolicy, status fallowv1alpha1.ReclaimPolicyStatus) error {
	policy.Status = status
	if err := r.Status().Update(ctx, policy); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			// A conflict just means someone else wrote first; the next pass
			// recomputes from scratch anyway.
			return nil
		}
		return fmt.Errorf("updating status of %s: %w", policy.Name, err)
	}
	return nil
}

func (r *ReclaimPolicyReconciler) setReady(status *fallowv1alpha1.ReclaimPolicyStatus, s metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             s,
		Reason:             reason,
		Message:            truncate(message, 32768),
		ObservedGeneration: status.ObservedGeneration,
		LastTransitionTime: metav1.NewTime(r.now()),
	})
}

func (r *ReclaimPolicyReconciler) now() time.Time {
	if r.Clock == nil {
		return time.Now()
	}
	return r.Clock.Now()
}

func (r *ReclaimPolicyReconciler) resync() time.Duration {
	if r.ResyncPeriod <= 0 {
		return DefaultResyncPeriod
	}
	return r.ResyncPeriod
}

// SetupWithManager wires the controller into the manager.
func (r *ReclaimPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fallowv1alpha1.ReclaimPolicy{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.policiesForWorkload)).
		Named("reclaimpolicy").
		Complete(r)
}

// policiesForWorkload wakes every policy when a workload changes. Deciding
// which policies actually match would mean re-running both selectors here,
// and policies are few while the reconcile that follows is cheap and
// idempotent -- so the blunt version is the cheaper one overall.
func (r *ReclaimPolicyReconciler) policiesForWorkload(ctx context.Context, _ client.Object) []reconcile.Request {
	policies := &fallowv1alpha1.ReclaimPolicyList{}
	if err := r.List(ctx, policies); err != nil {
		log.FromContext(ctx).Error(err, "listing policies for workload event")
		return nil
	}
	requests := make([]reconcile.Request, 0, len(policies.Items))
	for i := range policies.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&policies.Items[i]),
		})
	}
	return requests
}

func indexTargets(targets []fallowv1alpha1.TargetStatus) map[string]*fallowv1alpha1.TargetStatus {
	index := make(map[string]*fallowv1alpha1.TargetStatus, len(targets))
	for i := range targets {
		index[key(targets[i].Namespace, targets[i].Name)] = &targets[i]
	}
	return index
}

// carryForwardDeleted keeps the records of workloads that no longer exist,
// as long as their namespace is still enrolled.
func carryForwardDeleted(previous map[string]*fallowv1alpha1.TargetStatus, current []fallowv1alpha1.TargetStatus, namespaces []string) []fallowv1alpha1.TargetStatus {
	seen := make(map[string]struct{}, len(current))
	for _, t := range current {
		seen[key(t.Namespace, t.Name)] = struct{}{}
	}
	enrolled := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		enrolled[ns] = struct{}{}
	}

	var kept []fallowv1alpha1.TargetStatus
	for k, t := range previous {
		if _, ok := seen[k]; ok {
			continue
		}
		if t.Stage != fallowv1alpha1.StageDeleted {
			// Not deleted and not found: it was removed by someone else, or
			// stopped matching the selector. Either way it is no longer this
			// policy's business.
			continue
		}
		if _, ok := enrolled[t.Namespace]; !ok {
			continue
		}
		kept = append(kept, *t)
	}
	return kept
}

func countLive(targets []fallowv1alpha1.TargetStatus) int32 {
	var n int32
	for _, t := range targets {
		if t.Stage != fallowv1alpha1.StageDeleted {
			n++
		}
	}
	return n
}

func countReclaimed(targets []fallowv1alpha1.TargetStatus) int32 {
	var n int32
	for _, t := range targets {
		if t.Stage == fallowv1alpha1.StageScaledToZero || t.Stage == fallowv1alpha1.StageDeleted {
			n++
		}
	}
	return n
}

func sortTargets(targets []fallowv1alpha1.TargetStatus) {
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Namespace != targets[j].Namespace {
			return targets[i].Namespace < targets[j].Namespace
		}
		return targets[i].Name < targets[j].Name
	})
}

func key(namespace, name string) string { return namespace + "/" + name }

func archiveNameFor(workload string) string { return archive.ArchiveName(workload) }

func ignorable(err error) bool { return apierrors.IsNotFound(err) }

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
