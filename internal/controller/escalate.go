package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
	"github.com/Tani314/Fallow/internal/idle"
)

// climbRequeue is how soon to look again when a workload has more rungs to
// climb than this pass is willing to take.
const climbRequeue = time.Second

// reconcileWorkload walks one workload one step along the ladder and reports
// where it ended up, plus how long until it needs looking at again.
func (r *ReclaimPolicyReconciler) reconcileWorkload(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	prev *fallowv1alpha1.TargetStatus,
) (fallowv1alpha1.TargetStatus, time.Duration, error) {
	now := r.now()
	current := currentStage(d, prev)

	verdict, err := r.Detector.Probe(ctx, d)
	if err != nil {
		return fallowv1alpha1.TargetStatus{}, 0, fmt.Errorf("probing %s/%s: %w", d.Namespace, d.Name, err)
	}

	target := fallowv1alpha1.TargetStatus{
		Namespace:          d.Namespace,
		Name:               d.Name,
		Kind:               "Deployment",
		Stage:              current,
		LastTransitionTime: metav1.NewTime(now),
	}
	if prev != nil {
		target.LastTransitionTime = prev.LastTransitionTime
		target.OriginalReplicas = prev.OriginalReplicas
	}

	// Activity beats every deadline. A workload that looks busy falls all
	// the way back to Active in one pass, however far it had climbed --
	// there is no partial descent, because a workload someone is using
	// should not still be one tick away from being scaled down.
	if !verdict.Idle {
		if current != fallowv1alpha1.StageActive {
			if err := r.restore(ctx, policy, d, current, prev); err != nil {
				return fallowv1alpha1.TargetStatus{}, 0, err
			}
			target.LastTransitionTime = metav1.NewTime(now)
		}
		target.Stage = fallowv1alpha1.StageActive
		target.IdleSince = nil
		target.OriginalReplicas = nil
		target.Reason = verdict.Reason
		return target, r.resync(), nil
	}

	idleSince, err := r.resolveIdleSince(ctx, policy, d, prev, now)
	if err != nil {
		return fallowv1alpha1.TargetStatus{}, 0, err
	}
	target.IdleSince = ptr(metav1.NewTime(idleSince))

	elapsed := now.Sub(idleSince)
	due, until := stageAt(policy.Spec, elapsed)
	next := nextStage(current, due)

	if next == current {
		target.Reason = fmt.Sprintf("idle %s; %s", round(elapsed), verdict.Reason)
		if until <= 0 {
			// The ladder is truncated here: nothing further is configured,
			// so there is no deadline to wake up for.
			return target, r.resync(), nil
		}
		return target, until, nil
	}

	replicas, archived, err := r.advance(ctx, policy, d, next, verdict, target.OriginalReplicas, elapsed)
	if err != nil {
		return fallowv1alpha1.TargetStatus{}, 0, err
	}

	target.Stage = next
	target.LastTransitionTime = metav1.NewTime(enteredAt(policy.Spec, idleSince, next))
	if replicas != nil {
		target.OriginalReplicas = replicas
	}
	target.Reason = stageReason(next, elapsed, verdict, archived)

	if next != due {
		return target, climbRequeue, nil
	}
	if until <= 0 {
		return target, r.resync(), nil
	}
	return target, until, nil
}

// advance performs the mutation for one rung. It returns the replica count
// captured on the way to zero, and the archive name written on the way to
// deletion, so the caller can record both in status.
func (r *ReclaimPolicyReconciler) advance(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	to fallowv1alpha1.Stage,
	verdict idle.Verdict,
	known *int32,
	elapsed time.Duration,
) (*int32, string, error) {
	switch to {
	case fallowv1alpha1.StageNotified:
		message := "Fallow will begin reclaiming this workload unless it becomes active"
		if policy.Spec.Stages.Notify != nil && policy.Spec.Stages.Notify.Message != "" {
			message = policy.Spec.Stages.Notify.Message
		}
		if err := r.markStage(ctx, policy, d, to, nil); err != nil {
			return nil, "", err
		}
		r.event(policy, d, corev1.EventTypeNormal, "IdleDetected",
			"%s (policy %s: %s)", message, policy.Name, verdict.Reason)
		return nil, "", nil

	case fallowv1alpha1.StageScaledToZero:
		replicas := known
		if d.Spec.Replicas != nil && *d.Spec.Replicas > 0 {
			replicas = ptr(*d.Spec.Replicas)
		}
		if err := r.markStage(ctx, policy, d, to, replicas); err != nil {
			return nil, "", err
		}
		r.event(policy, d, corev1.EventTypeNormal, "ScaledToZero",
			"scaled to 0 replicas (was %s) after %s idle; policy %s",
			describeReplicas(replicas), round(elapsed), policy.Name)
		return replicas, "", nil

	case fallowv1alpha1.StageDeleted:
		archived := ""
		if archiveEnabled(policy) {
			name, err := r.archiveWorkload(ctx, policy, d)
			if err != nil {
				// Refusing to delete an unarchivable workload is the whole
				// bargain of the delete stage: reversibility is not a
				// best-effort extra, so a failed archive stops the deletion.
				return nil, "", fmt.Errorf("archiving %s/%s before delete: %w", d.Namespace, d.Name, err)
			}
			archived = name
		}
		// Emitted before the delete: the object has to exist for the event
		// to have anything to attach to.
		if archived != "" {
			r.event(policy, d, corev1.EventTypeWarning, "Deleted",
				"deleted after %s idle; manifest archived in ConfigMap %q (policy %s)",
				round(elapsed), archived, policy.Name)
		} else {
			r.event(policy, d, corev1.EventTypeWarning, "Deleted",
				"deleted after %s idle; archiving disabled, this is not reversible (policy %s)",
				round(elapsed), policy.Name)
		}
		if !policy.Spec.DryRun {
			if err := r.Delete(ctx, d); err != nil && !ignorable(err) {
				return nil, "", fmt.Errorf("deleting %s/%s: %w", d.Namespace, d.Name, err)
			}
		}
		return nil, archived, nil
	}
	return nil, "", nil
}

// restore returns a workload to service. Scaling back up uses the count
// Fallow saved on the way down, never a guess: if no count was recorded --
// because the workload was already at zero when Fallow found it -- the
// replica count is left alone rather than invented.
func (r *ReclaimPolicyReconciler) restore(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	from fallowv1alpha1.Stage,
	prev *fallowv1alpha1.TargetStatus,
) error {
	replicas := originalReplicas(d, prev)

	if !policy.Spec.DryRun {
		patch := client.MergeFrom(d.DeepCopy())
		delete(d.Annotations, fallowv1alpha1.AnnotationStage)
		delete(d.Annotations, fallowv1alpha1.AnnotationIdleSince)
		delete(d.Annotations, fallowv1alpha1.AnnotationOriginalReplicas)
		delete(d.Annotations, fallowv1alpha1.AnnotationPolicy)
		if from == fallowv1alpha1.StageScaledToZero && replicas != nil {
			d.Spec.Replicas = replicas
		}
		if err := r.Patch(ctx, d, patch); err != nil {
			return fmt.Errorf("restoring %s/%s: %w", d.Namespace, d.Name, err)
		}
	}

	if from == fallowv1alpha1.StageScaledToZero {
		r.event(policy, d, corev1.EventTypeNormal, "Restored",
			"workload is active again; restored to %s replicas (policy %s)",
			describeReplicas(replicas), policy.Name)
	} else {
		r.event(policy, d, corev1.EventTypeNormal, "Restored",
			"workload is active again; escalation cancelled at stage %s (policy %s)", from, policy.Name)
	}
	return nil
}

// markStage writes the controller's durable memory onto the workload: the
// rung it is on, when it went idle, which policy claimed it, and what to
// scale back to. Everything needed to reverse an action lives on the object,
// so a controller restart -- or a handover to a different controller -- loses
// nothing.
func (r *ReclaimPolicyReconciler) markStage(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	stage fallowv1alpha1.Stage,
	replicas *int32,
) error {
	if policy.Spec.DryRun {
		return nil
	}

	patch := client.MergeFrom(d.DeepCopy())
	if d.Annotations == nil {
		d.Annotations = map[string]string{}
	}
	d.Annotations[fallowv1alpha1.AnnotationStage] = string(stage)
	d.Annotations[fallowv1alpha1.AnnotationPolicy] = policy.Name
	if replicas != nil {
		d.Annotations[fallowv1alpha1.AnnotationOriginalReplicas] = strconv.FormatInt(int64(*replicas), 10)
	}
	if stage == fallowv1alpha1.StageScaledToZero {
		d.Spec.Replicas = ptr(int32(0))
	}
	if err := r.Patch(ctx, d, patch); err != nil {
		return fmt.Errorf("marking %s/%s as %s: %w", d.Namespace, d.Name, stage, err)
	}
	return nil
}

// archiveWorkload stores the manifest and records where it went.
func (r *ReclaimPolicyReconciler) archiveWorkload(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
) (string, error) {
	if r.Archiver == nil {
		return "", fmt.Errorf("archiving requested but no archiver is configured")
	}
	if policy.Spec.DryRun {
		// Nothing is written, but the name is deterministic, so a dry run
		// can still report exactly where the manifest would have landed.
		return archiveNameFor(d.Name), nil
	}
	return r.Archiver.Archive(ctx, d)
}

// resolveIdleSince finds when this workload was first seen idle, recording
// the answer on the object the first time so later passes agree with this
// one even across a controller restart.
func (r *ReclaimPolicyReconciler) resolveIdleSince(
	ctx context.Context,
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	prev *fallowv1alpha1.TargetStatus,
	now time.Time,
) (time.Time, error) {
	if raw, ok := d.Annotations[fallowv1alpha1.AnnotationIdleSince]; ok {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t, nil
		}
		log.FromContext(ctx).Info("ignoring unparseable idle-since annotation",
			"workload", d.Namespace+"/"+d.Name, "value", raw)
	}
	// A dry run never writes to the workload, so its only memory is the
	// policy's own status. That is enough for escalation to progress
	// exactly as it would live, which is the point of a dry run.
	if prev != nil && prev.IdleSince != nil {
		return prev.IdleSince.Time, nil
	}

	if !policy.Spec.DryRun {
		patch := client.MergeFrom(d.DeepCopy())
		if d.Annotations == nil {
			d.Annotations = map[string]string{}
		}
		d.Annotations[fallowv1alpha1.AnnotationIdleSince] = now.UTC().Format(time.RFC3339)
		d.Annotations[fallowv1alpha1.AnnotationPolicy] = policy.Name
		if err := r.Patch(ctx, d, patch); err != nil {
			return time.Time{}, fmt.Errorf("recording idle-since on %s/%s: %w", d.Namespace, d.Name, err)
		}
	}
	return now, nil
}

// event emits on the workload, and mirrors reclamation onto the policy so
// the audit trail survives the workload it describes.
func (r *ReclaimPolicyReconciler) event(
	policy *fallowv1alpha1.ReclaimPolicy,
	d *appsv1.Deployment,
	eventType, reason, format string,
	args ...any,
) {
	if r.Recorder == nil {
		return
	}
	message := fmt.Sprintf(format, args...)
	if policy.Spec.DryRun {
		reason = "DryRun" + reason
		message = "(dry run, no changes made) " + message
	}
	r.Recorder.Event(d, eventType, reason, message)
	r.Recorder.Event(policy, eventType, reason,
		fmt.Sprintf("%s/%s: %s", d.Namespace, d.Name, message))
}

// currentStage reads the rung a workload is on, preferring the annotation on
// the object over the policy's status: the object is the source of truth,
// because it is what survives a controller that loses its status writes.
func currentStage(d *appsv1.Deployment, prev *fallowv1alpha1.TargetStatus) fallowv1alpha1.Stage {
	switch fallowv1alpha1.Stage(d.Annotations[fallowv1alpha1.AnnotationStage]) {
	case fallowv1alpha1.StageNotified:
		return fallowv1alpha1.StageNotified
	case fallowv1alpha1.StageScaledToZero:
		return fallowv1alpha1.StageScaledToZero
	case fallowv1alpha1.StageDeleted:
		return fallowv1alpha1.StageDeleted
	case fallowv1alpha1.StageActive:
		return fallowv1alpha1.StageActive
	}
	if prev != nil {
		return prev.Stage
	}
	return fallowv1alpha1.StageActive
}

// originalReplicas recovers the pre-reclamation replica count.
func originalReplicas(d *appsv1.Deployment, prev *fallowv1alpha1.TargetStatus) *int32 {
	if raw, ok := d.Annotations[fallowv1alpha1.AnnotationOriginalReplicas]; ok {
		if n, err := strconv.ParseInt(raw, 10, 32); err == nil {
			return ptr(int32(n))
		}
	}
	if prev != nil && prev.OriginalReplicas != nil {
		return ptr(*prev.OriginalReplicas)
	}
	return nil
}

func stageReason(stage fallowv1alpha1.Stage, elapsed time.Duration, verdict idle.Verdict, archived string) string {
	switch stage {
	case fallowv1alpha1.StageNotified:
		return fmt.Sprintf("notified after %s idle; %s", round(elapsed), verdict.Reason)
	case fallowv1alpha1.StageScaledToZero:
		return fmt.Sprintf("scaled to zero after %s idle", round(elapsed))
	case fallowv1alpha1.StageDeleted:
		if archived != "" {
			return fmt.Sprintf("deleted after %s idle; archived in ConfigMap %q", round(elapsed), archived)
		}
		return fmt.Sprintf("deleted after %s idle; not archived", round(elapsed))
	default:
		return verdict.Reason
	}
}

func archiveEnabled(policy *fallowv1alpha1.ReclaimPolicy) bool {
	del := policy.Spec.Stages.Delete
	// The CRD defaults Archive to true; a policy built in Go without going
	// through defaulting gets the same answer here.
	return del != nil && (del.Archive == nil || *del.Archive)
}

func describeReplicas(r *int32) string {
	if r == nil {
		return "an unrecorded number of"
	}
	return strconv.FormatInt(int64(*r), 10)
}

func round(d time.Duration) time.Duration { return d.Round(time.Second) }

func ptr[T any](v T) *T { return &v }
