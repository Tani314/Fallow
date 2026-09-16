// Package controller reconciles ReclaimPolicy objects against the workloads
// they govern.
package controller

import (
	"time"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
)

// rank orders the escalation ladder. Comparing ranks is how the reconciler
// tells "climbing" from "falling back", which is the distinction the whole
// controller turns on: climbing is rate-limited to one rung per pass, while
// falling back to Active is always immediate.
func rank(s fallowv1alpha1.Stage) int {
	switch s {
	case fallowv1alpha1.StageNotified:
		return 1
	case fallowv1alpha1.StageScaledToZero:
		return 2
	case fallowv1alpha1.StageDeleted:
		return 3
	default:
		return 0
	}
}

// stageAt reports the furthest rung a workload idle for elapsed has earned,
// and how long until the rung after that comes due.
//
// Deadlines are cumulative from the moment the workload was first seen idle,
// which is why a single idle-since annotation is enough to rebuild the whole
// escalation state after a controller restart. Nothing about a workload's
// position on the ladder lives only in the controller's memory.
//
// A missing stage truncates the ladder: a policy with Notify but no
// ScaleToZero can never climb past Notified, however long a workload sits
// there. That makes the safe configuration the one you get by writing less.
func stageAt(spec fallowv1alpha1.ReclaimPolicySpec, elapsed time.Duration) (fallowv1alpha1.Stage, time.Duration) {
	stages := spec.Stages
	if stages.Notify == nil {
		return fallowv1alpha1.StageActive, 0
	}

	deadline := spec.IdleAfter.Duration
	if elapsed < deadline {
		return fallowv1alpha1.StageActive, deadline - elapsed
	}

	if stages.ScaleToZero == nil {
		return fallowv1alpha1.StageNotified, 0
	}
	deadline += stages.ScaleToZero.After.Duration
	if elapsed < deadline {
		return fallowv1alpha1.StageNotified, deadline - elapsed
	}

	if stages.Delete == nil {
		return fallowv1alpha1.StageScaledToZero, 0
	}
	deadline += stages.Delete.After.Duration
	if elapsed < deadline {
		return fallowv1alpha1.StageScaledToZero, deadline - elapsed
	}

	return fallowv1alpha1.StageDeleted, 0
}

// enteredAt returns when a workload idle since idleSince reached stage. The
// status reports a derived timestamp rather than the wall-clock moment the
// controller noticed, so a controller that was asleep for an hour does not
// claim the transition happened when it woke up.
func enteredAt(spec fallowv1alpha1.ReclaimPolicySpec, idleSince time.Time, stage fallowv1alpha1.Stage) time.Time {
	offset := time.Duration(0)
	if rank(stage) >= rank(fallowv1alpha1.StageNotified) {
		offset += spec.IdleAfter.Duration
	}
	if rank(stage) >= rank(fallowv1alpha1.StageScaledToZero) && spec.Stages.ScaleToZero != nil {
		offset += spec.Stages.ScaleToZero.After.Duration
	}
	if rank(stage) >= rank(fallowv1alpha1.StageDeleted) && spec.Stages.Delete != nil {
		offset += spec.Stages.Delete.After.Duration
	}
	return idleSince.Add(offset)
}

// nextStage returns the rung to move to now. Escalation advances one rung
// per pass even when several deadlines have already passed, so a controller
// that comes back from an outage to find every deadline expired still walks
// the workload down the ladder instead of jumping from Active to Deleted.
// Each rung announces itself, and -- the part that matters for recovery --
// the replica count is captured on the way through ScaledToZero, so the
// archive written at deletion knows what size to restore.
func nextStage(current, target fallowv1alpha1.Stage) fallowv1alpha1.Stage {
	if rank(target) <= rank(current) {
		return target
	}
	switch current {
	case fallowv1alpha1.StageActive:
		return fallowv1alpha1.StageNotified
	case fallowv1alpha1.StageNotified:
		return fallowv1alpha1.StageScaledToZero
	default:
		return fallowv1alpha1.StageDeleted
	}
}
