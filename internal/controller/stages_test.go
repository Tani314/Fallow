package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
)

func spec(idleAfter time.Duration, stages fallowv1alpha1.ReclaimStages) fallowv1alpha1.ReclaimPolicySpec {
	return fallowv1alpha1.ReclaimPolicySpec{
		IdleAfter: metav1.Duration{Duration: idleAfter},
		Stages:    stages,
	}
}

func hours(n int) time.Duration { return time.Duration(n) * time.Hour }

// TestStageAt pins the cumulative deadline arithmetic that lets a single
// idle-since timestamp rebuild the whole escalation state.
func TestStageAt(t *testing.T) {
	full := spec(hours(1), fallowv1alpha1.ReclaimStages{
		Notify:      &fallowv1alpha1.NotifyStage{},
		ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: hours(2)}},
		Delete:      &fallowv1alpha1.DeleteStage{After: metav1.Duration{Duration: hours(24)}},
	})

	for _, tc := range []struct {
		name      string
		spec      fallowv1alpha1.ReclaimPolicySpec
		elapsed   time.Duration
		want      fallowv1alpha1.Stage
		wantUntil time.Duration
	}{
		{"before the first deadline", full, hours(0), fallowv1alpha1.StageActive, hours(1)},
		{"one minute short of notify", full, hours(1) - time.Minute, fallowv1alpha1.StageActive, time.Minute},
		{"exactly at notify", full, hours(1), fallowv1alpha1.StageNotified, hours(2)},
		{"midway through the notice period", full, hours(2), fallowv1alpha1.StageNotified, hours(1)},
		{"at the scale deadline", full, hours(3), fallowv1alpha1.StageScaledToZero, hours(24)},
		{"one hour short of deletion", full, hours(26), fallowv1alpha1.StageScaledToZero, hours(1)},
		{"at the delete deadline", full, hours(27), fallowv1alpha1.StageDeleted, 0},
		{"long past every deadline", full, hours(900), fallowv1alpha1.StageDeleted, 0},
		{
			// A ladder with no notify stage is a disabled policy, however
			// long a workload has been idle.
			name: "no notify stage disables the policy",
			spec: spec(hours(1), fallowv1alpha1.ReclaimStages{
				ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: hours(2)}},
			}),
			elapsed: hours(900),
			want:    fallowv1alpha1.StageActive,
		},
		{
			name: "notify only stops at Notified",
			spec: spec(hours(1), fallowv1alpha1.ReclaimStages{
				Notify: &fallowv1alpha1.NotifyStage{},
			}),
			elapsed: hours(900),
			want:    fallowv1alpha1.StageNotified,
		},
		{
			name: "no delete stage stops at zero replicas",
			spec: spec(hours(1), fallowv1alpha1.ReclaimStages{
				Notify:      &fallowv1alpha1.NotifyStage{},
				ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: hours(2)}},
			}),
			elapsed: hours(900),
			want:    fallowv1alpha1.StageScaledToZero,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, until := stageAt(tc.spec, tc.elapsed)
			if got != tc.want {
				t.Fatalf("stage = %s, want %s", got, tc.want)
			}
			if until != tc.wantUntil {
				t.Fatalf("until next deadline = %s, want %s", until, tc.wantUntil)
			}
		})
	}
}

// TestNextStageClimbsOneRung checks escalation is rate-limited upward while
// falling back is immediate.
func TestNextStageClimbsOneRung(t *testing.T) {
	for _, tc := range []struct {
		current, target, want fallowv1alpha1.Stage
	}{
		{fallowv1alpha1.StageActive, fallowv1alpha1.StageDeleted, fallowv1alpha1.StageNotified},
		{fallowv1alpha1.StageNotified, fallowv1alpha1.StageDeleted, fallowv1alpha1.StageScaledToZero},
		{fallowv1alpha1.StageScaledToZero, fallowv1alpha1.StageDeleted, fallowv1alpha1.StageDeleted},
		{fallowv1alpha1.StageActive, fallowv1alpha1.StageActive, fallowv1alpha1.StageActive},
		// Falling back skips no rungs in the other direction.
		{fallowv1alpha1.StageScaledToZero, fallowv1alpha1.StageActive, fallowv1alpha1.StageActive},
		{fallowv1alpha1.StageDeleted, fallowv1alpha1.StageActive, fallowv1alpha1.StageActive},
	} {
		if got := nextStage(tc.current, tc.target); got != tc.want {
			t.Errorf("nextStage(%s, %s) = %s, want %s", tc.current, tc.target, got, tc.want)
		}
	}
}

// TestEnteredAtIsDerivedNotObserved checks the status reports when a
// transition was due, not when a sleepy controller noticed it.
func TestEnteredAtIsDerivedNotObserved(t *testing.T) {
	idleSince := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	s := spec(hours(1), fallowv1alpha1.ReclaimStages{
		Notify:      &fallowv1alpha1.NotifyStage{},
		ScaleToZero: &fallowv1alpha1.ScaleToZeroStage{After: metav1.Duration{Duration: hours(2)}},
		Delete:      &fallowv1alpha1.DeleteStage{After: metav1.Duration{Duration: hours(24)}},
	})

	for _, tc := range []struct {
		stage  fallowv1alpha1.Stage
		offset time.Duration
	}{
		{fallowv1alpha1.StageActive, 0},
		{fallowv1alpha1.StageNotified, hours(1)},
		{fallowv1alpha1.StageScaledToZero, hours(3)},
		{fallowv1alpha1.StageDeleted, hours(27)},
	} {
		want := idleSince.Add(tc.offset)
		if got := enteredAt(s, idleSince, tc.stage); !got.Equal(want) {
			t.Errorf("enteredAt(%s) = %s, want %s", tc.stage, got, want)
		}
	}
}
