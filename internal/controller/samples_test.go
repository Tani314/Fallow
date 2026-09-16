package controller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	fallowv1alpha1 "github.com/Tani314/Fallow/api/v1alpha1"
)

// TestSamplesDecodeStrictly checks the shipped samples are valid against the
// API type -- a sample with a typo teaches the typo.
func TestSamplesDecodeStrictly(t *testing.T) {
	paths, err := filepath.Glob("../../config/samples/fallow_v1alpha1_*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no samples found: %v", err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading: %v", err)
			}
			p := &fallowv1alpha1.ReclaimPolicy{}
			if err := yaml.UnmarshalStrict(raw, p); err != nil {
				t.Fatalf("does not decode into a ReclaimPolicy: %v", err)
			}
			if p.APIVersion != "fallow.dev/v1alpha1" || p.Kind != "ReclaimPolicy" {
				t.Fatalf("apiVersion/kind = %s/%s", p.APIVersion, p.Kind)
			}
			if p.Spec.IdleAfter.Duration <= 0 {
				t.Errorf("idleAfter did not parse: %v", p.Spec.IdleAfter)
			}
			if p.Spec.Stages.Notify == nil {
				t.Error("no notify stage, so this sample policy is inert")
			}
			if strings.Contains(path, "ephemeral") {
				if p.Spec.Stages.Delete == nil || p.Spec.Stages.Delete.Archive == nil || *p.Spec.Stages.Delete.Archive {
					t.Error("the ephemeral sample is meant to show archive: false")
				}
			}
		})
	}
}
