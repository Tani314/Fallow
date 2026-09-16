// Package v1alpha1 contains the Fallow API types.
//
// +kubebuilder:object:generate=true
// +groupName=fallow.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version for all types in this package.
	GroupVersion = schema.GroupVersion{Group: "fallow.dev", Version: "v1alpha1"}

	// SchemeBuilder registers these types with a runtime.Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this package to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
