package toposerver

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestBuildPodDisruptionBudget(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
	}

	got, err := BuildPodDisruptionBudget(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildPodDisruptionBudget() error = %v", err)
	}

	labels := metadata.BuildStandardLabels("test-cluster", ComponentName)
	metadata.AddClusterLabel(labels, "test-cluster")
	want := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "multigres.com/v1alpha1",
					Kind:               "TopoServer",
					Name:               "test-toposerver",
					UID:                "test-uid",
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: ptr.To(intstr.FromInt32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(labels),
			},
			UnhealthyPodEvictionPolicy: ptr.To(policyv1.AlwaysAllow),
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("BuildPodDisruptionBudget() mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildPodDisruptionBudgetInvalidScheme(t *testing.T) {
	_, err := BuildPodDisruptionBudget(
		&multigresv1alpha1.TopoServer{ObjectMeta: metav1.ObjectMeta{Name: "test"}},
		runtime.NewScheme(),
	)
	if err == nil {
		t.Fatal("BuildPodDisruptionBudget() should fail with an unregistered scheme")
	}
}
