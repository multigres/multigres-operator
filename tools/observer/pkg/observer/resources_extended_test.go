package observer

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

func TestCheckPVCValidation(t *testing.T) {
	ctx := context.Background()

	boundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-pool-0",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}

	pendingPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-pool-1",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimPending,
		},
	}

	podWithBoundPVC := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-pod-0",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentPool,
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data-pool-0",
					},
				},
			}},
			Containers: []corev1.Container{{Name: "postgres", Image: "pg:16"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	podWithPendingPVC := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-pod-1",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentPool,
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data-pool-1",
					},
				},
			}},
			Containers: []corev1.Container{{Name: "postgres", Image: "pg:16"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	podWithMissingPVC := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-pod-2",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentPool,
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "data-pool-nonexistent",
					},
				},
			}},
			Containers: []corev1.Container{{Name: "postgres", Image: "pg:16"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	t.Run("bound PVC no findings", func(t *testing.T) {
		o := newTestObserver(boundPVC, podWithBoundPVC)
		o.checkPVCValidation(ctx)
		findings := collectFindings(o)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings for bound PVC, got %d: %+v", len(findings), findings)
		}
	})

	t.Run("pending PVC reports error", func(t *testing.T) {
		o := newTestObserver(pendingPVC, podWithPendingPVC)
		o.checkPVCValidation(ctx)
		findings := collectFindings(o)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding for pending PVC, got %d", len(findings))
		}
		if findings[0].Severity != report.SeverityError {
			t.Errorf("expected error, got %s", findings[0].Severity)
		}
	})

	t.Run("missing PVC reports fatal", func(t *testing.T) {
		o := newTestObserver(podWithMissingPVC)
		o.checkPVCValidation(ctx)
		findings := collectFindings(o)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding for missing PVC, got %d", len(findings))
		}
		if findings[0].Severity != report.SeverityFatal {
			t.Errorf("expected fatal, got %s", findings[0].Severity)
		}
	})
}

func TestCheckServiceEndpoints(t *testing.T) {
	ctx := context.Background()

	svcWithEndpoints := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-svc",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentMultigateway,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.1",
		},
	}

	endpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gw-svc",
			Namespace: "test-ns",
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.1.0.1"}},
		}},
	}

	svcNoEndpoints := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan-svc",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentMultiorch,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.2",
		},
	}

	emptyEndpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-svc",
			Namespace: "test-ns",
		},
		Subsets: []corev1.EndpointSubset{},
	}

	svcEmptyEndpoints := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-svc",
			Namespace: "test-ns",
			Labels: map[string]string{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentMultigateway,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.3",
		},
	}

	t.Run("service with ready endpoints no findings", func(t *testing.T) {
		o := newTestObserver(svcWithEndpoints, endpoints)
		o.checkServiceEndpoints(ctx)
		findings := collectFindings(o)
		if len(findings) != 0 {
			t.Errorf("expected 0 findings, got %d: %+v", len(findings), findings)
		}
	})

	t.Run("service without endpoints warns", func(t *testing.T) {
		o := newTestObserver(svcNoEndpoints)
		o.checkServiceEndpoints(ctx)
		findings := collectFindings(o)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding for missing endpoints, got %d", len(findings))
		}
		if findings[0].Severity != report.SeverityWarn {
			t.Errorf("expected warn, got %s", findings[0].Severity)
		}
	})

	t.Run("service with zero ready endpoints warns", func(t *testing.T) {
		o := newTestObserver(svcEmptyEndpoints, emptyEndpoints)
		o.checkServiceEndpoints(ctx)
		findings := collectFindings(o)
		if len(findings) != 1 {
			t.Fatalf("expected 1 finding for empty endpoints, got %d", len(findings))
		}
		if findings[0].Severity != report.SeverityWarn {
			t.Errorf("expected warn, got %s", findings[0].Severity)
		}
	})
}
