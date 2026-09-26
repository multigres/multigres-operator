package testutil_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

func TestComparisonOptions(t *testing.T) {
	t.Parallel()
	now := time.Now()

	tests := map[string]struct {
		obj1    any
		obj2    any
		options cmp.Options
	}{
		"IgnoreMetaRuntimeFields": {
			obj1: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc",
					Namespace: "default",
					// Runtime fields differ - should be ignored
					UID:               "uid1",
					ResourceVersion:   "123",
					CreationTimestamp: metav1.Time{Time: time.Now()},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
				},
			},
			obj2: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc",
					Namespace: "default",
					// Runtime fields differ - should be ignored
					UID:               "different-uid",
					ResourceVersion:   "999",
					CreationTimestamp: metav1.Time{Time: time.Now().Add(1 * time.Hour)},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP, // Must match
				},
			},
			options: testutil.IgnoreMetaRuntimeFields(),
		},
		"IgnoreStatus": {
			obj1: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "1.2.3.4"}},
					},
				},
			},
			obj2: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "5.6.7.8"}},
					},
				},
			},
			options: cmp.Options{testutil.IgnoreStatus()},
		},
		"IgnoreObjectMetaCompletely": {
			obj1: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc1",
					Namespace: "ns1",
					UID:       "uid1",
					Labels:    map[string]string{"foo": "bar"},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			obj2: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "completely-different",
					Namespace: "ns2",
					UID:       "uid2",
					Labels:    map[string]string{"different": "labels"},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			options: cmp.Options{testutil.IgnoreObjectMetaCompletely()},
		},
		"CompareOptions": {
			obj1: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "svc",
					UID:               "uid1",
					ResourceVersion:   "123",
					CreationTimestamp: metav1.Time{Time: now},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "1.2.3.4"}},
					},
				},
			},
			obj2: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "svc",
					UID:               "uid2",
					ResourceVersion:   "999",
					CreationTimestamp: metav1.Time{Time: now.Add(1 * time.Hour)},
				},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "5.6.7.8"}},
					},
				},
			},
			options: testutil.CompareOptions(),
		},
		"CompareSpecOnly": {
			obj1: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc1",
					Namespace: "ns1",
					Labels:    map[string]string{"foo": "bar"},
				},
				Spec: corev1.ServiceSpec{
					Type:  corev1.ServiceTypeClusterIP,
					Ports: []corev1.ServicePort{{Name: "http", Port: 80}},
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "1.2.3.4"}},
					},
				},
			},
			obj2: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "different",
					Namespace: "ns2",
					Labels:    map[string]string{"different": "labels"},
				},
				Spec: corev1.ServiceSpec{
					Type:  corev1.ServiceTypeClusterIP,
					Ports: []corev1.ServicePort{{Name: "http", Port: 80}},
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{{IP: "9.9.9.9"}},
					},
				},
			},
			options: testutil.CompareSpecOnly(),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			diff := cmp.Diff(tc.obj1, tc.obj2, tc.options...)
			assert.NewCollecting(t).
				Eq("", diff, "%s should make objects match, but found diff:\n", name)
		})
	}
}

func TestIgnoreProbeDefaults(t *testing.T) {
	t.Parallel()

	pod1 := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "nginx",
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/healthz",
							Port:   intstr.FromInt32(8080),
							Scheme: "HTTP",
						},
					},
					TimeoutSeconds:   1,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				},
			}},
		},
	}

	pod2 := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "nginx",
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/healthz",
							Port:   intstr.FromInt32(8080),
							Scheme: "HTTPS",
						},
					},
					TimeoutSeconds:   5,
					SuccessThreshold: 2,
					FailureThreshold: 10,
				},
			}},
		},
	}

	diff := cmp.Diff(pod1, pod2,
		testutil.IgnoreProbeDefaults(),
		testutil.IgnoreMetaRuntimeFields(),
		testutil.IgnorePodSpecDefaults(),
	)
	assert.NewCollecting(t).
		Eq("", diff, "IgnoreProbeDefaults should ignore probe defaults, but found diff:\n")
}

func TestIgnorePVCRuntimeFields(t *testing.T) {
	t.Parallel()

	blockMode := corev1.PersistentVolumeBlock
	fsMode := corev1.PersistentVolumeFilesystem

	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data-pvc",
			Namespace:  "default",
			Finalizers: []string{"kubernetes.io/pvc-protection"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  &blockMode,
		},
	}

	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  &fsMode,
		},
	}

	diff := cmp.Diff(pvc1, pvc2,
		testutil.IgnorePVCRuntimeFields(),
		testutil.IgnoreMetaRuntimeFields(),
	)
	assert.NewCollecting(t).
		Eq("", diff, "IgnorePVCRuntimeFields should ignore finalizers and VolumeMode, but found diff:\n")
}
