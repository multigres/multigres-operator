package ctrltest

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// relationsFakeClient builds a fake client over core types only. Unlike
// identityFakeClient it registers no CRDs, because pod-to-PVC lookups do
// not involve them.
func relationsFakeClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

func pvcVolume(name, claim string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claim,
			},
		},
	}
}

func podWithVolumes(ns, name string, volumes ...corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
			Volumes:    volumes,
		},
	}
}

func TestRelationsPVCOfReadsClaimFromVolumes(t *testing.T) {
	ctx := t.Context()
	p := podWithVolumes("ns1", "pod-a", pvcVolume("data-volume", "pod-a-data"))
	c := relationsFakeClient(p)

	claim, err := PVCOf(ctx, c, "ns1", "pod-a", "data-volume")
	if err != nil {
		t.Fatalf("PVCOf: %v", err)
	}
	if claim != "pod-a-data" {
		t.Errorf("PVCOf = %q, want pod-a-data", claim)
	}
}

// TestRelationsPVCOfReturnsDataVolumeAmongOthers pins the bug the reviewer
// found: a pod can carry a second PVC-backed volume alongside the one named
// by volumeName (here, a volume meant to represent a backup volume), and
// PVCOf must still return the named volume's PVC rather than refusing to
// answer because more than one PVC-backed volume exists.
func TestRelationsPVCOfReturnsDataVolumeAmongOthers(t *testing.T) {
	ctx := t.Context()
	p := podWithVolumes(
		"ns1", "pod-a",
		pvcVolume("data-volume", "pod-a-data"),
		pvcVolume("backup-data", "pod-a-backup"),
	)
	c := relationsFakeClient(p)

	claim, err := PVCOf(ctx, c, "ns1", "pod-a", "data-volume")
	if err != nil {
		t.Fatalf("PVCOf: %v", err)
	}
	if claim != "pod-a-data" {
		t.Errorf("PVCOf = %q, want pod-a-data", claim)
	}
}

// TestRelationsPVCOfErrorsWhenNoDataVolume covers the guard the implementer
// declared untested in round 1: a pod with PVC-backed volumes, none of them
// named by volumeName, must error loudly rather than silently return "".
func TestRelationsPVCOfErrorsWhenNoDataVolume(t *testing.T) {
	ctx := t.Context()
	p := podWithVolumes("ns1", "pod-a", pvcVolume("backup-data", "pod-a-backup"))
	c := relationsFakeClient(p)

	_, err := PVCOf(ctx, c, "ns1", "pod-a", "data-volume")
	if err == nil {
		t.Fatalf("PVCOf = nil error, want an error for a pod with no data volume")
	}
	noVolumeNamed := strings.Contains(err.Error(), "no volume named")
	namesDataVolume := strings.Contains(err.Error(), "data-volume")
	if !noVolumeNamed || !namesDataVolume {
		t.Errorf("error %q does not name the missing data volume", err.Error())
	}
}

// TestRelationsPVCOfErrorsWhenTwoVolumesShareDataVolumeName covers the
// "somehow claim that same name" half of the coordinator's ruling: two
// volumes both named by volumeName is not a shape any real caller builds, but
// PVCOf must still refuse to guess between them rather than picking one.
func TestRelationsPVCOfErrorsWhenTwoVolumesShareDataVolumeName(t *testing.T) {
	ctx := t.Context()
	p := podWithVolumes(
		"ns1", "pod-a",
		pvcVolume("data-volume", "pod-a-data-1"),
		pvcVolume("data-volume", "pod-a-data-2"),
	)
	c := relationsFakeClient(p)

	_, err := PVCOf(ctx, c, "ns1", "pod-a", "data-volume")
	if err == nil {
		t.Fatalf("PVCOf = nil error, want an error naming both PVCs")
	}
	namesFirst := strings.Contains(err.Error(), "pod-a-data-1")
	namesSecond := strings.Contains(err.Error(), "pod-a-data-2")
	if !namesFirst || !namesSecond {
		t.Errorf("error %q does not name both PVCs", err.Error())
	}
}

// TestRelationsPVCOfErrorsWhenDataVolumeIsNotPVCBacked pins the nil check in
// PVCOf's volume loop. Without it, a pod carrying a data-named volume that is
// not PVC-backed dereferences a nil PersistentVolumeClaim and panics, which in
// a harness helper reads as a suite crash rather than as a failed assertion.
func TestRelationsPVCOfErrorsWhenDataVolumeIsNotPVCBacked(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-0", Namespace: "ns"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name:         "data-volume",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}},
	}
	c := relationsFakeClient(pod)

	_, err := PVCOf(t.Context(), c, "ns", "pool-0", "data-volume")
	if err == nil {
		t.Fatal("PVCOf returned no error for a data volume that is not PVC-backed")
	}
	if !strings.Contains(err.Error(), "data-volume") {
		t.Fatalf("error does not name the data volume: %v", err)
	}
}

// TestRelationsPVCOfSelectsTheNamedVolumeNotTheFirst pins the volumeName
// parameter itself. An implementation that ignored it and hardcoded a volume
// name would pass every other test in this file.
func TestRelationsPVCOfSelectsTheNamedVolumeNotTheFirst(t *testing.T) {
	p := podWithVolumes(
		"ns1", "pod-a",
		pvcVolume("first-volume", "claim-first"),
		pvcVolume("second-volume", "claim-second"),
	)
	c := relationsFakeClient(p)

	claim, err := PVCOf(t.Context(), c, "ns1", "pod-a", "second-volume")
	if err != nil {
		t.Fatalf("PVCOf: %v", err)
	}
	if claim != "claim-second" {
		t.Errorf("PVCOf = %q, want claim-second", claim)
	}
}
