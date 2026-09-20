package suite

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
)

// identityFakeClient builds a fake client that knows about both the
// operator's CRDs and core types, since a Shard's status and a Pod's
// volumes both need to round-trip through it.
func identityFakeClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&Shard{}).
		Build()
}

func shardWithRoles(ns, name string, roles map[string]string) *Shard {
	return &Shard{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status: multigresv1alpha1.ShardStatus{
			PodRoles: roles,
		},
	}
}

func TestIdentityMembersOfClassifiesAndSortsReplicas(t *testing.T) {
	c := newBareCase(t)
	shard := shardWithRoles("ns1", "shard1", map[string]string{
		"pod-primary":   "PRIMARY",
		"pod-replica-b": "REPLICA",
		"pod-replica-a": "REPLICA",
	})
	fc := identityFakeClient(shard)

	members, err := MembersOf(c.Context(), fc, client.ObjectKeyFromObject(shard))
	c.NoError(err, "MembersOf")

	c.Check().Eq("pod-primary", members.Primary, "Primary")
	want := []string{"pod-replica-a", "pod-replica-b"}
	gotSorted := len(members.Replicas) == len(want) &&
		members.Replicas[0] == want[0] && members.Replicas[1] == want[1]
	c.Check().True(gotSorted, "Replicas = %v, want %v (sorted)", members.Replicas, want)
}

func TestIdentityMembersOfSeparatesQuarantinedFromReplicas(t *testing.T) {
	c := newBareCase(t)
	shard := shardWithRoles("ns1", "shard1", map[string]string{
		"pod-primary":     "PRIMARY",
		"pod-replica":     "REPLICA",
		"pod-quarantined": "QUARANTINED",
	})
	fc := identityFakeClient(shard)

	members, err := MembersOf(c.Context(), fc, client.ObjectKeyFromObject(shard))
	c.NoError(err, "MembersOf")

	c.Check().EqDiff([]string{"pod-replica"}, members.Replicas, "Replicas")
	c.Check().EqDiff([]string{"pod-quarantined"}, members.Quarantined, "Quarantined")
	c.NotContains(members.Replicas, "pod-quarantined", "quarantined pod leaked into Replicas")
}

// TestIdentityMembersOfUnrecognizedRoleErrors pins the guard that catches any
// role value outside {PRIMARY, REPLICA, QUARANTINED}, the set the operator's
// single writer (pkg/data-handler/topo/pooler.go) can actually produce. DRAINED
// is deliberately used as the unknown value here: the CRD doc comment on
// PodRoles still lists it, but commit e3677f0 removed it from the writer, so
// it is exactly the stale value a careless "known roles" list would still
// accept.
func TestIdentityMembersOfUnrecognizedRoleErrors(t *testing.T) {
	c := newBareCase(t)
	shard := shardWithRoles("ns1", "shard1", map[string]string{
		"pod-primary": "PRIMARY",
		"pod-drained": "DRAINED",
	})
	fc := identityFakeClient(shard)

	_, err := MembersOf(c.Context(), fc, client.ObjectKeyFromObject(shard))
	c.Error(err, "want an error for an unrecognized role")

	c.Check().
		ErrorContains(err, "unrecognized role", "want the error to call DRAINED an unrecognized role")
	c.Check().ErrorContains(err, "DRAINED", "want the error to name DRAINED specifically")
}

// TestIdentityMembersOfErrorCases pins the three distinct error cases the
// brief calls out. An absent primary and a duplicated primary are both bugs
// a test should be able to pin, but they are different bugs, so a test that
// only checked "err != nil" could not tell them apart.
func TestIdentityMembersOfErrorCases(t *testing.T) {
	c := newBareCase(t)
	cases := []struct {
		name     string
		roles    map[string]string
		wantErrs []string
	}{
		{
			name:     "empty PodRoles",
			roles:    map[string]string{},
			wantErrs: []string{"empty"},
		},
		{
			name: "no primary",
			roles: map[string]string{
				"pod-a": "REPLICA",
				"pod-b": "QUARANTINED",
			},
			wantErrs: []string{"no pod has role PRIMARY"},
		},
		{
			name: "two primaries",
			roles: map[string]string{
				"pod-a": "PRIMARY",
				"pod-b": "PRIMARY",
			},
			wantErrs: []string{"more than one pod has role PRIMARY", "pod-a", "pod-b"},
		},
	}

	// Each case's error must be distinguishable from the other two, so this
	// collects every message actually produced and cross-checks that no
	// case's message satisfies another case's expectation.
	messages := make(map[string]string, len(cases))

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newBareCase(t)
			shard := shardWithRoles("ns1", "shard1", tc.roles)
			fc := identityFakeClient(shard)

			_, err := MembersOf(c.Context(), fc, client.ObjectKeyFromObject(shard))
			c.Error(err, "want an error for %s", tc.name)
			for _, want := range tc.wantErrs {
				c.Check().ErrorContains(err, want)
			}
			messages[tc.name] = err.Error()
		})
	}

	c.Check().NotEq(messages["empty PodRoles"], messages["no primary"],
		"empty PodRoles and no-primary produced the same error message")
	c.Check().NotEq(messages["no primary"], messages["two primaries"],
		"no-primary and two-primaries produced the same error message")
	c.Check().NotEq(messages["empty PodRoles"], messages["two primaries"],
		"empty PodRoles and two-primaries produced the same error message")
}

// identityPVCVolume is a thin copy of relations_test.go's pvcVolume. Kept
// separate rather than shared, since after Task 2.1 the original lives in
// another package.
func identityPVCVolume(name, claim string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claim,
			},
		},
	}
}

// identityPodWithVolumes is a thin copy of relations_test.go's
// podWithVolumes. Kept separate rather than shared, since after Task 2.1 the
// original lives in another package.
func identityPodWithVolumes(ns, name string, volumes ...corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
			Volumes:    volumes,
		},
	}
}

// TestIdentityShardPVCOfUsesTheShardDataVolumeName pins which volume name
// the wrapper supplies. Nothing else does: PVCOf's own tests pass a
// literal, so a wrapper handing over the wrong constant is invisible.
func TestIdentityShardPVCOfUsesTheShardDataVolumeName(t *testing.T) {
	c := newBareCase(t)
	p := identityPodWithVolumes(
		"ns1", "pod-a",
		identityPVCVolume(shardcontroller.DataVolumeName, "pod-a-data"),
		identityPVCVolume("backup-data", "pod-a-backup"),
	)
	fc := identityFakeClient(p)

	claim, err := ShardPVCOf(c.Context(), fc, "ns1", "pod-a")
	c.NoError(err, "ShardPVCOf")
	c.Check().Eq("pod-a-data", claim)
}

// TestIdentityMembersOfSortsEnoughReplicasToCatchMapOrder pins the sorts in
// MembersOf. PodRoles is a Go map and Go randomises map iteration, so two
// replicas would agree with insertion order half the time and the assertion
// would be a coin flip. Five in reverse order leaves a 1-in-120 chance of
// passing against an unsorted implementation.
func TestIdentityMembersOfSortsEnoughReplicasToCatchMapOrder(t *testing.T) {
	c := newBareCase(t)
	shard := &Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "shard-0", Namespace: "ns"},
		Status: multigresv1alpha1.ShardStatus{PodRoles: map[string]string{
			"pool-primary": "PRIMARY",
			"pool-e":       "REPLICA",
			"pool-d":       "REPLICA",
			"pool-c":       "REPLICA",
			"pool-b":       "REPLICA",
			"pool-a":       "REPLICA",
			"quar-e":       "QUARANTINED",
			"quar-d":       "QUARANTINED",
			"quar-c":       "QUARANTINED",
			"quar-b":       "QUARANTINED",
			"quar-a":       "QUARANTINED",
		}},
	}
	fc := identityFakeClient(shard)

	got, err := MembersOf(c.Context(), fc, client.ObjectKeyFromObject(shard))
	c.NoError(err, "MembersOf")
	wantReplicas := []string{"pool-a", "pool-b", "pool-c", "pool-d", "pool-e"}
	wantQuarantined := []string{"quar-a", "quar-b", "quar-c", "quar-d", "quar-e"}
	c.Check().EqDiff(wantReplicas, got.Replicas, "Replicas")
	c.Check().EqDiff(wantQuarantined, got.Quarantined, "Quarantined")
}
