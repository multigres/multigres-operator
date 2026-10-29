package shard

import (
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// Reproduce template drift after provisioning, including a missing pod with
// only a retained PVC. Neither case may allocate a second pod or data volume.
func TestLiteRejectsProvisionedIdentityChange(t *testing.T) {
	for _, keepPod := range []bool{true, false} {
		t.Run(
			map[bool]string{true: "live primary", false: "retained data only"}[keepPod],
			func(t *testing.T) {
				shard, pool := newTestLiteShard(), newTestPoolSpec()
				pool.Cells = []multigresv1alpha1.CellName{"z1"}
				scheme := reloadTestScheme(t)
				pod, err := BuildLitePod(shard, "original", "z1", pool, scheme)
				require.NoError(t, err)
				pvc, err := BuildPoolDataPVC(shard, "original", "z1", pool, 0, false, scheme)
				require.NoError(t, err)
				shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"replacement": pool,
				}
				objects := []client.Object{shard, pvc}
				if keepPod {
					objects = append(objects, pod)
				}
				c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
				r := &ShardReconciler{
					// The informer has not observed the old resources yet.
					Client: fake.NewClientBuilder().
						WithScheme(scheme).
						WithObjects(shard).
						Build(),
					APIReader: c,
					Scheme:    scheme,
					Recorder:  record.NewFakeRecorder(10),
					CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
						t.Fatal("identity drift must be rejected before topology writes")
						return nil, nil
					},
				}
				_, err = r.reconcileLite(
					t.Context(),
					shard,
					reloadTestRendered(),
					storageClassCheck{},
				)
				require.ErrorContains(t, err, "lite pool identity is immutable")
				pods := &corev1.PodList{}
				require.NoError(t, c.List(t.Context(), pods))
				if keepPod {
					require.Len(t, pods.Items, 1)
					require.Equal(t, pod.Name, pods.Items[0].Name)
				} else {
					require.Empty(t, pods.Items)
				}
				pvcs := &corev1.PersistentVolumeClaimList{}
				require.NoError(t, c.List(t.Context(), pvcs))
				require.Len(t, pvcs.Items, 1)
				require.Equal(t, pvc.Name, pvcs.Items[0].Name)
			},
		)
	}
}
