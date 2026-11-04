//go:build integration
// +build integration

package multigrescluster

import (
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// The refused path reads a TableGroup from the informer and writes Retain
// into it some time later. In between, the TableGroup can be deleted (a
// catalog TableGroup that was dropped) or deleted and created again. The
// write must then change nothing: an apply that brought a deleted TableGroup
// back would provision its Shards and pods on a cluster the operator has
// refused to build, and one over a newer TableGroup would put the spec that
// was read over it. Both ways of writing are covered: the apply for a
// TableGroup the operator applied, the patch for one it did not.
func TestWriteRetained_OnlyWritesTheTableGroupItRead(t *testing.T) {
	scheme := setupScheme()
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths(
		filepath.Join("..", "..", "..", "..", "config", "crd", "bases"),
	))
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := t.Context()
	r := &MultigresClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
	}
	retain := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}

	// One reconcile of a cluster gives a TableGroup as the operator builds
	// and applies it. No manager is running, so nothing writes it again.
	const clusterName, namespace = "write-retained", "default"
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password", Key: "password",
			},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "postgres", Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "default", Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"default": {
									Type:  "readWrite",
									Cells: []multigresv1alpha1.CellName{"zone-a"},
								},
							},
						},
					}},
				}},
			}},
		},
	}
	require.NoError(t, c.Create(ctx, cluster))
	store := newClusterTopologyMemoryStore(t)
	r.CreateTopoStore = func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
		return noCloseStore{store}, nil
	}
	list := &multigresv1alpha1.TableGroupList{}
	for range 5 {
		_, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace},
		})
		require.NoError(t, err)
		require.NoError(t, c.List(ctx, list, client.InNamespace(namespace)))
		if len(list.Items) == 1 {
			break
		}
	}
	require.Len(t, list.Items, 1)
	built := list.Items[0].DeepCopy()
	key := client.ObjectKeyFromObject(built)
	require.NoError(t, c.Delete(ctx, built))

	for name, applied := range map[string]bool{
		"a TableGroup the operator applied": true,
		"a TableGroup written another way":  false,
	} {
		t.Run(name, func(t *testing.T) {
			create := func(durability string) *multigresv1alpha1.TableGroup {
				t.Helper()
				tg := &multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name: built.Name, Namespace: built.Namespace,
						Labels: built.Labels, Annotations: built.Annotations,
						OwnerReferences: built.OwnerReferences,
					},
					Spec: built.Spec.ForApply(),
				}
				tg.Spec.DurabilityPolicy = durability
				if applied {
					tg.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TableGroup"))
					require.NoError(t, c.Patch(ctx, tg, client.Apply, client.ForceOwnership,
						client.FieldOwner(ownership.ManagerChildWriter)))
				} else {
					require.NoError(t, c.Create(ctx, tg))
				}
				got := &multigresv1alpha1.TableGroup{}
				require.NoError(t, c.Get(ctx, key, got))
				return got
			}
			// retained is the write the refused path makes for stored.
			retained := func(stored *multigresv1alpha1.TableGroup) error {
				tg := stored.DeepCopy()
				tg.Spec.PVCDeletionPolicy = retain
				return r.writeRetained(ctx, tg, stored)
			}

			// The TableGroup as read is written.
			stored := create("AT_LEAST_2")
			require.NoError(t, retained(stored))
			got := &multigresv1alpha1.TableGroup{}
			require.NoError(t, c.Get(ctx, key, got))
			require.Equal(t, retain, got.Spec.PVCDeletionPolicy)

			// Rewritten since it was read: refused, and left as it is.
			require.Error(t, retained(stored), "a write from a stale read was accepted")
			after := &multigresv1alpha1.TableGroup{}
			require.NoError(t, c.Get(ctx, key, after))
			require.Equal(t, got.ResourceVersion, after.ResourceVersion)

			// Deleted since it was read: not brought back.
			stale := after.DeepCopy()
			require.NoError(t, c.Delete(ctx, after))
			err := retained(stale)
			require.Error(t, err)
			require.True(t, apierrors.IsNotFound(err) || apierrors.IsConflict(err), "%v", err)
			require.True(t, apierrors.IsNotFound(c.Get(ctx, key, &multigresv1alpha1.TableGroup{})),
				"the write created a TableGroup that had been deleted")

			// Deleted and created again since it was read: the new one keeps
			// its own spec.
			recreated := create("MULTI_CELL_AT_LEAST_2")
			require.NotEqual(t, stale.UID, recreated.UID)
			require.Error(t, retained(stale))
			require.NoError(t, c.Get(ctx, key, got))
			require.Equal(t, recreated.ResourceVersion, got.ResourceVersion)
			require.Equal(t, "MULTI_CELL_AT_LEAST_2", got.Spec.DurabilityPolicy)
			require.NotEqual(t, retain, got.Spec.PVCDeletionPolicy)
			require.NoError(t, c.Delete(ctx, got))
		})
	}
}
