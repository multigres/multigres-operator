package multigrescluster

import (
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
)

// One edit selects another S3 bucket and another ServiceAccount, and the new
// account cannot be applied because another object controls it. The
// repository and the identity that reads and writes it are one selection:
// the sidecars keep the bucket and the account they have, and topology keeps
// recording that bucket, until the new account is applied. Publishing the new
// bucket alone would roll the pods onto a repository their account has no
// access to, and publishing it with an account named in the S3 block would
// start them under the other owner's identity. The rest of the edit still
// arrives.
func TestShardSA_FailedAccountSwitchKeepsRepositoryAndAccountTogether(t *testing.T) {
	s3 := func(bucket, account string) *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: bucket, Region: "us-east-1", ServiceAccountName: account,
			},
		}
	}
	for name, tc := range map[string]struct {
		// before is the accepted selection the pods run with, and edit the
		// one whose account, shardSAName, another object controls.
		before, edit func(*multigresv1alpha1.MultigresClusterSpec)
		wantAccount  string
	}{
		"from an account the operator manages": {
			before: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Backup = s3("bucket-a", "")
				spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: "acct-a"}
			},
			edit: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Backup = s3("bucket-b", "")
				spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: shardSAName}
			},
			wantAccount: "acct-a",
		},
		"from an account named in the S3 block": {
			before: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Backup = s3("bucket-a", "irsa-a")
			},
			edit: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Backup = s3("bucket-b", shardSAName)
				spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: shardSAName}
			},
			wantAccount: "irsa-a",
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			scheme := setupScheme()
			coreTpl, cellTpl, shardTpl, cluster, _, _ := setupFixtures(t)
			cluster.Spec.Images.PgBackRestd = "registry/multigres@sha256:abc"
			tc.before(&cluster.Spec)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(coreTpl, cellTpl, shardTpl, cluster).
				WithStatusSubresource(
					&multigresv1alpha1.MultigresCluster{},
					&multigresv1alpha1.Cell{},
					&multigresv1alpha1.TableGroup{},
				).
				Build()
			store := newClusterTopologyMemoryStore(t)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(1000),
				CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{store}, nil
				},
			}
			key := client.ObjectKeyFromObject(cluster)
			reconcile := func() error {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				return err
			}
			// selection is what the cluster's pods run with and what
			// topology records: the sidecar's bucket, the ServiceAccount of a
			// pod built from the TableGroup's Shard, and the bucket of the
			// database's topology record.
			type selection struct{ sidecarBucket, podAccount, topologyBucket, durability string }
			current := func() selection {
				t.Helper()
				list := &multigresv1alpha1.TableGroupList{}
				require.NoError(t, c.List(ctx, list))
				require.Len(t, list.Items, 1)
				tg := &list.Items[0]
				require.NotNil(t, tg.Spec.SidecarBackupLocation)
				require.NotNil(t, tg.Spec.SidecarBackupLocation.S3)
				shard, err := tablegroupcontroller.BuildShard(tg, &tg.Spec.Shards[0], scheme)
				require.NoError(t, err)
				var podAccount string
				for poolName, pool := range shard.Spec.Pools {
					pod, err := shardcontroller.BuildPoolPod(
						shard, string(poolName), string(pool.Cells[0]), pool, 0, scheme)
					require.NoError(t, err)
					podAccount = pod.Spec.ServiceAccountName
				}
				db, err := store.GetDatabase(ctx, string(tg.Spec.DatabaseName))
				require.NoError(t, err)
				require.NotNil(t, db.BackupLocation.GetS3())
				return selection{
					sidecarBucket:  tg.Spec.SidecarBackupLocation.S3.Bucket,
					podAccount:     podAccount,
					topologyBucket: db.BackupLocation.GetS3().Bucket,
					durability:     tg.Spec.DurabilityPolicy,
				}
			}
			edit := func(mutate func(*multigresv1alpha1.MultigresClusterSpec)) {
				t.Helper()
				got := &multigresv1alpha1.MultigresCluster{}
				require.NoError(t, c.Get(ctx, key, got))
				mutate(&got.Spec)
				require.NoError(t, c.Update(ctx, got))
			}

			for range 2 {
				require.NoError(t, reconcile())
			}
			before := current()
			require.Equal(
				t,
				selection{"bucket-a", tc.wantAccount, "bucket-a", before.durability},
				before,
			)

			foreign := preOwnedSA()
			require.NoError(t, c.Create(ctx, foreign))
			edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
				tc.edit(spec)
				spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
			})
			for range 2 {
				// A conflict is reported in the condition, not as an error.
				require.NoError(t, reconcile())
			}
			require.Equal(
				t,
				selection{
					"bucket-a",
					tc.wantAccount,
					"bucket-a",
					"MULTI_CELL_AT_LEAST_2",
				},
				current(),
				"the new repository was published without the account that goes with it",
			)

			// The database's record is lost while the selection is held,
			// as it is when the global topology is replaced. The record that
			// is created again is the one the pods are on, on the first
			// pass and on the ones after it: multipooler refuses a sidecar
			// whose location differs from the record's.
			db := string(cluster.Spec.Databases[0].Name)
			require.NoError(t, store.DeleteDatabase(ctx, db, true))
			for range 2 {
				require.NoError(t, reconcile())
				require.Equal(t,
					selection{"bucket-a", tc.wantAccount, "bucket-a", "MULTI_CELL_AT_LEAST_2"},
					current(),
					"a record created again took the repository that is not applied")
			}

			// A TableGroup from before the sidecar has no location to give
			// a record that has to be created again, and the spec's is the
			// one that is not applied. The record is then left for the pass
			// that applies the account.
			list := &multigresv1alpha1.TableGroupList{}
			require.NoError(t, c.List(ctx, list))
			require.Len(t, list.Items, 1)
			list.Items[0].Spec.SidecarBackupLocation = nil
			list.Items[0].Spec.Images.PgBackRestd = ""
			require.NoError(t, c.Update(ctx, &list.Items[0]))
			require.NoError(t, store.DeleteDatabase(ctx, db, true))
			for range 2 {
				require.NoError(t, reconcile())
				_, err := store.GetDatabase(ctx, db)
				require.ErrorIs(t, err, &topoclient.TopoError{Code: topoclient.NoNode},
					"a record was created for a database whose pods have no sidecar location")
			}

			// The other owner lets go of the account, and the whole
			// selection arrives together.
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), foreign))
			foreign.OwnerReferences = nil
			require.NoError(t, c.Update(ctx, foreign))
			for range 2 {
				require.NoError(t, reconcile())
			}
			require.Equal(t,
				selection{"bucket-b", shardSAName, "bucket-b", "MULTI_CELL_AT_LEAST_2"}, current())
		})
	}
}
