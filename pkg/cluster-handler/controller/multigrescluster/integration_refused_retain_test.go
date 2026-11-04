//go:build integration

package multigrescluster_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestRefusedClusterRetainsDeclaredAndDynamicTableGroups refuses a cluster
// that has one TableGroup its spec declares and one the catalog created, and
// then asks for its claims to be kept, the edit an owner makes before
// deleting a cluster that cannot be repaired. Both TableGroups must carry
// Retain for every shard: a TableGroup the catalog created is named nowhere
// in the spec, and deleting the cluster deletes the claims of every Shard
// that still says Delete.
//
// The policy is written under the manager that applies TableGroups, against
// an API server, so that what it leaves behind follows the spec again once
// the cluster is accepted: a level the accepted spec no longer sets is
// removed, and the labels, annotations and owner reference of the TableGroup
// are what they were.
func TestRefusedClusterRetainsDeclaredAndDynamicTableGroups(t *testing.T) {
	ctx := t.Context()
	_, factory := memorytopo.NewServerAndFactory(ctx, "zone-a")
	k8sClient, _, _ := setupIntegrationCore(t, nil,
		func(r *multigrescluster.MultigresClusterReconciler) {
			r.CreateTopoStore = func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
				return topoclient.NewWithFactory(
					factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
				), nil
			}
			r.ListDesiredTableGroups = func(
				context.Context, *multigresv1alpha1.MultigresCluster, string,
			) ([]multiadmin.TableGroup, error) {
				return []multiadmin.TableGroup{{
					ID: 7, Name: "commerce", Shards: []string{"0-inf"},
					State: multiadmin.StateCreating,
				}}, nil
			}
		})

	const name = "refused-retain"
	deleteAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	retainAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: testNamespace,
			Annotations: map[string]string{multigresv1alpha1.AnnotationDynamicTableGroups: "true"},
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate: "default", CellTemplate: "default", ShardTemplate: "default",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Images:            multigresv1alpha1.ClusterImages{PgBackRestd: "registry/multigres@sha256:abc"},
			PVCDeletionPolicy: deleteAll,
			Cells:             []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
					Shards:  []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
				}},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, k8sClient.Create(ctx, cluster))

	const wait, tick = 30 * time.Second, 200 * time.Millisecond
	tableGroups := func() (map[string]*multigresv1alpha1.TableGroup, error) {
		list := &multigresv1alpha1.TableGroupList{}
		if err := k8sClient.List(ctx, list, client.InNamespace(testNamespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: name}); err != nil {
			return nil, err
		}
		if len(list.Items) != 2 {
			return nil, fmt.Errorf("%d TableGroups, want the declared and the dynamic one", len(list.Items))
		}
		out := map[string]*multigresv1alpha1.TableGroup{}
		for i := range list.Items {
			kind := "declared"
			if list.Items[i].Labels[multigresv1alpha1.LabelDynamicTableGroup] == "true" {
				kind = "dynamic"
			}
			out[kind] = &list.Items[i]
		}
		if len(out) != 2 {
			return nil, fmt.Errorf("want one declared and one dynamic TableGroup, got %d kinds", len(out))
		}
		return out, nil
	}
	// effective is the policy the Shard controller acts on for every shard
	// and pool of tg, or an error when they differ from want.
	effective := func(tg *multigresv1alpha1.TableGroup, want *multigresv1alpha1.PVCDeletionPolicy) error {
		if len(tg.Spec.Shards) == 0 {
			return fmt.Errorf("%s has no shards", tg.Name)
		}
		for _, shard := range tg.Spec.Shards {
			policy := multigresv1alpha1.MergePVCDeletionPolicy(
				shard.PVCDeletionPolicy, tg.Spec.PVCDeletionPolicy)
			if len(shard.Pools) == 0 {
				return fmt.Errorf("%s shard %s has no pools", tg.Name, shard.Name)
			}
			for poolName, pool := range shard.Pools {
				got := multigresv1alpha1.MergePVCDeletionPolicy(pool.PVCDeletionPolicy, policy)
				if got == nil || *got != *want {
					return fmt.Errorf("%s shard %s pool %s: policy %+v, want %+v",
						tg.Name, shard.Name, poolName, got, want)
				}
			}
		}
		return nil
	}
	// claimOwners builds, with the TableGroup and Shard controllers' own
	// builders, every Shard of tg and every claim of those Shards, and
	// returns how many of the pool data claims and of the shared backup
	// claims carry an owner reference. The garbage
	// collector deletes a claim with its Shard only through that reference,
	// and envtest runs no garbage collector, so the reference is as far as
	// "the claims survive deleting the cluster" can be followed here.
	claimOwners := func(tg *multigresv1alpha1.TableGroup) (data, backup, total int) {
		t.Helper()
		scheme := k8sClient.Scheme()
		for i := range tg.Spec.Shards {
			shard, err := tablegroupcontroller.BuildShard(tg, &tg.Spec.Shards[i], scheme)
			require.NoError(t, err)
			claim, err := shardcontroller.BuildSharedBackupPVC(
				shard, shardcontroller.ShouldDeleteShardLevelPVCOnRemoval(shard), scheme)
			require.NoError(t, err)
			total++
			backup += len(claim.OwnerReferences)
			for poolName, pool := range shard.Spec.Pools {
				require.NotEmpty(t, pool.Cells)
				claim, err := shardcontroller.BuildPoolDataPVC(
					shard, string(poolName), string(pool.Cells[0]), pool, 0,
					shardcontroller.ShouldDeletePVCOnShardRemoval(shard, pool), scheme)
				require.NoError(t, err)
				total++
				data += len(claim.OwnerReferences)
			}
		}
		return data, backup, total
	}
	eventually := func(what string, check func(map[string]*multigresv1alpha1.TableGroup) error) map[string]*multigresv1alpha1.TableGroup {
		t.Helper()
		var last error
		var got map[string]*multigresv1alpha1.TableGroup
		if !assert.Eventually(t, func() bool {
			got, last = tableGroups()
			if last == nil {
				last = check(got)
			}
			return last == nil
		}, wait, tick) {
			require.NoError(t, last, what)
		}
		return got
	}
	edit := func(mutate func(*multigresv1alpha1.MultigresClusterSpec)) {
		t.Helper()
		require.Eventually(t, func() bool {
			stored := &multigresv1alpha1.MultigresCluster{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored); err != nil {
				return false
			}
			mutate(&stored.Spec)
			return k8sClient.Update(ctx, stored) == nil
		}, wait, tick)
	}

	before := eventually("both TableGroups with Delete", func(tgs map[string]*multigresv1alpha1.TableGroup) error {
		for _, tg := range tgs {
			if err := effective(tg, deleteAll); err != nil {
				return err
			}
		}
		return nil
	})

	for kind, tg := range before {
		data, backup, total := claimOwners(tg)
		require.Positive(t, total, kind)
		require.Equal(t, total, data+backup, "%s: with Delete every claim goes with its Shard", kind)
	}

	// Refused: a shard-level S3 block in a distributed cluster whose
	// database-level location is the filesystem. The same edit asks for
	// the claims to be kept, at the pool's own level, which is a level the
	// spec stops setting below.
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
			},
		}
		spec.Databases[0].TableGroups[0].Shards[0].Overrides = &multigresv1alpha1.ShardOverrides{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {PVCDeletionPolicy: retainAll},
			},
		}
	})
	require.Eventually(t, func() bool {
		stored := &multigresv1alpha1.MultigresCluster{}
		return k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored) == nil &&
			meta.IsStatusConditionFalse(stored.Status.Conditions,
				multigresv1alpha1.ConditionBackupRepositoryValid)
	}, wait, tick, "the cluster was not refused")
	retained := eventually("Retain on the declared and the dynamic TableGroup",
		func(tgs map[string]*multigresv1alpha1.TableGroup) error {
			for _, tg := range tgs {
				if err := effective(tg, retainAll); err != nil {
					return err
				}
			}
			return nil
		})
	for kind, tg := range retained {
		// The pool asked to keep its claims. The shared backup claim follows
		// the shard's own policy, which still says Delete.
		data, backup, _ := claimOwners(tg)
		require.Zero(t, data, "%s: a pool's claim would be deleted with its Shard", kind)
		require.Positive(t, backup, kind)
		was := before[kind]
		require.Equal(t, was.Labels, tg.Labels, kind)
		require.Equal(t, was.OwnerReferences, tg.OwnerReferences, kind)
		wantAnnotations := map[string]string{}
		for k, v := range was.Annotations {
			if k != metadata.AnnotationSpecHash {
				wantAnnotations[k] = v
			}
		}
		gotAnnotations := tg.Annotations
		if gotAnnotations == nil {
			gotAnnotations = map[string]string{}
		}
		require.Equal(t, wantAnnotations, gotAnnotations, kind)
		// Nothing else of the refused spec reached it.
		require.Equal(t, was.Spec.Shards[0].Backup, tg.Spec.Shards[0].Backup, kind)
	}

	// Still refused, the cluster as a whole is told to keep its claims: no
	// claim of either TableGroup goes with its Shard any more, the shared
	// backup claim included.
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.PVCDeletionPolicy = retainAll
	})
	eventually("no claim owned by a Shard of either TableGroup",
		func(tgs map[string]*multigresv1alpha1.TableGroup) error {
			for kind, tg := range tgs {
				if data, backup, _ := claimOwners(tg); data+backup != 0 {
					return fmt.Errorf("%s: %d data and %d backup claims would be deleted with their Shard",
						kind, data, backup)
				}
			}
			return nil
		})

	// Accepted again, with a spec that no longer sets a policy for the pool:
	// the TableGroups follow the spec, and the Retain written while refused
	// does not stay behind at that level.
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Databases[0].TableGroups[0].Shards[0].Backup = nil
		spec.Databases[0].TableGroups[0].Shards[0].Overrides.Pools["default"] = multigresv1alpha1.PoolSpec{}
		spec.PVCDeletionPolicy = deleteAll
	})
	eventually("the spec's Delete again once it is accepted",
		func(tgs map[string]*multigresv1alpha1.TableGroup) error {
			for kind, tg := range tgs {
				if err := effective(tg, deleteAll); err != nil {
					return fmt.Errorf("%s: %w", kind, err)
				}
				if data, backup, total := claimOwners(tg); data+backup != total {
					return fmt.Errorf("%s: %d of %d claims go with their Shard", kind, data+backup, total)
				}
			}
			return nil
		})
}
