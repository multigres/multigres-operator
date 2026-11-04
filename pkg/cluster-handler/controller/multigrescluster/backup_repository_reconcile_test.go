package multigrescluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// credentialMarker stands for a credential written into an S3 endpoint's
// URL. It must not appear in a condition, a status message or an event.
const credentialMarker = "hunter2-MARKER"

// A cluster whose backup repository is refused goes through whole reconciles
// here, on an external global topology whose managed predecessor is still
// present.
//
// While refused it must show the refusal where an operator looks (the
// BackupRepositoryValid condition, the Degraded phase, one event) and must
// not act on the refused spec: no database record in topology, no TableGroup,
// and the managed topology left in place. It returns neither an error nor a
// requeue, because nothing but an edit to the cluster changes the outcome.
// Once the spec is fixed, the next reconcile clears all of it.
func TestReconcile_RefusedBackupRepository(t *testing.T) {
	s3 := func() *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
			},
		}
	}
	for name, tc := range map[string]struct {
		// refuse makes the spec one the controller refuses, and fix makes it
		// one it accepts.
		refuse, fix func(*multigresv1alpha1.MultigresClusterSpec)
		wantReason  string
		wantMessage string
	}{
		"S3 only in a shard block": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].TableGroups[0].Shards[0].Backup = s3()
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup = s3()
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback,
			wantMessage: "local filesystem",
		},
		"a tablegroup block with a cipher the database lacks": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup = s3()
				spec.Databases[0].TableGroups[0].Backup = &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{
						SecretName: "tablegroup-cipher",
					},
				}
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].TableGroups[0].Backup = nil
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantMessage: "encryption.secretName",
		},
		"a shard block in another bucket": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup = s3()
				other := s3()
				other.S3.Bucket = "shard-bucket"
				spec.Databases[0].TableGroups[0].Shards[0].Backup = other
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].TableGroups[0].Shards[0].Backup.S3.Bucket = "bucket"
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantMessage: "s3.bucket",
		},
		// A filesystem block below an encrypted S3 location replaces what the
		// shard inherits, cipher included, while the sidecar keeps the
		// location's.
		"a tablegroup block of another type": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				encrypted := s3()
				encrypted.Encryption = &multigresv1alpha1.BackupEncryptionConfig{
					SecretName: "cipher",
				}
				spec.Databases[0].Backup = encrypted
				spec.Databases[0].TableGroups[0].Backup = &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
				}
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].TableGroups[0].Backup = nil
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantMessage: "sets type",
		},
		// The two rules admission applies beyond the repository ones. A
		// cluster stored with either would otherwise have its Shards built
		// and each of them refused, with the cluster reporting Healthy.
		"S3 with no credential source the sidecar supports": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				nodeRole := s3()
				nodeRole.S3.CredentialsSecret = ""
				nodeRole.S3.UseEnvCredentials = true
				spec.Databases[0].Backup = nodeRole
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup.S3.CredentialsSecret = "s3-creds"
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryUnsupportedCredentials,
			wantMessage: "no credential source",
		},
		"an S3 endpoint with a scheme other than https": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				plain := s3()
				plain.S3.Endpoint = "http://key:" + credentialMarker + "@minio.storage.svc:9000"
				spec.Databases[0].Backup = plain
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup.S3.Endpoint = "https://minio.storage.svc:9000"
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryUnsupportedEndpoint,
			wantMessage: "a scheme other than https",
		},
		"a filesystem path outside the backup volume": {
			refuse: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup = &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/data/backups"},
				}
			},
			fix: func(spec *multigresv1alpha1.MultigresClusterSpec) {
				spec.Databases[0].Backup = s3()
			},
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryUnsupportedPath,
			wantMessage: "/data/backups",
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			scheme := setupScheme()
			coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
			cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://external-topo:2379"},
				},
			}
			tc.refuse(&cluster.Spec)
			managedTopo := &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName + "-global-topo",
					Namespace: namespace,
					Labels: map[string]string{
						metadata.LabelMultigresCluster: clusterName,
						metadata.LabelAppComponent:     metadata.ComponentGlobalTopo,
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: multigresv1alpha1.GroupVersion.String(),
						Kind:       "MultigresCluster",
						Name:       clusterName,
						UID:        cluster.UID,
						Controller: ptr.To(true),
					}},
				},
				Spec: multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}},
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(coreTpl, cellTpl, shardTpl, cluster, managedTopo).
				WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
				Build()
			store := newClusterTopologyMemoryStore(t)
			recorder := record.NewFakeRecorder(200)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: recorder,
				CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{store}, nil
				},
			}
			key := types.NamespacedName{Name: clusterName, Namespace: namespace}
			stored := func() *multigresv1alpha1.MultigresCluster {
				t.Helper()
				got := &multigresv1alpha1.MultigresCluster{}
				require.NoError(t, c.Get(ctx, key, got))
				return got
			}
			tableGroups := func() int {
				t.Helper()
				list := &multigresv1alpha1.TableGroupList{}
				require.NoError(t, c.List(ctx, list, client.InNamespace(namespace)))
				return len(list.Items)
			}
			// eventsNaming drains the recorder and returns the events that
			// contain one of the reasons.
			eventsNaming := func(reasons ...string) []string {
				var events []string
				for len(recorder.Events) > 0 {
					e := <-recorder.Events
					for _, reason := range reasons {
						if strings.Contains(e, reason) {
							events = append(events, e)
							break
						}
					}
				}
				return events
			}

			for range 2 {
				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err, "a refusal is not retried")
				assert.Zero(t, res, "a refusal is not polled")
			}

			got := stored()
			cond := meta.FindStatusCondition(
				got.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
			)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionFalse, cond.Status)
			assert.Equal(t, tc.wantReason, cond.Reason)
			assert.Contains(t, cond.Message, tc.wantMessage)
			assert.Equal(t, multigresv1alpha1.PhaseDegraded, got.Status.Phase)
			assert.Contains(t, got.Status.Message, tc.wantMessage)
			assert.NotContains(t, cond.Message, credentialMarker)
			assert.NotContains(t, got.Status.Message, credentialMarker)

			// Retiring the managed topology waits on a Healthy cluster and
			// says so in an event. A refused cluster is not on its way to
			// Healthy, so the only thing it reports is the refusal.
			events := eventsNaming("BackupRepositoryInvalid", "GlobalTopoRetire")
			require.Len(t, events, 1,
				"the refusal is reported once, and nothing else is: %v", events)
			assert.Contains(t, events[0], "BackupRepositoryInvalid")
			assert.Contains(t, events[0], tc.wantMessage)
			assert.NotContains(t, events[0], credentialMarker)

			assert.Zero(t, tableGroups(), "no TableGroup may be applied from a refused spec")
			_, err := store.GetDatabase(ctx, "db1")
			require.Error(t, err, "no database may be registered from a refused spec")
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(managedTopo),
				&multigresv1alpha1.TopoServer{}),
				"the managed global topology must not be retired while refused")

			fixed := stored()
			tc.fix(&fixed.Spec)
			require.NoError(t, c.Update(ctx, fixed))
			_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			got = stored()
			cond = meta.FindStatusCondition(
				got.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
			)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
			assert.NotEqual(t, multigresv1alpha1.PhaseDegraded, got.Status.Phase)
			assert.NotContains(t, got.Status.Message, "refused")
			assert.Equal(t, 1, tableGroups())
			db, err := store.GetDatabase(ctx, "db1")
			require.NoError(t, err)
			assert.Equal(t, "bucket", db.GetBackupLocation().GetS3().GetBucket())
			assert.Empty(t, eventsNaming("BackupRepositoryInvalid"))
		})
	}
}

// refusedClusterFixture is a stored cluster the controller refuses (S3 only in
// a shard block), with its templates and any further objects.
func refusedClusterFixture(
	t *testing.T,
	objs ...client.Object,
) (*MultigresClusterReconciler, client.Client, *record.FakeRecorder, types.NamespacedName) {
	t.Helper()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
	cluster.Spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(append([]client.Object{coreTpl, cellTpl, shardTpl, cluster}, objs...)...).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()
	store := newClusterTopologyMemoryStore(t)
	recorder := record.NewFakeRecorder(200)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{store}, nil
		},
	}
	return r, c, recorder, types.NamespacedName{Name: clusterName, Namespace: namespace}
}

// A refused cluster still reconciles its cells, and a cell that left the spec
// is deleted in steps the controller polls for. The refusal ends the pass
// without a requeue only when nothing else is waiting on one.
func TestReconcile_RefusedClusterStillPollsACellBeingRemoved(t *testing.T) {
	removed := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-zone-gone",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "zone-gone"},
	}
	r, c, _, key := refusedClusterFixture(t, removed)

	for range 2 {
		res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, res.RequeueAfter,
			"the cell's removal is polled while the cluster is refused")
	}
	got := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, c.Get(t.Context(), key, got))
	assert.True(t, meta.IsStatusConditionFalse(
		got.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid))
	cell := &multigresv1alpha1.Cell{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(removed), cell))
	assert.NotEmpty(t, cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion])
}

// The refusal is reported once while it stays the same, and again when an
// edit trades it for a different one: the first event no longer describes
// what is wrong.
func TestReconcile_RefusalEventFollowsTheReason(t *testing.T) {
	r, c, recorder, key := refusedClusterFixture(t)
	refusals := func() []string {
		var events []string
		for len(recorder.Events) > 0 {
			if e := <-recorder.Events; strings.Contains(e, "BackupRepositoryInvalid") {
				events = append(events, e)
			}
		}
		return events
	}
	reconcile := func() {
		t.Helper()
		for range 2 {
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
		}
	}

	reconcile()
	first := refusals()
	require.Len(t, first, 1)
	assert.Contains(t, first[0], "local filesystem")

	cluster := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, c.Get(t.Context(), key, cluster))
	shardBlock := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Backup
	cluster.Spec.Databases[0].Backup = shardBlock.DeepCopy()
	shardBlock.S3.Region = "eu-west-1"
	require.NoError(t, c.Update(t.Context(), cluster))
	reconcile()
	second := refusals()
	require.Len(t, second, 1, "a new reason is a new event")
	assert.Contains(t, second[0], "s3.region")

	require.NoError(t, c.Get(t.Context(), key, cluster))
	cluster.Labels["touched"] = "true"
	require.NoError(t, c.Update(t.Context(), cluster))
	reconcile()
	assert.Empty(t, refusals(), "the same reason is not reported again")
}

// A refused cluster's TableGroups stay as the last accepted spec made them,
// with one exception: an edit that asks for PVCs to be kept reaches them.
// Deleting the cluster garbage-collects every claim its Shards still own, and
// which claims those are follows the policy the TableGroup carries, so a
// Retain that never arrived would lose the volumes the edit was made to keep.
// The other direction is not propagated: a refused spec does not make a
// claim deletable.
func TestReconcile_RefusedClusterStillRetainsPVCs(t *testing.T) {
	ctx := t.Context()
	deleteAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	retainAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	refuse := func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
			},
		}
	}
	for name, tc := range map[string]struct {
		accepted, refused *multigresv1alpha1.PVCDeletionPolicy
		want              multigresv1alpha1.PVCDeletionPolicy
	}{
		"an edit to Retain reaches the TableGroup": {deleteAll, retainAll, *retainAll},
		"an edit to Delete does not":               {retainAll, deleteAll, *retainAll},
		// A cluster without a policy is defaulted to Delete for both.
		"an edit that removes the policy changes nothing": {deleteAll, nil, *deleteAll},
		"an edit to Retain for deletion only": {
			deleteAll,
			&multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := setupScheme()
			coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
			cluster.Spec.PVCDeletionPolicy = tc.accepted
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(coreTpl, cellTpl, shardTpl, cluster).
				WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
				Build()
			store := newClusterTopologyMemoryStore(t)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(200),
				CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{store}, nil
				},
			}
			key := types.NamespacedName{Name: clusterName, Namespace: namespace}
			tableGroup := func() *multigresv1alpha1.TableGroup {
				t.Helper()
				list := &multigresv1alpha1.TableGroupList{}
				require.NoError(t, c.List(ctx, list, client.InNamespace(namespace)))
				require.Len(t, list.Items, 1)
				return &list.Items[0]
			}
			// effective is the policy the Shard controller would act on for
			// the TableGroup's first shard and pool.
			effective := func(tg *multigresv1alpha1.TableGroup) multigresv1alpha1.PVCDeletionPolicy {
				require.Len(t, tg.Spec.Shards, 1)
				shard := tg.Spec.Shards[0]
				require.Len(t, shard.Pools, 1)
				policy := multigresv1alpha1.MergePVCDeletionPolicy(
					shard.PVCDeletionPolicy, tg.Spec.PVCDeletionPolicy)
				for _, pool := range shard.Pools {
					policy = multigresv1alpha1.MergePVCDeletionPolicy(
						pool.PVCDeletionPolicy, policy)
				}
				require.NotNil(t, policy)
				return *policy
			}
			for range 3 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			before := tableGroup()
			require.Equal(t, *tc.accepted, effective(before))

			stored := &multigresv1alpha1.MultigresCluster{}
			require.NoError(t, c.Get(ctx, key, stored))
			refuse(&stored.Spec)
			stored.Spec.PVCDeletionPolicy = tc.refused
			require.NoError(t, c.Update(ctx, stored))
			for range 2 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			require.NoError(t, c.Get(ctx, key, stored))
			require.True(t, meta.IsStatusConditionFalse(
				stored.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid))

			after := tableGroup()
			assert.Equal(t, tc.want, effective(after))
			// Nothing else of the refused spec reached it.
			after.Spec.PVCDeletionPolicy, before.Spec.PVCDeletionPolicy = nil, nil
			for i := range after.Spec.Shards {
				after.Spec.Shards[i].PVCDeletionPolicy = nil
				before.Spec.Shards[i].PVCDeletionPolicy = nil
				for pool, spec := range after.Spec.Shards[i].Pools {
					spec.PVCDeletionPolicy = nil
					after.Spec.Shards[i].Pools[pool] = spec
				}
				for pool, spec := range before.Spec.Shards[i].Pools {
					spec.PVCDeletionPolicy = nil
					before.Spec.Shards[i].Pools[pool] = spec
				}
			}
			assert.Equal(t, before.Spec, after.Spec)
		})
	}
}

// A refused cluster's shards stay on the global topology they are on, since
// nothing is registered and no TableGroup is rewritten while it is refused.
// Its cells still reconcile, but an edit of the global topology made while
// refused must not reach them: the gateways would move to a topology that
// holds none of the cluster's databases while the shards stay on the other.
// The edit arrives once the cluster is accepted.
func TestReconcile_RefusedClusterKeepsCellsOnTheServingTopology(t *testing.T) {
	ctx := t.Context()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
	external := func(endpoint string) *multigresv1alpha1.GlobalTopoServerSpec {
		return &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{multigresv1alpha1.EndpointUrl(endpoint)},
			},
		}
	}
	cluster.Spec.GlobalTopoServer = external("http://serving-topo:2379")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, cluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()
	store := newClusterTopologyMemoryStore(t)
	r := &MultigresClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(500),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{store}, nil
		},
	}
	key := types.NamespacedName{Name: clusterName, Namespace: namespace}
	reconcile := func() {
		t.Helper()
		for range 2 {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
		}
	}
	addresses := func() (cells, tableGroups []string) {
		t.Helper()
		cellList := &multigresv1alpha1.CellList{}
		require.NoError(t, c.List(ctx, cellList, client.InNamespace(namespace)))
		for _, cell := range cellList.Items {
			cells = append(cells, cell.Spec.GlobalTopoServer.Address)
		}
		tgList := &multigresv1alpha1.TableGroupList{}
		require.NoError(t, c.List(ctx, tgList, client.InNamespace(namespace)))
		for _, tg := range tgList.Items {
			tableGroups = append(tableGroups, tg.Spec.GlobalTopoServer.Address)
		}
		require.NotEmpty(t, cells)
		require.NotEmpty(t, tableGroups)
		return cells, tableGroups
	}
	edit := func(mutate func(*multigresv1alpha1.MultigresClusterSpec)) {
		t.Helper()
		stored := &multigresv1alpha1.MultigresCluster{}
		require.NoError(t, c.Get(ctx, key, stored))
		mutate(&stored.Spec)
		require.NoError(t, c.Update(ctx, stored))
	}
	all := func(want string, got []string) {
		t.Helper()
		for _, address := range got {
			require.Equal(t, want, address)
		}
	}

	reconcile()
	cells, tableGroups := addresses()
	all("http://serving-topo:2379", cells)
	all("http://serving-topo:2379", tableGroups)

	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.GlobalTopoServer = external("http://new-topo:2379")
		spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
			},
		}
	})
	reconcile()
	cells, tableGroups = addresses()
	all("http://serving-topo:2379", tableGroups)
	all("http://serving-topo:2379", cells)

	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Databases[0].TableGroups[0].Shards[0].Backup = nil
	})
	reconcile()
	cells, tableGroups = addresses()
	all("http://new-topo:2379", cells)
	all("http://new-topo:2379", tableGroups)
}

// A refused cluster whose PVC deletion policy cannot be worked out, here
// because a shard names a ShardTemplate that does not exist, still reports
// the refusal: the pass writes status before it fails, so the condition and
// the phase say why nothing is reconciled, and the error is retried.
func TestReconcile_RefusedClusterReportsTheRefusalWhenRetainFails(t *testing.T) {
	ctx := t.Context()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, cluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()
	store := newClusterTopologyMemoryStore(t)
	r := &MultigresClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(200),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{store}, nil
		},
	}
	key := types.NamespacedName{Name: clusterName, Namespace: namespace}
	for range 2 {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
	}

	stored := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, c.Get(ctx, key, stored))
	shard := &stored.Spec.Databases[0].TableGroups[0].Shards[0]
	shard.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
		},
	}
	shard.ShardTemplate = "no-such-template"
	require.NoError(t, c.Update(ctx, stored))

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.ErrorContains(t, err, "no-such-template")
	require.NoError(t, c.Get(ctx, key, stored))
	require.True(t, meta.IsStatusConditionFalse(
		stored.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid),
		"the refusal never reached status")
	require.Equal(t, multigresv1alpha1.PhaseDegraded, stored.Status.Phase)
}

// A refused cluster that is told to keep its claims has lost the ShardTemplate
// its shards resolve from, so what the spec asks for at a shard's or a pool's
// own level is not known. The cluster's own Retain does not depend on the
// template and must still reach every TableGroup, the one the catalog
// created included: the owner is about to delete the cluster, and deleting
// it deletes the claims of every Shard that still says Delete. Where a
// stored pool says Delete itself, the tablegroup's level cannot reach it,
// and the pass says which shard is left unprotected.
func TestReconcile_RefusedClusterRetainsWithoutItsShardTemplate(t *testing.T) {
	deleteAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	retainAll := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	for name, tc := range map[string]struct {
		// poolPolicy is the policy the accepted spec set on the pool itself.
		poolPolicy *multigresv1alpha1.PVCDeletionPolicy
		kept       bool
	}{
		"no level below the cluster sets a policy": {kept: true},
		"the stored pool says Delete itself":       {poolPolicy: deleteAll},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			scheme := setupScheme()
			coreTpl, cellTpl, shardTpl, cluster, clusterName, namespace := setupFixtures(t)
			cluster.Annotations = map[string]string{
				multigresv1alpha1.AnnotationDynamicTableGroups: "true",
			}
			cluster.Spec.PVCDeletionPolicy = deleteAll
			// A catalog TableGroup is shaped from the database's default one.
			cluster.Spec.Databases[0].TableGroups[0].Default = true
			if tc.poolPolicy != nil {
				shard := &cluster.Spec.Databases[0].TableGroups[0].Shards[0]
				shard.Overrides = &multigresv1alpha1.ShardOverrides{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {PVCDeletionPolicy: tc.poolPolicy},
					},
				}
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(coreTpl, cellTpl, shardTpl, cluster).
				WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
				Build()
			store := newClusterTopologyMemoryStore(t)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(200),
				CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{store}, nil
				},
				ListDesiredTableGroups: func(
					context.Context, *multigresv1alpha1.MultigresCluster, string,
				) ([]multiadmin.TableGroup, error) {
					return []multiadmin.TableGroup{{
						ID: 7, Name: "commerce", Shards: []string{"0-inf"},
						State: multiadmin.StateCreating,
					}}, nil
				},
			}
			key := types.NamespacedName{Name: clusterName, Namespace: namespace}
			tableGroups := func() []multigresv1alpha1.TableGroup {
				t.Helper()
				list := &multigresv1alpha1.TableGroupList{}
				require.NoError(t, c.List(ctx, list, client.InNamespace(namespace)))
				require.Len(t, list.Items, 2, "want the declared and the catalog TableGroup")
				return list.Items
			}
			// kept is whether every claim of tg stays when its Shards go.
			kept := func(tg *multigresv1alpha1.TableGroup) bool {
				require.NotEmpty(t, tg.Spec.Shards)
				for i := range tg.Spec.Shards {
					require.NotEmpty(t, tg.Spec.Shards[i].Pools)
					if !retainedThroughout(
						&tg.Spec.Shards[i],
						tg.Spec.PVCDeletionPolicy,
						retainAll,
					) {
						return false
					}
				}
				return true
			}
			for range 3 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
			}
			var dynamic int
			for _, tg := range tableGroups() {
				require.False(t, kept(&tg), "%s keeps its claims before anyone asked", tg.Name)
				if isDynamic(&tg) {
					dynamic++
				}
			}
			require.Equal(t, 1, dynamic)

			// The template is lost, the backup spec is refused, and the
			// cluster is told to keep its claims.
			require.NoError(t, c.Delete(ctx, shardTpl))
			stored := &multigresv1alpha1.MultigresCluster{}
			require.NoError(t, c.Get(ctx, key, stored))
			stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket: "bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
				},
			}
			stored.Spec.PVCDeletionPolicy = retainAll
			require.NoError(t, c.Update(ctx, stored))

			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.Error(t, err, "a shard that cannot be resolved is reported")
			for _, tg := range tableGroups() {
				require.Equal(t, retainAll, tg.Spec.PVCDeletionPolicy,
					"%s: the cluster's Retain did not reach the TableGroup's own level", tg.Name)
				assert.Equal(t, tc.kept, kept(&tg), tg.Name)
				if tc.kept {
					assert.NotContains(t, err.Error(), "'"+tg.Name+"'")
				} else {
					assert.ErrorContains(t, err, "tablegroup '"+tg.Name+"' shard ")
					assert.ErrorContains(t, err, "those PVCs are not kept")
				}
			}
			require.NoError(t, c.Get(ctx, key, stored))
			require.True(t, meta.IsStatusConditionFalse(
				stored.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid))
		})
	}
}
