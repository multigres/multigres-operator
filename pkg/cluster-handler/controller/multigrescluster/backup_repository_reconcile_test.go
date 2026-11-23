package multigrescluster

import (
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
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

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

			// Retiring the managed topology waits on a Healthy cluster and
			// says so in an event. A refused cluster is not on its way to
			// Healthy, so the only thing it reports is the refusal.
			events := eventsNaming("BackupRepositoryInvalid", "GlobalTopoRetire")
			require.Len(t, events, 1,
				"the refusal is reported once, and nothing else is: %v", events)
			assert.Contains(t, events[0], "BackupRepositoryInvalid")
			assert.Contains(t, events[0], tc.wantMessage)

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
