package shard

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cert"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// pvcOwnerRefFixture is a shard with one live pool ("main", cells z1 and z2)
// and the PVCs a shard of that identity can find under its four identity
// labels: its own, built by the production builders with no ownerRef as under
// a Retain policy, and strangers that only share the labels.
type pvcOwnerRefFixture struct {
	r     *ShardReconciler
	shard *multigresv1alpha1.Shard
	// own are the PVCs the shard made, which must follow the policy.
	own map[string]*corev1.PersistentVolumeClaim
	// strangers are PVCs the shard did not make, which it must never adopt.
	strangers map[string]*corev1.PersistentVolumeClaim
	// adoptedStranger carries this Shard's controller ref without having a
	// builder name, the state an unconditional adoption leaves behind.
	adoptedStranger *corev1.PersistentVolumeClaim
	// orphan is the shard's own PVC marked for the GC cronjob.
	orphan *corev1.PersistentVolumeClaim
}

func newPVCOwnerRefFixture(t *testing.T) *pvcOwnerRefFixture {
	t.Helper()
	scheme := reloadTestScheme(t)
	shard := newTestShard()
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1", "z2"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
	}

	f := &pvcOwnerRefFixture{
		shard:     shard,
		own:       map[string]*corev1.PersistentVolumeClaim{},
		strangers: map[string]*corev1.PersistentVolumeClaim{},
	}
	addOwn := func(pvc *corev1.PersistentVolumeClaim, err error) {
		t.Helper()
		require.NoError(t, err)
		require.Empty(t, pvc.OwnerReferences)
		f.own[pvc.Name] = pvc
	}
	addOwn(BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme))
	addOwn(BuildPoolDataPVC(shard, "main", "z2", pool, 0, false, scheme))
	// A surge or scale up index beyond the pool's replica count.
	addOwn(BuildPoolDataPVC(shard, "main", "z1", pool, 12, false, scheme))
	// A pool since removed from the spec; it follows the shard-level policy.
	addOwn(BuildPoolDataPVC(shard, "gone", "z1", pool, 0, false, scheme))
	addOwn(BuildSharedBackupPVC(shard, false, scheme))

	// A per-cell backup PVC of a shard whose backup repository is
	// cell-scoped. No production builder emits one any more, so this is the
	// shared backup PVC relabelled and renamed into that shape.
	legacy, err := BuildSharedBackupPVC(shard, false, scheme)
	require.NoError(t, err)
	legacy.Name = legacyCellBackupPVCName(shard, "z1")
	metadata.AddCellLabel(legacy.Labels, "z1")
	addOwn(legacy, nil)

	identity := func(name string) *corev1.PersistentVolumeClaim {
		backup, err := BuildSharedBackupPVC(shard, false, scheme)
		require.NoError(t, err)
		backup.Name = name
		return backup
	}
	addStranger := func(pvc *corev1.PersistentVolumeClaim) {
		f.strangers[pvc.Name] = pvc
	}
	// The shared backup PVC's shape under another name.
	addStranger(identity("impostor-shared-pvc"))
	// A pool data PVC's labels under a name no index produces.
	dataLike, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
	require.NoError(t, err)
	dataLike.Name = BuildPoolDataPVCName(shard, "main", "z1", 0) + "-restored"
	addStranger(dataLike)
	// A builder name, but labelled for a different cell than it names.
	crossCell, err := BuildPoolDataPVC(shard, "main", "z2", pool, 0, false, scheme)
	require.NoError(t, err)
	crossCell.Name = BuildPoolDataPVCName(shard, "main", "z1", 7)
	addStranger(crossCell)
	// Already controlled by another owner.
	controlled := identity("controlled-by-another")
	controlled.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "ConfigMap",
		Name:       "someone-else",
		UID:        "someone-else-uid",
		Controller: new(true),
	}}
	addStranger(controlled)

	f.adoptedStranger, err = BuildSharedBackupPVC(shard, true, scheme)
	require.NoError(t, err)
	f.adoptedStranger.Name = "adopted-stranger"
	require.True(t, metav1.IsControlledBy(f.adoptedStranger, shard))

	f.orphan, err = BuildPoolDataPVC(shard, "main", "z2", pool, 1, false, scheme)
	require.NoError(t, err)
	f.orphan.Labels[metadata.LabelOrphan] = "20260101T000000Z"

	objs := []client.Object{shard, f.adoptedStranger, f.orphan}
	for _, pvc := range f.own {
		objs = append(objs, pvc)
	}
	for _, pvc := range f.strangers {
		objs = append(objs, pvc)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	f.r = &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}

	// The fake client assigns resourceVersions on create, so the strangers are
	// re-read to compare against what the reconcile left.
	for name, pvc := range f.strangers {
		got := &corev1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pvc), got))
		f.strangers[name] = got
	}
	return f
}

func (f *pvcOwnerRefFixture) get(
	t *testing.T,
	pvc *corev1.PersistentVolumeClaim,
) *corev1.PersistentVolumeClaim {
	t.Helper()
	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.r.Get(t.Context(), client.ObjectKeyFromObject(pvc), got))
	return got
}

// requireStrangersUntouched checks every stranger is byte-for-byte what the
// fixture created: not adopted, and not written to at all.
func (f *pvcOwnerRefFixture) requireStrangersUntouched(t *testing.T) {
	t.Helper()
	for name, want := range f.strangers {
		got := f.get(t, want)
		require.Equal(t, want.ResourceVersion, got.ResourceVersion, "stranger %s was written", name)
		require.Equal(t, want.OwnerReferences, got.OwnerReferences, "stranger %s owners", name)
	}
	require.Empty(t, f.get(t, f.orphan).OwnerReferences, "orphan-labelled PVC adopted")
}

func TestReconcilePVCOwnerRefsAdoptsOnlyTheShardsOwnPVCs(t *testing.T) {
	f := newPVCOwnerRefFixture(t)
	retain := multigresv1alpha1.RetainPVCRetentionPolicy
	del := multigresv1alpha1.DeletePVCRetentionPolicy

	// Each phase sets the shard-level and the "main" pool override policy
	// together in one update, the way one spec edit lands, and the sequence
	// goes out and back so each transition is also exercised in reverse.
	phases := []struct {
		name       string
		shardLevel multigresv1alpha1.PVCRetentionPolicyType
		mainPool   multigresv1alpha1.PVCRetentionPolicyType // "" for no override
	}{
		// First, so the stranger already carrying this Shard's ref meets a
		// Delete policy before anything else could have released it.
		{"retain to delete", del, ""},
		{"delete back to retain", retain, ""},
		{"retain to delete again", del, ""},
		{"shard delete, main pool retain override", del, retain},
		{"shard retain, main pool delete override", retain, del},
		{"override dropped, delete", del, ""},
	}
	for _, phase := range phases {
		f.shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: phase.shardLevel,
		}
		pool := f.shard.Spec.Pools["main"]
		pool.PVCDeletionPolicy = nil
		if phase.mainPool != "" {
			pool.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: phase.mainPool,
			}
		}
		f.shard.Spec.Pools["main"] = pool

		require.NoError(t, f.r.reconcilePVCOwnerRefs(t.Context(), f.shard), phase.name)

		mainDelete := phase.shardLevel == del
		if phase.mainPool != "" {
			mainDelete = phase.mainPool == del
		}
		for name, pvc := range f.own {
			want := phase.shardLevel == del
			if pvc.Labels[metadata.LabelMultigresPool] == "main" {
				want = mainDelete
			}
			require.Equal(t, want, metav1.IsControlledBy(f.get(t, pvc), f.shard),
				"%s: own PVC %s controller ref", phase.name, name)
		}
		f.requireStrangersUntouched(t)
		require.Empty(t, f.get(t, f.adoptedStranger).OwnerReferences,
			"%s: a stranger carrying this Shard's ref must be released", phase.name)
	}
}

// A PVC retained from a deleted Shard of the same identity is the new Shard's
// storage, since its pods mount it by name, so it is adopted under Delete.
func TestReconcilePVCOwnerRefsAdoptsPreviousIncarnationsPVC(t *testing.T) {
	scheme := reloadTestScheme(t)
	previous := newTestShard()
	previous.UID = "previous-incarnation"
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	previous.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	data, err := BuildPoolDataPVC(previous, "main", "z1", pool, 0, false, scheme)
	require.NoError(t, err)
	backup, err := BuildSharedBackupPVC(previous, false, scheme)
	require.NoError(t, err)

	current := previous.DeepCopy()
	current.UID = "current-incarnation"
	current.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current, data, backup).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}

	require.NoError(t, r.reconcilePVCOwnerRefs(t.Context(), current))
	for _, pvc := range []*corev1.PersistentVolumeClaim{data, backup} {
		got := &corev1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pvc), got))
		require.True(t, metav1.IsControlledBy(got, current), "%s not adopted", pvc.Name)
	}
}

func TestIsShardBuiltPVC(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := newTestShard()
	pool := newTestPoolSpec()
	data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 3, false, scheme)
	require.NoError(t, err)
	backup, err := BuildSharedBackupPVC(shard, false, scheme)
	require.NoError(t, err)
	prefix := strings.TrimSuffix(data.Name, "3")

	other := newTestShard()
	other.Spec.ShardName = "other"
	otherData, err := BuildPoolDataPVC(other, "main", "z1", pool, 3, false, scheme)
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim)
		from   *corev1.PersistentVolumeClaim
		want   bool
	}{
		{"pool data PVC", func(*corev1.PersistentVolumeClaim) {}, data, true},
		{"shared backup PVC", func(*corev1.PersistentVolumeClaim) {}, backup, true},
		{"legacy per-cell backup PVC", func(p *corev1.PersistentVolumeClaim) {
			p.Name = legacyCellBackupPVCName(shard, "z1")
			p.Labels[metadata.LabelMultigresCell] = "z1"
		}, backup, true},
		{"legacy backup name without its cell label", func(p *corev1.PersistentVolumeClaim) {
			p.Name = legacyCellBackupPVCName(shard, "z1")
		}, backup, false},
		{"backup name with a pool label", func(p *corev1.PersistentVolumeClaim) {
			p.Labels[metadata.LabelMultigresPool] = "main"
			p.Labels[metadata.LabelMultigresCell] = "z1"
		}, backup, false},
		{"other name without a pool label", func(p *corev1.PersistentVolumeClaim) {
			p.Name = "impostor-shared-pvc"
		}, backup, false},
		{"data name without a cell label", func(p *corev1.PersistentVolumeClaim) {
			delete(p.Labels, metadata.LabelMultigresCell)
		}, data, false},
		{"data name for another cell", func(p *corev1.PersistentVolumeClaim) {
			p.Labels[metadata.LabelMultigresCell] = "z2"
		}, data, false},
		{"data name for another pool", func(p *corev1.PersistentVolumeClaim) {
			p.Labels[metadata.LabelMultigresPool] = "spare"
		}, data, false},
		{"another shard's data name", func(*corev1.PersistentVolumeClaim) {}, otherData, false},
		{
			"zero-padded index",
			func(p *corev1.PersistentVolumeClaim) { p.Name = prefix + "03" },
			data,
			false,
		},
		{
			"negative index",
			func(p *corev1.PersistentVolumeClaim) { p.Name = prefix + "-3" },
			data,
			false,
		},
		{
			"signed index",
			func(p *corev1.PersistentVolumeClaim) { p.Name = prefix + "+3" },
			data,
			false,
		},
		{"no index", func(p *corev1.PersistentVolumeClaim) { p.Name = prefix }, data, false},
		{
			"trailing text",
			func(p *corev1.PersistentVolumeClaim) { p.Name = prefix + "3-x" },
			data,
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvc := tc.from.DeepCopy()
			tc.mutate(pvc)
			require.Equal(t, tc.want, isShardBuiltPVC(shard, pvc))
		})
	}
}

// The names below are what BuildSharedBackupPVCName returned for these inputs
// while the backup repository was cell-scoped and the function took a cell.
// They are literals so that a change to legacyCellBackupPVCName cannot also
// change what the test expects.
func TestLegacyCellBackupPVCNameMatchesTheCellScopedBuilder(t *testing.T) {
	short := newTestShard()
	require.Equal(t,
		"backup-data-test-cluster-postgres-default-0-inf-z1-fef9c6a4",
		legacyCellBackupPVCName(short, "z1"))

	long := newTestShard()
	long.Labels["multigres.com/cluster"] = strings.Repeat("c", 40)
	long.Spec.TableGroupName = multigresv1alpha1.TableGroupName(strings.Repeat("t", 30))
	require.Equal(t,
		"backup-data-cccccccccccccccccccccccccccccccccccccccc---664dd5fd",
		legacyCellBackupPVCName(long, "zone-a"))
}

// A shard whose sidecar location is refused reconciles nothing, except that
// it lets go of the PVCs its spec says to keep: they are garbage-collected
// with the shard while it owns them, so a Retain set on a shard that cannot
// be repaired has to arrive before the shard is deleted. A refused shard
// never takes ownership, which is what would make a claim deletable.
func TestRefusedShardStillReleasesPVCsForRetain(t *testing.T) {
	ctx := t.Context()
	scheme := reloadTestScheme(t)
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	shard := newTestShard()
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	refused := &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/data/x"},
	}
	shard.Spec.SidecarBackupLocation = refused
	shard.Spec.Backup = refused.DeepCopy()
	require.Error(t, validateSidecarLocation(shard), "the fixture must be a refused shard")
	del := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	retain := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	shard.Spec.PVCDeletionPolicy = del
	data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, true, scheme)
	require.NoError(t, err)
	backup, err := BuildSharedBackupPVC(shard, true, scheme)
	require.NoError(t, err)
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: shard.Namespace},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, data, backup, password).
		WithStatusSubresource(shard).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), APIReader: c,
	}
	owned := func() map[string]bool {
		t.Helper()
		current := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), current))
		out := map[string]bool{}
		for _, pvc := range []*corev1.PersistentVolumeClaim{data, backup} {
			got := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), got))
			out[pvc.Name] = metav1.IsControlledBy(got, current)
		}
		return out
	}
	setPolicy := func(policy *multigresv1alpha1.PVCDeletionPolicy) {
		t.Helper()
		current := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), current))
		current.Spec.PVCDeletionPolicy = policy
		require.NoError(t, c.Update(ctx, current))
		_, err := r.Reconcile(ctx, ctrlRequest(shard))
		require.NoError(t, err)
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), current))
		require.True(t, meta.IsStatusConditionFalse(current.Status.Conditions,
			multigresv1alpha1.ConditionBackupRepositoryValid), "the shard must be refused")
	}
	both := map[string]bool{data.Name: true, backup.Name: true}
	neither := map[string]bool{data.Name: false, backup.Name: false}

	setPolicy(del)
	require.Equal(t, both, owned())
	setPolicy(retain)
	require.Equal(t, neither, owned(), "a refused shard kept PVCs its spec says to retain")
	setPolicy(del)
	require.Equal(t, neither, owned(), "a refused shard took ownership of PVCs")
}

// A refused shard's pods keep running, so what they depend on keeps being
// reconciled: the pgBackRest certificate they mount, and the readiness gate
// Kubernetes and the PodDisruptionBudget read. The gate of a pod that can no
// longer be observed must not stay at the last verdict from before the
// refusal.
func TestRefusedShardKeepsCertificatesAndReadinessCurrent(t *testing.T) {
	ctx := t.Context()
	scheme := reloadTestScheme(t)
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	shard := newTestShard()
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"cell1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	accepted := shard.DeepCopy()
	filesystemBackups(accepted)
	pod, err := BuildPoolPod(accepted, "main", "cell1", pool, 0, scheme)
	require.NoError(t, err)
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: PoolerDataReadyCondition, Status: corev1.ConditionTrue, Reason: "Serving",
	}}
	refused := &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/data/x"},
	}
	shard.Spec.SidecarBackupLocation = refused
	shard.Spec.Backup = refused.DeepCopy()
	require.Error(t, validateSidecarLocation(shard), "the fixture must be a refused shard")
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: shard.Namespace},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, password).
		WithStatusSubresource(shard, pod).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), APIReader: c,
		PoolerClients:   poolerclient.Static(rpcclient.NewFakeClient()),
		CreateTopoStore: newMemoryTopoFactory(),
	}

	result, err := r.Reconcile(ctx, ctrlRequest(shard))
	require.NoError(t, err)

	current := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), current))
	require.True(t, meta.IsStatusConditionFalse(current.Status.Conditions,
		multigresv1alpha1.ConditionBackupRepositoryValid), "the shard must be refused")

	got := &corev1.Pod{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), got))
	gate := findPoolerReadinessCondition(got.Status.Conditions)
	require.NotNil(t, gate)
	require.Equal(t, corev1.ConditionFalse, gate.Status,
		"a pooler that is not registered in topology stayed ready on a refused shard")
	require.Positive(t, result.RequeueAfter, "the unobserved pod is looked at again")

	require.NoError(t, c.Get(ctx, client.ObjectKey{
		Namespace: shard.Namespace, Name: shard.Name + "-pgbackrest-tls",
	}, &corev1.Secret{}), "the certificate of a refused shard is not reconciled")

	pods := &corev1.PodList{}
	require.NoError(t, c.List(ctx, pods, client.InNamespace(shard.Namespace)))
	require.Len(t, pods.Items, 1, "a refused shard creates nothing")
}

// A Shard written by an operator from before the sidecar has a backup block,
// no sidecar image and no sidecar location. When its cluster cannot be
// accepted it stays that way, refused, and its pods keep running and keep
// mounting the certificate that operator generated. That certificate is
// renewed from the backup block's TLS selection, and no pod is replaced for
// it.
func TestRefusedShardFromBeforeTheSidecarRenewsItsCertificate(t *testing.T) {
	ctx := t.Context()
	scheme := reloadTestScheme(t)
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	shard := newTestShard()
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"cell1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	current := shard.DeepCopy()
	filesystemBackups(current)
	pod, err := BuildPoolPod(current, "main", "cell1", pool, 0, scheme)
	require.NoError(t, err)
	pod.UID = "pod-from-before"

	shard.Spec.Images.PgBackRestd = ""
	shard.Spec.SidecarBackupLocation = nil
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups"},
	}
	require.Error(t, validateSidecarLocation(shard), "the fixture must be a refused shard")
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: shard.Namespace},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, password).
		WithStatusSubresource(shard, pod).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), APIReader: c,
		PoolerClients:   poolerclient.Static(rpcclient.NewFakeClient()),
		CreateTopoStore: newMemoryTopoFactory(),
	}

	// The certificate as the earlier operator left it, with ten days to go.
	seed := shard.DeepCopy()
	seed.Spec.SidecarBackupLocation = seed.Spec.Backup
	require.NoError(t, r.reconcilePgBackRestCerts(ctx, seed))
	caKey := client.ObjectKey{Namespace: shard.Namespace, Name: shard.Name + "-pgbackrest-ca"}
	tlsKey := client.ObjectKey{Namespace: shard.Namespace, Name: shard.Name + "-pgbackrest-tls"}
	caSecret, tlsSecret := &corev1.Secret{}, &corev1.Secret{}
	require.NoError(t, c.Get(ctx, caKey, caSecret))
	require.NoError(t, c.Get(ctx, tlsKey, tlsSecret))
	ca, err := cert.ParseCA(caSecret.Data["ca.crt"], caSecret.Data["ca.key"])
	require.NoError(t, err)
	notAfter := func(certPEM []byte) time.Time {
		t.Helper()
		block, _ := pem.Decode(certPEM)
		require.NotNil(t, block)
		parsed, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		return parsed.NotAfter
	}
	block, _ := pem.Decode(tlsSecret.Data["tls.crt"])
	require.NotNil(t, block)
	template, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	template.NotAfter = time.Now().Add(10 * 24 * time.Hour)
	der, err := x509.CreateCertificate(
		rand.Reader, template, ca.Cert, template.PublicKey, ca.Key)
	require.NoError(t, err)
	tlsSecret.Data["tls.crt"] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, c.Update(ctx, tlsSecret))
	require.Less(t, time.Until(notAfter(tlsSecret.Data["tls.crt"])), cert.RotationThreshold)

	_, err = r.Reconcile(ctx, ctrlRequest(shard))
	require.NoError(t, err)

	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
	require.True(t, meta.IsStatusConditionFalse(stored.Status.Conditions,
		multigresv1alpha1.ConditionBackupRepositoryValid), "the shard must be refused")
	require.NoError(t, c.Get(ctx, tlsKey, tlsSecret))
	require.Greater(t, time.Until(notAfter(tlsSecret.Data["tls.crt"])), 300*24*time.Hour,
		"the certificate the running pods mount was not renewed")
	pods := &corev1.PodList{}
	require.NoError(t, c.List(ctx, pods, client.InNamespace(shard.Namespace)))
	require.Len(t, pods.Items, 1)
	require.Equal(t, pod.UID, pods.Items[0].UID, "the pod was replaced")
	require.Empty(t, pods.Items[0].Annotations[metadata.AnnotationDrainState])
}
