package shard

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestValidateStorageClassDependencies(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	newReconciler := func(objs ...client.Object) *ShardReconciler {
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objs...).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		return &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	}

	filesystemBackup := func(class string) *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Storage: multigresv1alpha1.StorageSpec{Class: class},
			},
		}
	}

	t.Run("nothing explicit reports one not-specified verdict", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
				},
			},
		}
		r := newReconciler(shard)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionTrue || check.reason != storageClassNotSpecifiedReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
		if check.backupDependency != nil || check.poolDependency != nil {
			t.Fatalf("expected no dependency errors, got %+v", check)
		}
	})

	t.Run("explicit classes all present report found", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: filesystemBackup("backup-sc"),
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "fast"},
					},
				},
			},
		}
		r := newReconciler(
			shard,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "backup-sc"}},
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "fast"}},
		)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionTrue || check.reason != storageClassFoundReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
	})

	// One writer means one verdict, and in the mixed cases the merged verdict
	// reports Found where the old pair reported NotSpecified: the pool validator
	// ran last and claimed the shard as unconfigured even when the backup class
	// was explicit. The reason is published on the condition, so both directions
	// of "some of it is explicit" are pinned here.
	t.Run("explicit backup class with no pool class reports found", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: filesystemBackup("backup-sc"),
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
				},
			},
		}
		r := newReconciler(
			shard,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "backup-sc"}},
		)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionTrue || check.reason != storageClassFoundReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
	})

	t.Run("explicit pool class with no backup class reports found", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "fast"},
					},
				},
			},
		}
		r := newReconciler(
			shard,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "fast"}},
		)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionTrue || check.reason != storageClassFoundReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
	})

	t.Run("missing backup class reports the backup dependency", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec:       multigresv1alpha1.ShardSpec{Backup: filesystemBackup("missing-sc")},
		}
		r := newReconciler(shard)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionFalse || check.reason != storageClassNotFoundReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
		if !isMissingStorageClassDependency(check.backupDependency) {
			t.Fatalf("expected backup dependency error, got %v", check.backupDependency)
		}
		if check.poolDependency != nil {
			t.Fatalf("expected no pool dependency error, got %v", check.poolDependency)
		}
		if !strings.Contains(check.message, `"missing-sc"`) {
			t.Fatalf("message must name the class, got %q", check.message)
		}
	})

	t.Run("missing pool class reports the pool dependency", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "missing-sc"},
					},
				},
			},
		}
		r := newReconciler(shard)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if check.status != metav1.ConditionFalse || check.reason != storageClassNotFoundReason {
			t.Fatalf("unexpected verdict: %+v", check)
		}
		if !isMissingStorageClassDependency(check.poolDependency) {
			t.Fatalf("expected pool dependency error, got %v", check.poolDependency)
		}
		if check.backupDependency != nil {
			t.Fatalf("expected no backup dependency error, got %v", check.backupDependency)
		}
		if !strings.Contains(check.message, "primary") {
			t.Fatalf("message must name the pool, got %q", check.message)
		}
	})

	t.Run("missing backup class wins over a missing pool class", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: filesystemBackup("missing-backup-sc"),
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Storage: multigresv1alpha1.StorageSpec{
							Size:  "10Gi",
							Class: "missing-pool-sc",
						},
					},
				},
			},
		}
		r := newReconciler(shard)

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !isMissingStorageClassDependency(check.backupDependency) ||
			check.poolDependency != nil {
			t.Fatalf("expected only the backup dependency, got %+v", check)
		}
	})

	// Spec.Pools is a map, so an unordered scan would report whichever missing
	// pool Go's iteration reached first and flap the condition message.
	t.Run("the reported pool is stable across calls", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"aaa": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "missing-a"},
					},
					"bbb": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "missing-b"},
					},
					"ccc": {
						Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "missing-c"},
					},
				},
			},
		}
		r := newReconciler(shard)

		want := ""
		for i := range 30 {
			check, err := r.validateStorageClassDependencies(t.Context(), shard)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i == 0 {
				want = check.message
			}
			if check.message != want {
				t.Fatalf("message flapped: %q then %q", want, check.message)
			}
		}
		if !strings.Contains(want, "aaa") {
			t.Fatalf("expected the first pool by name, got %q", want)
		}
	})
}

// TestSetStorageClassCondition_AppliesOnlyConditions pins the defect that made
// a healthy shard rewrite its status forever. The guard's apply payload is a
// complete statement of what its field manager owns, so it must carry
// status.conditions and nothing else: a payload built from a typed ShardStatus
// literal also serialises orchReady:false and poolsReady:false, and with
// ForceOwnership it seizes both from updateStatus on every reconcile.
//
// The assertion is on the serialised payload rather than on the Go value,
// because the Go value looks correct in both the fixed and the broken version.
// The zero values only become an assertion once they are marshalled.
//
// It cannot be made against the object after a round trip here: the fake client
// does not scope an apply patch to the payload's fields, it replaces the whole
// status, so orchReady and poolsReady are lost either way. The round-trip half
// of this belongs to an apiserver, and lives in TestShardStatusQuiesces under
// test/suite.
func TestSetStorageClassCondition_AppliesOnlyConditions(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
		Status: multigresv1alpha1.ShardStatus{
			OrchReady:  true,
			PoolsReady: true,
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	var captured []byte
	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnStatusPatch: func(obj client.Object) error {
			raw, err := json.Marshal(obj)
			if err != nil {
				t.Fatalf("marshal patch payload: %v", err)
			}
			captured = raw
			return nil
		},
	})

	r := &ShardReconciler{Client: fakeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	check := storageClassCheck{
		status:  metav1.ConditionTrue,
		reason:  storageClassNotSpecifiedReason,
		message: "No explicit backup filesystem or pool StorageClass configured; using cluster default",
	}
	if err := r.setStorageClassCondition(t.Context(), shard, check); err != nil {
		t.Fatalf("setStorageClassCondition: %v", err)
	}
	if captured == nil {
		t.Fatal("guard did not apply a status patch")
	}

	var payload struct {
		Status map[string]json.RawMessage `json:"status"`
	}
	if err := json.Unmarshal(captured, &payload); err != nil {
		t.Fatalf("unmarshal patch payload: %v", err)
	}
	keys := slices.Sorted(maps.Keys(payload.Status))
	if !slices.Equal(keys, []string{"conditions"}) {
		t.Fatalf("guard payload must own status.conditions only, got %v", keys)
	}

	var conditions []metav1.Condition
	if err := json.Unmarshal(payload.Status["conditions"], &conditions); err != nil {
		t.Fatalf("unmarshal conditions: %v", err)
	}
	if len(conditions) != 1 || conditions[0].Type != conditionStorageClassValid {
		t.Fatalf("guard payload must carry exactly the %s condition, got %+v",
			conditionStorageClassValid, conditions)
	}
	if conditions[0].Status != check.status || conditions[0].Reason != check.reason ||
		conditions[0].Message != check.message {
		t.Fatalf("condition does not match the verdict: %+v", conditions[0])
	}
}

// TestStorageClassCondition_IsStableAcrossReconciles pins the second defect:
// two validations wrote the same condition type with different messages, so
// every reconcile rewrote the other's and the condition never settled. Once it
// has one writer, the second cycle must not write at all.
//
// This drives the guard cycle directly rather than Reconcile, because a full
// reconcile calls updateStatus again afterwards and the fake client's apply
// drops the guard's condition when it does (see
// TestSetStorageClassCondition_AppliesOnlyConditions). The full-reconcile form
// of this assertion is TestShardStatusQuiesces under test/suite.
func TestStorageClassCondition_IsStableAcrossReconciles(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	// No explicit StorageClass anywhere: the case both validations used to
	// claim with a message of their own.
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	patches := 0
	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnStatusPatch: func(client.Object) error {
			patches++
			return nil
		},
	})

	r := &ShardReconciler{Client: fakeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}

	reconcileStorageClasses := func() []byte {
		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		if err := r.setStorageClassCondition(t.Context(), shard, check); err != nil {
			t.Fatalf("set condition: %v", err)
		}

		var got multigresv1alpha1.Shard
		if err := baseClient.Get(t.Context(), client.ObjectKeyFromObject(shard), &got); err != nil {
			t.Fatalf("read shard: %v", err)
		}
		cond := findCondition(got.Status.Conditions, conditionStorageClassValid)
		if cond == nil {
			t.Fatalf("no %s condition", conditionStorageClassValid)
		}
		raw, err := json.Marshal(cond)
		if err != nil {
			t.Fatalf("marshal condition: %v", err)
		}
		return raw
	}

	first := reconcileStorageClasses()
	if patches != 1 {
		t.Fatalf("first cycle must apply the condition once, applied %d times", patches)
	}

	second := reconcileStorageClasses()
	if string(first) != string(second) {
		t.Fatalf(
			"StorageClassValid condition moved between reconciles:\n  %s\n  %s",
			first,
			second,
		)
	}
	if patches != 1 {
		t.Fatalf("second cycle rewrote a settled condition: %d applies total", patches)
	}
}

// guardStatusApply is one status apply captured under the guard's field
// owner: the top-level keys of the applied status object, and the single
// StorageClassValid condition it carried.
type guardStatusApply struct {
	statusKeys []string
	condition  metav1.Condition
}

// TestReconcile_StorageClassConditionSettlesAcrossReconciles drives the real
// ShardReconciler.Reconcile, not the guard cycle in isolation, because the hot
// loop only existed when the guard's apply and updateStatus's apply landed
// against the same object in the same reconcile: a reviewer showed that
// reintroducing a second writer for StorageClassValid (e.g. splitting the
// backup and pool checks back into two condition-setting calls) passes
// TestStorageClassCondition_IsStableAcrossReconciles above, because that test
// drives the merged validateStorageClassDependencies directly and never
// exercises whatever calls the write path from within one Reconcile pass.
//
// It captures every guard-owned status apply's payload directly, identified
// by the real client.SubResourcePatchOptions.FieldManager rather than by
// payload shape, so the assertions hold regardless of how the guard's payload
// happens to be built.
//
// It does not assert that the second reconcile makes zero guard applies, even
// though on a real API server it would: the fake client's SSA implementation
// (managedfields.DeducedTypeConverter, since the Shard CRD's structural schema
// isn't available to it) treats the +listType=map Conditions slice as atomic,
// so updateStatus's own apply - a different field owner, earlier in the same
// Reconcile - replaces status.conditions wholesale and erases whatever the
// guard wrote in the previous reconcile. That forces the guard to see no
// existing condition and reapply every single call, on both fixed and broken
// code, which is exactly why TestStorageClassCondition_IsStableAcrossReconciles
// avoids driving Reconcile for its own assertion. What is real and worth
// pinning here: each individual Reconcile call makes at most one guard apply
// (a second writer within one pass would make two), and the verdict it writes
// does not flap from one reconcile to the next. The true single-writer,
// settles-for-real proof against a schema-aware apply is
// TestShardStatusQuiesces under test/suite.
func TestReconcile_StorageClassConditionSettlesAcrossReconciles(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	// No explicit StorageClass anywhere: the case the guard and updateStatus
	// used to fight over.
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "stable-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "replica",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}
	setTestPostgresPasswordSecretRef(shard)

	var guardApplies []guardStatusApply
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, testPostgresPasswordSecretForShard(shard)).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context,
				c client.Client,
				subResourceName string,
				obj client.Object,
				patch client.Patch,
				opts ...client.SubResourcePatchOption,
			) error {
				popts := (&client.SubResourcePatchOptions{}).ApplyOptions(opts)
				if popts.FieldManager == "multigres-resource-handler-guard" {
					raw, err := json.Marshal(obj)
					if err != nil {
						t.Fatalf("marshal guard payload: %v", err)
					}
					var payload struct {
						Status map[string]json.RawMessage `json:"status"`
					}
					if err := json.Unmarshal(raw, &payload); err != nil {
						t.Fatalf("unmarshal guard payload: %v", err)
					}
					var conditions []metav1.Condition
					if raw, ok := payload.Status["conditions"]; ok {
						if err := json.Unmarshal(raw, &conditions); err != nil {
							t.Fatalf("unmarshal guard conditions: %v", err)
						}
					}
					if len(conditions) != 1 {
						t.Fatalf(
							"guard payload must carry exactly one condition, got %+v",
							conditions,
						)
					}
					guardApplies = append(guardApplies, guardStatusApply{
						statusKeys: slices.Sorted(maps.Keys(payload.Status)),
						condition:  conditions[0],
					})
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	r := &ShardReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		CreateTopoStore: newMemoryTopoFactory(),
	}

	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}

	assertOnlyOwnsConditions := func(t *testing.T, apply guardStatusApply) {
		t.Helper()
		if !slices.Equal(apply.statusKeys, []string{"conditions"}) {
			t.Fatalf("guard payload must own status.conditions only, got %v", apply.statusKeys)
		}
		if apply.condition.Type != conditionStorageClassValid {
			t.Fatalf("guard payload must carry the %s condition, got %+v",
				conditionStorageClassValid, apply.condition)
		}
	}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if len(guardApplies) != 1 {
		t.Fatalf(
			"first reconcile must make exactly one guard apply (a second writer is back), got %d",
			len(guardApplies),
		)
	}
	first := guardApplies[0]
	assertOnlyOwnsConditions(t, first)
	if first.condition.Status != metav1.ConditionTrue ||
		first.condition.Reason != storageClassNotSpecifiedReason {
		t.Fatalf("unexpected first verdict: %+v", first.condition)
	}

	// The fake client's SSA status apply replaces the whole object rather than
	// scoping to the applied fields, which drops Spec. Restore it exactly as the
	// existing multi-reconcile loop in TestShardReconciler_Reconcile does, so the
	// second reconcile sees the same spec as the first rather than erroring on a
	// shard with no pools.
	var stored multigresv1alpha1.Shard
	if err := fakeClient.Get(t.Context(), req.NamespacedName, &stored); err != nil {
		t.Fatalf("read shard before restoring spec: %v", err)
	}
	stored.Spec = shard.Spec
	if err := fakeClient.Update(t.Context(), &stored); err != nil {
		t.Fatalf("restore shard spec: %v", err)
	}

	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(guardApplies) != 2 {
		t.Fatalf(
			"second reconcile must make exactly one guard apply of its own (a second writer "+
				"is back), got %d total",
			len(guardApplies),
		)
	}
	second := guardApplies[1]
	assertOnlyOwnsConditions(t, second)

	if second.condition.Status != first.condition.Status ||
		second.condition.Reason != first.condition.Reason ||
		second.condition.Message != first.condition.Message {
		t.Fatalf(
			"StorageClassValid verdict flapped between reconciles:\n  %+v\n  %+v",
			first.condition,
			second.condition,
		)
	}
}

func TestReconcile_MissingStorageClassReturnsDependencyRequeueEvenWhenPVCExists(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: testPostgresAuthRefName,
				Key:  PostgresPasswordSecretKey,
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(1)),
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Storage: multigresv1alpha1.StorageSpec{
						Size:  "10Gi",
						Class: "missing-sc",
					},
				},
			},
		},
	}

	existingPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildPoolDataPVCName(shard, "primary", "zone1", 0),
			Namespace: "default",
			Labels: buildPoolLabelsWithCell(
				shard,
				"primary",
				"zone1",
			),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, existingPVC, testPostgresPasswordSecretForShard(shard)).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	r := &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       c,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(shard),
	})
	if err != nil {
		t.Fatalf("expected non-error dependency requeue, got error: %v", err)
	}
	if result.RequeueAfter != storageClassDependencyRequeue {
		t.Fatalf("requeueAfter = %v, want %v", result.RequeueAfter, storageClassDependencyRequeue)
	}
}

func TestIsMissingStorageClassDependencyWrapped(t *testing.T) {
	err := errors.New("other")
	if isMissingStorageClassDependency(err) {
		t.Fatal("expected false for non-dependency error")
	}

	wrapped := errors.Join(errors.New("outer"), &missingStorageClassDependencyError{className: "x"})
	if !isMissingStorageClassDependency(wrapped) {
		t.Fatal("expected true for wrapped missing dependency error")
	}
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func TestShardReconciler_FieldOwnershipIsolation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("updateStatus patch contains only Available condition", func(t *testing.T) {
		t.Parallel()

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-field-owner",
				Namespace: "default",
				Labels: map[string]string{
					metadata.LabelMultigresCluster: "test-cluster",
				},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					Cells: []multigresv1alpha1.CellName{"zone1"},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Cells:           []multigresv1alpha1.CellName{"zone1"},
						Type:            "readWrite",
						Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
						ReplicasPerCell: ptr.To(int32(1)),
					},
				},
			},
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "primary", "zone1", 0),
				Namespace: "default",
				Labels: metadata.GetSelectorLabels(
					buildPoolLabelsWithCell(shard, "primary", "zone1"),
				),
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}

		moName := buildHashedMultiorchName(shard, "zone1")
		mo := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: moName, Namespace: "default"},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
			Status:     appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1},
		}

		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, mo).
			WithStatusSubresource(shard, pod, mo).
			Build()

		var capturedPatchObj client.Object
		fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnStatusPatch: func(obj client.Object) error {
				capturedPatchObj = obj
				return nil
			},
		})

		r := &ShardReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}

		if err := r.updateStatus(t.Context(), shard, renderedConfig{}); err != nil {
			t.Fatalf("updateStatus: %v", err)
		}

		patchShard, ok := capturedPatchObj.(*multigresv1alpha1.Shard)
		if !ok {
			t.Fatalf("expected *Shard patch, got %T", capturedPatchObj)
		}

		for _, c := range patchShard.Status.Conditions {
			if c.Type == conditionStorageClassValid {
				t.Fatalf(
					"updateStatus patch must not contain %s condition",
					conditionStorageClassValid,
				)
			}
		}
		availCond := findCondition(patchShard.Status.Conditions, "Available")
		if availCond == nil {
			t.Fatal("updateStatus patch must contain Available condition")
		}
	})
}

// storageClassGateScheme registers everything a full Reconcile of the gate
// fixture below touches.
func storageClassGateScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	return scheme
}

// storageClassGateShard is a shard that reconciles far enough to pass both
// storage-class gates, so a test can make exactly one of them fire by naming a
// class that does not exist.
func storageClassGateShard(backupClass, poolClass string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gate-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: testPostgresAuthRefName,
				Key:  PostgresPasswordSecretKey,
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: backupClass},
				},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					ReplicasPerCell: ptr.To(int32(1)),
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "readWrite",
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi", Class: poolClass},
				},
			},
		},
	}
}

func childExists(t *testing.T, c client.Client, obj client.Object, namespace, name string) bool {
	t.Helper()
	err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err == nil {
		return true
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error reading %T %s: %v", obj, name, err)
	}
	return false
}

// TestReconcile_MissingBackupStorageClassStopsBeforeTheSharedBackupPVC and its
// pool twin below pin where the two gates sit, not just that they fire. The
// returned RequeueAfter is the same wherever a gate is placed, so the only
// observable that moves when a gate moves is which children the pass created:
// a missing backup class must stop before the shared backup PVC is applied, and
// a missing pool class must stop after it and before anything that consumes
// pool storage.
func TestReconcile_MissingBackupStorageClassStopsBeforeTheSharedBackupPVC(t *testing.T) {
	scheme := storageClassGateScheme()
	shard := storageClassGateShard("missing-backup-sc", "")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, testPostgresPasswordSecretForShard(shard)).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	r := &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       c,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(shard),
	})
	if err != nil {
		t.Fatalf("expected non-error dependency requeue, got error: %v", err)
	}
	if result.RequeueAfter != storageClassDependencyRequeue {
		t.Fatalf("requeueAfter = %v, want %v", result.RequeueAfter, storageClassDependencyRequeue)
	}

	ns := shard.Namespace
	if !childExists(t, c, &corev1.ConfigMap{}, ns, PgHbaConfigMapName(shard.Name)) {
		t.Error("pg_hba ConfigMap is missing: the backup gate moved above the shared ConfigMaps")
	}
	if childExists(t, c, &appsv1.Deployment{}, ns, buildHashedMultiorchName(shard, "zone1")) {
		t.Error("Multiorch Deployment was created: the backup gate moved below the Multiorch block")
	}
	if childExists(t, c, &corev1.PersistentVolumeClaim{}, ns, BuildSharedBackupPVCName(shard)) {
		t.Error(
			"shared backup PVC was created against a StorageClass that does not exist: " +
				"the backup gate moved below the backup PVC block",
		)
	}
	if childExists(t, c, &corev1.ConfigMap{}, ns, PostgresConfigMapName(shard.Name)) {
		t.Error("postgres config ConfigMap was created: the reconcile ran past both gates")
	}
}

func TestReconcile_MissingPoolStorageClassStopsAfterTheSharedBackupPVC(t *testing.T) {
	scheme := storageClassGateScheme()
	shard := storageClassGateShard("backup-sc", "missing-pool-sc")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			shard,
			testPostgresPasswordSecretForShard(shard),
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "backup-sc"}},
		).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	r := &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		APIReader:       c,
		CreateTopoStore: newMemoryTopoFactory(),
	}

	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(shard),
	})
	if err != nil {
		t.Fatalf("expected non-error dependency requeue, got error: %v", err)
	}
	if result.RequeueAfter != storageClassDependencyRequeue {
		t.Fatalf("requeueAfter = %v, want %v", result.RequeueAfter, storageClassDependencyRequeue)
	}

	ns := shard.Namespace
	if !childExists(t, c, &appsv1.Deployment{}, ns, buildHashedMultiorchName(shard, "zone1")) {
		t.Error("Multiorch Deployment is missing: the pool gate moved above the Multiorch block")
	}
	if !childExists(t, c, &corev1.PersistentVolumeClaim{}, ns, BuildSharedBackupPVCName(shard)) {
		t.Error(
			"shared backup PVC is missing: a missing pool class must not stop the reconcile " +
				"before the backup PVC, whose own StorageClass is present",
		)
	}
	if childExists(t, c, &corev1.ConfigMap{}, ns, PostgresConfigMapName(shard.Name)) {
		t.Error("postgres config ConfigMap was created: the pool gate moved below it")
	}
	if childExists(t, c, &corev1.Pod{}, ns, BuildPoolPodName(shard, "primary", "zone1", 0)) {
		t.Error(
			"pool pod was created against a StorageClass that does not exist: " +
				"the pool gate no longer precedes the workloads that consume pool storage",
		)
	}
}

// persistStorageClassCondition writes a prior StorageClassValid verdict so a
// test can drive setStorageClassCondition against known persisted state. The
// generation is read back rather than assumed, because the skip compares the
// condition's observedGeneration against the object's.
func persistStorageClassCondition(
	t *testing.T,
	c client.Client,
	key client.ObjectKey,
	build func(generation int64) metav1.Condition,
) {
	t.Helper()

	var shard multigresv1alpha1.Shard
	if err := c.Get(t.Context(), key, &shard); err != nil {
		t.Fatalf("read shard: %v", err)
	}
	shard.Status.Conditions = []metav1.Condition{build(shard.Generation)}
	if err := c.Status().Update(t.Context(), &shard); err != nil {
		t.Fatalf("seed condition: %v", err)
	}
}

// TestSetStorageClassCondition_RepublishesWhenOneComparedFieldDiffers takes the
// skip-if-unchanged test apart conjunct by conjunct. Dropping any one of them
// widens the skip so it swallows a real change, and a case where several fields
// differ at once cannot see that, so each case here differs from the persisted
// condition in exactly one compared field.
//
// Some of these field combinations are not reachable verdicts: no production
// verdict pairs StorageClassFound with False. The unit under test is
// setStorageClassCondition, whose contract is per-field, and driving it with a
// storageClassCheck directly is what makes one field at a time possible.
func TestSetStorageClassCondition_RepublishesWhenOneComparedFieldDiffers(t *testing.T) {
	t.Parallel()

	const foundMessage = "All explicitly configured StorageClasses are present"
	settled := storageClassCheck{
		status:  metav1.ConditionTrue,
		reason:  storageClassFoundReason,
		message: foundMessage,
	}

	cases := []struct {
		name       string
		persisted  func(generation int64) metav1.Condition
		wantPatch  bool
		wantReason string
	}{
		{
			name: "status differs",
			persisted: func(generation int64) metav1.Condition {
				return metav1.Condition{
					Type:               conditionStorageClassValid,
					Status:             metav1.ConditionFalse,
					Reason:             storageClassFoundReason,
					Message:            foundMessage,
					ObservedGeneration: generation,
					LastTransitionTime: metav1.Now(),
				}
			},
			wantPatch: true,
		},
		{
			name: "reason differs",
			persisted: func(generation int64) metav1.Condition {
				return metav1.Condition{
					Type:               conditionStorageClassValid,
					Status:             metav1.ConditionTrue,
					Reason:             storageClassNotSpecifiedReason,
					Message:            foundMessage,
					ObservedGeneration: generation,
					LastTransitionTime: metav1.Now(),
				}
			},
			wantPatch: true,
		},
		{
			name: "message differs",
			persisted: func(generation int64) metav1.Condition {
				return metav1.Condition{
					Type:               conditionStorageClassValid,
					Status:             metav1.ConditionTrue,
					Reason:             storageClassFoundReason,
					Message:            "a message from an older build",
					ObservedGeneration: generation,
					LastTransitionTime: metav1.Now(),
				}
			},
			wantPatch: true,
		},
		{
			name: "observedGeneration differs",
			persisted: func(generation int64) metav1.Condition {
				return metav1.Condition{
					Type:               conditionStorageClassValid,
					Status:             metav1.ConditionTrue,
					Reason:             storageClassFoundReason,
					Message:            foundMessage,
					ObservedGeneration: generation - 1,
					LastTransitionTime: metav1.Now(),
				}
			},
			wantPatch: true,
		},
		{
			name: "nothing differs",
			persisted: func(generation int64) metav1.Condition {
				return metav1.Condition{
					Type:               conditionStorageClassValid,
					Status:             metav1.ConditionTrue,
					Reason:             storageClassFoundReason,
					Message:            foundMessage,
					ObservedGeneration: generation,
					LastTransitionTime: metav1.Now(),
				}
			},
			wantPatch: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = storagev1.AddToScheme(scheme)

			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
			}
			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(shard).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				Build()

			key := client.ObjectKeyFromObject(shard)
			persistStorageClassCondition(t, baseClient, key, tc.persisted)

			patches := 0
			fakeClient := testutil.NewFakeClientWithFailures(
				baseClient,
				&testutil.FailureConfig{
					OnStatusPatch: func(client.Object) error {
						patches++
						return nil
					},
				},
			)
			r := &ShardReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			if err := r.setStorageClassCondition(t.Context(), shard, settled); err != nil {
				t.Fatalf("setStorageClassCondition: %v", err)
			}

			want := 0
			if tc.wantPatch {
				want = 1
			}
			if patches != want {
				t.Fatalf("applied %d patches, want %d", patches, want)
			}
			if !tc.wantPatch {
				return
			}

			var got multigresv1alpha1.Shard
			if err := baseClient.Get(t.Context(), key, &got); err != nil {
				t.Fatalf("read shard: %v", err)
			}
			cond := findCondition(got.Status.Conditions, conditionStorageClassValid)
			if cond == nil {
				t.Fatalf("no %s condition", conditionStorageClassValid)
			}
			if cond.Status != settled.status || cond.Reason != settled.reason ||
				cond.Message != settled.message || cond.ObservedGeneration != got.Generation {
				t.Fatalf("published condition does not match the verdict: %+v", *cond)
			}
		})
	}
}

// TestStorageClassCondition_UpdatesWhenTheVerdictChanges is the other half of
// TestStorageClassCondition_IsStableAcrossReconciles: the skip has to hold a
// settled condition still, and it has to let a changed verdict through. The
// missing StorageClass appears between the two cycles, so status, reason,
// message and lastTransitionTime all have to move with it.
func TestStorageClassCondition_UpdatesWhenTheVerdictChanges(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "appears-later"},
				},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	patches := 0
	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnStatusPatch: func(client.Object) error {
			patches++
			return nil
		},
	})
	r := &ShardReconciler{Client: fakeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}

	key := client.ObjectKeyFromObject(shard)
	cycle := func() metav1.Condition {
		t.Helper()

		check, err := r.validateStorageClassDependencies(t.Context(), shard)
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		if err := r.setStorageClassCondition(t.Context(), shard, check); err != nil {
			t.Fatalf("set condition: %v", err)
		}

		var got multigresv1alpha1.Shard
		if err := baseClient.Get(t.Context(), key, &got); err != nil {
			t.Fatalf("read shard: %v", err)
		}
		cond := findCondition(got.Status.Conditions, conditionStorageClassValid)
		if cond == nil {
			t.Fatalf("no %s condition", conditionStorageClassValid)
		}
		return *cond
	}

	before := cycle()
	if before.Status != metav1.ConditionFalse || before.Reason != storageClassNotFoundReason {
		t.Fatalf("first cycle must report the missing class: %+v", before)
	}
	if patches != 1 {
		t.Fatalf("first cycle applied %d patches, want 1", patches)
	}

	// Backdated because metav1.Time serialises at second precision and both
	// cycles run inside the same second, which would make a rewritten
	// lastTransitionTime indistinguishable from a preserved one.
	backdated := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	persistStorageClassCondition(t, baseClient, key, func(generation int64) metav1.Condition {
		return metav1.Condition{
			Type:               before.Type,
			Status:             before.Status,
			Reason:             before.Reason,
			Message:            before.Message,
			ObservedGeneration: generation,
			LastTransitionTime: backdated,
		}
	})

	if err := baseClient.Create(t.Context(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "appears-later"},
	}); err != nil {
		t.Fatalf("create StorageClass: %v", err)
	}

	after := cycle()
	if patches != 2 {
		t.Fatalf("the changed verdict was skipped: %d patches total", patches)
	}
	if after.Status != metav1.ConditionTrue {
		t.Errorf("status = %s, want %s", after.Status, metav1.ConditionTrue)
	}
	if after.Reason != storageClassFoundReason {
		t.Errorf("reason = %s, want %s", after.Reason, storageClassFoundReason)
	}
	if after.Message == before.Message {
		t.Errorf("message did not move off the missing-class text: %q", after.Message)
	}
	if want := "All explicitly configured StorageClasses are present"; after.Message != want {
		t.Errorf("message = %q, want %q", after.Message, want)
	}
	if !after.LastTransitionTime.After(backdated.Time) {
		t.Errorf(
			"lastTransitionTime = %s, want it moved past %s: the condition transitioned",
			after.LastTransitionTime,
			backdated,
		)
	}
}

// TestStorageClassCondition_PreservesLastTransitionTimeWithoutATransition
// covers the republish an upgrade from the two-writer build performs: the
// persisted condition still carries one of the two old messages, the verdict is
// the same True it always was, so the message has to be rewritten while
// lastTransitionTime stays put, matching meta.SetStatusCondition.
func TestStorageClassCondition_PreservesLastTransitionTimeWithoutATransition(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
			},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()

	key := client.ObjectKeyFromObject(shard)
	staleMessage := "No explicit pool StorageClass configured; using cluster default"
	backdated := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	persistStorageClassCondition(t, baseClient, key, func(generation int64) metav1.Condition {
		return metav1.Condition{
			Type:               conditionStorageClassValid,
			Status:             metav1.ConditionTrue,
			Reason:             storageClassNotSpecifiedReason,
			Message:            staleMessage,
			ObservedGeneration: generation,
			LastTransitionTime: backdated,
		}
	})

	patches := 0
	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnStatusPatch: func(client.Object) error {
			patches++
			return nil
		},
	})
	r := &ShardReconciler{Client: fakeClient, Scheme: scheme, Recorder: record.NewFakeRecorder(50)}

	check, err := r.validateStorageClassDependencies(t.Context(), shard)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := r.setStorageClassCondition(t.Context(), shard, check); err != nil {
		t.Fatalf("set condition: %v", err)
	}
	if patches != 1 {
		t.Fatalf("the stale message was not republished: %d patches", patches)
	}

	var got multigresv1alpha1.Shard
	if err := baseClient.Get(t.Context(), key, &got); err != nil {
		t.Fatalf("read shard: %v", err)
	}
	cond := findCondition(got.Status.Conditions, conditionStorageClassValid)
	if cond == nil {
		t.Fatalf("no %s condition", conditionStorageClassValid)
	}
	if cond.Message == staleMessage {
		t.Fatalf("message was not rewritten: %q", cond.Message)
	}
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("status = %s, want %s", cond.Status, metav1.ConditionTrue)
	}
	if !cond.LastTransitionTime.Time.Equal(backdated.Time) {
		t.Errorf(
			"lastTransitionTime = %s, want it preserved at %s: the status did not transition",
			cond.LastTransitionTime,
			backdated,
		)
	}
}
