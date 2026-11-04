package multigrescluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	shardSAName = "shard-sa"
	irsaKey     = "eks.amazonaws.com/role-arn"
)

func shardSACluster() *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "cluster-uid"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			ShardServiceAccount: &multigresv1alpha1.ShardServiceAccountSpec{
				Name:        shardSAName,
				Annotations: map[string]string{irsaKey: "arn:aws:iam::1:role/ours"},
			},
		},
	}
}

func newShardSAReconciler(
	t *testing.T,
	objs ...client.Object,
) (*MultigresClusterReconciler, client.Client) {
	t.Helper()
	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}, c
}

func getShardSA(t *testing.T, c client.Client) *corev1.ServiceAccount {
	t.Helper()
	sa := &corev1.ServiceAccount{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: shardSAName, Namespace: "default"},
		sa,
	); err != nil {
		t.Fatalf("get shard SA: %v", err)
	}
	return sa
}

func wantClusterOwnerRef(cluster *multigresv1alpha1.MultigresCluster) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         multigresv1alpha1.GroupVersion.String(),
		Kind:               "MultigresCluster",
		Name:               cluster.Name,
		UID:                cluster.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}
}

func TestShardSA_CreatedWithClusterOwner(t *testing.T) {
	cluster := shardSACluster()
	r, c := newShardSAReconciler(t)

	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	sa := getShardSA(t, c)
	wantRefs := []metav1.OwnerReference{wantClusterOwnerRef(cluster)}
	if diff := cmp.Diff(wantRefs, sa.OwnerReferences); diff != "" {
		t.Errorf("owner references (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(cluster.Spec.ShardServiceAccount.Annotations, sa.Annotations); diff != "" {
		t.Errorf("annotations (-want +got):\n%s", diff)
	}
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Errorf("automountServiceAccountToken = %v, want false", sa.AutomountServiceAccountToken)
	}
	if got := sa.Labels[metadata.LabelAppManagedBy]; got != metadata.ManagedByMultigres {
		t.Errorf("managed-by label = %q, want %q", got, metadata.ManagedByMultigres)
	}

	// A second pass is a no-op rather than an error or a duplicate owner.
	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if got := len(getShardSA(t, c).OwnerReferences); got != 1 {
		t.Errorf("owner references after second reconcile = %d, want 1", got)
	}
}

func TestShardSA_AdoptsExisting(t *testing.T) {
	cluster := shardSACluster()
	cluster.Spec.ShardServiceAccount.Annotations = map[string]string{
		irsaKey:    "arn:aws:iam::1:role/ours",
		"ours-new": "yes",
	}
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardSAName,
			Namespace: "default",
			Annotations: map[string]string{
				irsaKey:              "arn:aws:iam::1:role/platform",
				"platform.example/x": "keep",
			},
		},
	}
	r, c := newShardSAReconciler(t, existing)

	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	sa := getShardSA(t, c)
	wantRefs := []metav1.OwnerReference{wantClusterOwnerRef(cluster)}
	if diff := cmp.Diff(wantRefs, sa.OwnerReferences); diff != "" {
		t.Errorf("owner references (-want +got):\n%s", diff)
	}
	wantAnn := map[string]string{
		irsaKey:              "arn:aws:iam::1:role/ours",
		"ours-new":           "yes",
		"platform.example/x": "keep",
	}
	if diff := cmp.Diff(wantAnn, sa.Annotations); diff != "" {
		t.Errorf("annotations (-want +got):\n%s", diff)
	}

	// Dropping the spec must not delete an account we adopted.
	cluster.Spec.ShardServiceAccount = nil
	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	getShardSA(t, c)
}

// The owner is what protects the SA from shard deletion: Kubernetes garbage
// collection removes an object only when every owner is gone. Neither the fake
// client nor envtest runs the garbage collector (a testkit gap: no GC sim), so
// this pins the owner shape and that deleting a Shard leaves the SA.
func TestShardSA_SurvivesShardDeletion(t *testing.T) {
	cluster := shardSACluster()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard", Namespace: "default"},
	}
	r, c := newShardSAReconciler(t, shard)

	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(t.Context(), shard); err != nil {
		t.Fatal(err)
	}

	sa := getShardSA(t, c)
	for _, ref := range sa.OwnerReferences {
		if ref.Kind == "Shard" {
			t.Errorf("SA is owned by a Shard: %+v", ref)
		}
	}
	if len(sa.OwnerReferences) != 1 || sa.OwnerReferences[0].Kind != "MultigresCluster" {
		t.Errorf("owner references = %+v, want only the MultigresCluster", sa.OwnerReferences)
	}
}

func TestShardSA_NotCreatedWithoutSpec(t *testing.T) {
	tests := map[string]func(*multigresv1alpha1.MultigresCluster){
		"no shardSA spec": func(c *multigresv1alpha1.MultigresCluster) { c.Spec.ShardServiceAccount = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := shardSACluster()
			mutate(cluster)
			r, c := newShardSAReconciler(t)

			if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}

			list := &corev1.ServiceAccountList{}
			if err := c.List(t.Context(), list, client.InNamespace("default")); err != nil {
				t.Fatal(err)
			}
			if len(list.Items) != 0 {
				t.Errorf("created %d ServiceAccounts, want 0", len(list.Items))
			}
		})
	}
}

// Annotations in the spec are owned by the operator once applied: dropping a
// key removes it, even one the platform set first, while keys the operator
// never applied stay.
func TestShardSA_AnnotationRemovedFromSpecIsRemoved(t *testing.T) {
	cluster := shardSACluster()
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardSAName,
			Namespace: "default",
			Annotations: map[string]string{
				irsaKey:              "arn:aws:iam::1:role/platform",
				"platform.example/x": "keep",
			},
		},
	}
	r, c := newShardSAReconciler(t, existing)

	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if got := getShardSA(t, c).Annotations[irsaKey]; got != "arn:aws:iam::1:role/ours" {
		t.Fatalf("annotation %s = %q after adoption, want ours", irsaKey, got)
	}

	cluster.Spec.ShardServiceAccount.Annotations = nil
	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"platform.example/x": "keep"}
	if diff := cmp.Diff(want, getShardSA(t, c).Annotations); diff != "" {
		t.Errorf("annotations (-want +got):\n%s", diff)
	}
}

// Renaming applies the new account and leaves the old one, which may be shared
// with things the operator did not create.
func TestShardSA_RenameKeepsOldAccount(t *testing.T) {
	cluster := shardSACluster()
	r, c := newShardSAReconciler(t)

	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.ShardServiceAccount.Name = "shard-sa-renamed"
	if err := r.reconcileShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	list := &corev1.ServiceAccountList{}
	if err := c.List(t.Context(), list, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, sa := range list.Items {
		names = append(names, sa.Name)
	}
	if diff := cmp.Diff([]string{shardSAName, "shard-sa-renamed"}, names); diff != "" {
		t.Errorf("ServiceAccounts (-want +got):\n%s", diff)
	}
}

// The fake client does not enforce the apiserver's single-controller rule, so
// the apply is made to fail with the apiserver's message; the envtest
// integration test covers the real rejection.
func conflictClient(t *testing.T, base client.Client) client.Client {
	t.Helper()
	return testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			if _, ok := obj.(*corev1.ServiceAccount); ok {
				return errors.New("Only one reference can have Controller set to true")
			}
			return nil
		},
	})
}

func preOwnedSA() *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardSAName,
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "other",
				UID:        "other-uid",
				Controller: ptr.To(true),
			}},
		},
	}
}

func TestShardSA_ConflictIsReportedNotFatal(t *testing.T) {
	cluster := shardSACluster()
	scheme := setupScheme()
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(preOwnedSA()).Build()
	rec := record.NewFakeRecorder(10)
	r := &MultigresClusterReconciler{Client: conflictClient(t, base), Scheme: scheme, Recorder: rec}

	err := r.syncShardServiceAccount(t.Context(), cluster)
	require.ErrorIs(t, err, errShardServiceAccountControlled)
	r.reportShardServiceAccount(t.Context(), cluster, nil, nil, err)

	select {
	case evt := <-rec.Events:
		for _, want := range []string{"Warning", "ShardServiceAccountConflict", shardSAName, `Deployment "other"`} {
			if !strings.Contains(evt, want) {
				t.Errorf("event %q missing %q", evt, want)
			}
		}
	default:
		t.Fatal("no event emitted")
	}

	cond := meta.FindStatusCondition(
		cluster.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	)
	if cond == nil || cond.Status != metav1.ConditionFalse ||
		cond.Reason != multigresv1alpha1.ReasonShardServiceAccountConflict {
		t.Errorf("condition = %+v, want False/Conflict", cond)
	}
}

// The informer only holds ServiceAccounts carrying the managed-by label, so a
// conflicting account is invisible to the cached client. The base client here
// stands for that filtered cache (it never holds the account); the conflict
// must still be named because the lookup goes through the APIReader.
func TestShardSA_ConflictDetectedThroughAPIReaderNotCache(t *testing.T) {
	cluster := shardSACluster()
	scheme := setupScheme()
	cache := fake.NewClientBuilder().WithScheme(scheme).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(preOwnedSA()).Build()
	rec := record.NewFakeRecorder(10)
	r := &MultigresClusterReconciler{
		Client:    conflictClient(t, cache),
		APIReader: live,
		Scheme:    scheme,
		Recorder:  rec,
	}

	require.ErrorIs(t,
		r.syncShardServiceAccount(t.Context(), cluster), errShardServiceAccountControlled)
	cond := meta.FindStatusCondition(
		cluster.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	)
	if cond == nil || cond.Reason != multigresv1alpha1.ReasonShardServiceAccountConflict ||
		!strings.Contains(cond.Message, `Deployment "other"`) {
		t.Errorf("condition = %+v, want Conflict naming the other controller", cond)
	}

	// Without the APIReader the cache is all there is, and the conflict is
	// reported as a generic apply failure.
	cluster = shardSACluster()
	r = &MultigresClusterReconciler{
		Client: conflictClient(t, cache), Scheme: scheme, Recorder: rec,
	}
	err := r.syncShardServiceAccount(t.Context(), cluster)
	require.Error(t, err)
	require.NotErrorIs(t, err, errShardServiceAccountControlled,
		"an apply failure nobody can explain is retried on the work queue's backoff")
	cond = meta.FindStatusCondition(
		cluster.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	)
	if cond == nil || cond.Reason != multigresv1alpha1.ReasonShardServiceAccountApplyFailed {
		t.Errorf("condition = %+v, want ApplyFailed from the cache-only read", cond)
	}
}

func TestShardSA_ConditionLifecycle(t *testing.T) {
	cluster := shardSACluster()
	r, _ := newShardSAReconciler(t)

	if err := r.syncShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	cond := meta.FindStatusCondition(
		cluster.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v, want True", cond)
	}

	cluster.Spec.ShardServiceAccount = nil
	if err := r.syncShardServiceAccount(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(
		cluster.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	) != nil {
		t.Error("condition must be removed when the cluster no longer uses the account")
	}
}

// Whole-Reconcile tests, through the production builders.
func shardSAReconcile(
	t *testing.T,
	failures func(base client.Client) *testutil.FailureConfig,
	objs ...client.Object,
) (client.Client, *record.FakeRecorder, error) {
	t.Helper()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, _, _ := setupFixtures(t)
	cluster.Spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: shardSAName}
	all := append([]client.Object{coreTpl, cellTpl, shardTpl, cluster}, objs...)
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(all...).
		WithStatusSubresource(
			&multigresv1alpha1.MultigresCluster{},
			&multigresv1alpha1.Cell{},
			&multigresv1alpha1.TableGroup{},
		).
		Build()
	var c client.Client = base
	if failures != nil {
		c = testutil.NewFakeClientWithFailures(base, failures(base))
	}
	rec := record.NewFakeRecorder(100)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: rec,
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			_, factory := memorytopo.NewServerAndFactory(t.Context())
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			), nil
		},
	}
	_, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(cluster),
	})
	return base, rec, err
}

// A Shard that names the account must never exist before the account does, or
// the ServiceAccount admission plugin rejects its pods.
func TestShardSA_AppliedBeforeCellsAndTableGroups(t *testing.T) {
	saPresent := map[string]bool{}
	failures := func(base client.Client) *testutil.FailureConfig {
		observe := func(obj client.Object) error {
			var kind string
			switch obj.(type) {
			case *multigresv1alpha1.Cell:
				kind = "Cell"
			case *multigresv1alpha1.TableGroup:
				kind = "TableGroup"
			default:
				return nil
			}
			if _, seen := saPresent[kind]; !seen {
				err := base.Get(
					t.Context(),
					types.NamespacedName{Name: shardSAName, Namespace: "default"},
					&corev1.ServiceAccount{},
				)
				saPresent[kind] = err == nil
			}
			return nil
		}
		return &testutil.FailureConfig{OnCreate: observe, OnPatch: observe}
	}
	if _, _, err := shardSAReconcile(t, failures); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"Cell", "TableGroup"} {
		present, seen := saPresent[kind]
		if !seen {
			t.Fatalf("no %s write observed", kind)
		}
		if !present {
			t.Errorf("first %s write happened before the ServiceAccount existed", kind)
		}
	}
}

// An account that another object controls is not this cluster's to use. The
// conflict is reported and the cells still converge, but the account's name
// reaches no TableGroup, so no pod of this cluster starts under it: a
// TableGroup that does not exist is not created, and one that exists keeps
// the name it has while the rest of its spec still follows the cluster. Once
// the account is free, the name is published.
func TestShardSA_ConflictKeepsTheAccountFromShards(t *testing.T) {
	ctx := t.Context()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, _, _ := setupFixtures(t)
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, cluster).
		WithStatusSubresource(
			&multigresv1alpha1.MultigresCluster{},
			&multigresv1alpha1.Cell{},
			&multigresv1alpha1.TableGroup{},
		).
		Build()
	rec := record.NewFakeRecorder(1000)
	r := &MultigresClusterReconciler{
		Client: base, Scheme: scheme, Recorder: rec,
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			_, factory := memorytopo.NewServerAndFactory(t.Context())
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			), nil
		},
	}
	key := client.ObjectKeyFromObject(cluster)
	reconcile := func() error {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		return err
	}
	tableGroups := func() []multigresv1alpha1.TableGroup {
		t.Helper()
		list := &multigresv1alpha1.TableGroupList{}
		require.NoError(t, base.List(ctx, list))
		return list.Items
	}
	edit := func(mutate func(*multigresv1alpha1.MultigresClusterSpec)) {
		t.Helper()
		got := &multigresv1alpha1.MultigresCluster{}
		require.NoError(t, base.Get(ctx, key, got))
		mutate(&got.Spec)
		require.NoError(t, base.Update(ctx, got))
	}
	condition := func() *metav1.Condition {
		t.Helper()
		got := &multigresv1alpha1.MultigresCluster{}
		require.NoError(t, base.Get(ctx, key, got))
		return meta.FindStatusCondition(
			got.Status.Conditions, multigresv1alpha1.ConditionShardServiceAccountReady)
	}

	// A new cluster that names another object's account.
	foreign := preOwnedSA()
	require.NoError(t, base.Create(ctx, foreign))
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: shardSAName}
	})
	require.NoError(t, reconcile(), "a conflict is reported in the condition, not as an error")
	cond := condition()
	require.NotNil(t, cond)
	require.Equal(t, multigresv1alpha1.ReasonShardServiceAccountConflict, cond.Reason)
	cells := &multigresv1alpha1.CellList{}
	require.NoError(t, base.List(ctx, cells))
	require.NotEmpty(t, cells.Items, "cells were not reconciled after the conflict")
	require.Empty(t, tableGroups(), "a TableGroup was created with an account that is not ours")

	// Without the account in the spec the TableGroup is created, without a
	// name. Naming the foreign account again does not reach it, while
	// another edit of the same update does.
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) { spec.ShardServiceAccount = nil })
	require.NoError(t, reconcile())
	require.Len(t, tableGroups(), 1)
	require.Empty(t, tableGroups()[0].Spec.ShardServiceAccountName)
	edit(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: shardSAName}
		spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
	})
	require.NoError(t, reconcile(), "a conflict is reported in the condition, not as an error")
	require.Len(t, tableGroups(), 1)
	require.Empty(t, tableGroups()[0].Spec.ShardServiceAccountName,
		"the conflicting account's name reached a TableGroup")
	require.Equal(t, "MULTI_CELL_AT_LEAST_2", tableGroups()[0].Spec.DurabilityPolicy,
		"the conflict held back the rest of the spec")

	// The other owner lets go of the account.
	require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(foreign), foreign))
	foreign.OwnerReferences = nil
	require.NoError(t, base.Update(ctx, foreign))
	require.NoError(t, reconcile())
	require.Equal(t, shardSAName, tableGroups()[0].Spec.ShardServiceAccountName)
	require.Equal(t, metav1.ConditionTrue, condition().Status)

	var found bool
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, "ShardServiceAccountConflict") {
			found = true
		}
	}
	require.True(t, found, "no ShardServiceAccountConflict event")
}

// The decision not to apply is made from the account as the informer holds
// it, managedFields included. The fake client keeps no managedFields, so the
// accounts here carry the entry a real API server records for the operator's
// apply.
func TestShardServiceAccountApplied(t *testing.T) {
	cluster := shardSACluster()
	desired, err := buildShardServiceAccount(cluster, setupScheme())
	if err != nil {
		t.Fatal(err)
	}
	applied := func(ownedAnnotations string) *corev1.ServiceAccount {
		sa := desired.DeepCopy()
		sa.Annotations = map[string]string{
			irsaKey:              "arn:aws:iam::1:role/ours",
			"platform.example/x": "keep",
		}
		sa.ManagedFields = []metav1.ManagedFieldsEntry{
			{
				Manager: "platform", Operation: metav1.ManagedFieldsOperationUpdate,
				FieldsV1: metav1.NewFieldsV1(
					`{"f:metadata":{"f:annotations":{"f:platform.example/x":{}}}}`,
				),
			},
			{
				Manager:   shardServiceAccountFieldOwner(shardSACluster()),
				Operation: metav1.ManagedFieldsOperationApply,
				FieldsV1: metav1.NewFieldsV1(
					`{"f:automountServiceAccountToken":{},"f:metadata":{"f:annotations":{` +
						ownedAnnotations +
						`},"f:labels":{"f:app.kubernetes.io/managed-by":{}},` +
						`"f:ownerReferences":{"k:{\"uid\":\"cluster-uid\"}":{}}}}`,
				),
			},
		}
		return sa
	}
	const ours = `"f:eks.amazonaws.com/role-arn":{}`

	for name, tc := range map[string]struct {
		observed func() *corev1.ServiceAccount
		want     bool
	}{
		"as applied, with another manager's annotation": {
			observed: func() *corev1.ServiceAccount { return applied(ours) },
			want:     true,
		},
		"an annotation the operator applied and no longer sets": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours + `,"f:dropped-from-spec":{}`)
				sa.Annotations["dropped-from-spec"] = "stale"
				return sa
			},
		},
		"never applied by the operator": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.ManagedFields = sa.ManagedFields[:1]
				return sa
			},
		},
		"only an update entry of the operator": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.ManagedFields[1].Operation = metav1.ManagedFieldsOperationUpdate
				return sa
			},
		},
		"annotation value changed": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.Annotations[irsaKey] = "arn:aws:iam::1:role/platform"
				return sa
			},
		},
		"annotation removed": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				delete(sa.Annotations, irsaKey)
				return sa
			},
		},
		"token automount turned on": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.AutomountServiceAccountToken = ptr.To(true)
				return sa
			},
		},
		"token automount unset": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.AutomountServiceAccountToken = nil
				return sa
			},
		},
		"managed-by label removed": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.Labels = nil
				return sa
			},
		},
		"owner reference removed": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.OwnerReferences = nil
				return sa
			},
		},
		"owned by another cluster": {
			observed: func() *corev1.ServiceAccount {
				sa := applied(ours)
				sa.OwnerReferences[0].UID = "other-uid"
				return sa
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := shardServiceAccountApplied(
				tc.observed(),
				desired,
				shardServiceAccountFieldOwner(shardSACluster()),
			); got != tc.want {
				t.Errorf("shardServiceAccountApplied = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("no annotations wanted and none owned", func(t *testing.T) {
		bare := shardSACluster()
		bare.Spec.ShardServiceAccount.Annotations = nil
		want, err := buildShardServiceAccount(bare, setupScheme())
		if err != nil {
			t.Fatal(err)
		}
		if !shardServiceAccountApplied(
			applied(""),
			want,
			shardServiceAccountFieldOwner(shardSACluster()),
		) {
			t.Error("an account with nothing left to remove must not be applied again")
		}
		if shardServiceAccountApplied(
			applied(ours),
			want,
			shardServiceAccountFieldOwner(shardSACluster()),
		) {
			t.Error("an annotation dropped from the spec must be removed by an apply")
		}
	})
}
