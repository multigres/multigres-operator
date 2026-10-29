package multigrescluster

import (
	"errors"
	"strings"
	"testing"

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
	if err == nil {
		t.Fatal("want an error so the request requeues with backoff")
	}

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

	if err := r.syncShardServiceAccount(t.Context(), cluster); err == nil {
		t.Fatal("want an error so the request requeues with backoff")
	}
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
	r.APIReader = nil
	if err := r.syncShardServiceAccount(t.Context(), cluster); err == nil {
		t.Fatal("want an error")
	}
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

func TestShardSA_ConflictDoesNotHaltCells(t *testing.T) {
	failures := func(client.Client) *testutil.FailureConfig {
		return &testutil.FailureConfig{
			OnPatch: func(obj client.Object) error {
				if _, ok := obj.(*corev1.ServiceAccount); ok {
					return errors.New("Only one reference can have Controller set to true")
				}
				return nil
			},
		}
	}
	base, rec, err := shardSAReconcile(t, failures, preOwnedSA())
	if err == nil || !strings.Contains(err.Error(), "Controller set to true") {
		t.Fatalf("Reconcile error = %v, want the SA apply error", err)
	}

	cells := &multigresv1alpha1.CellList{}
	if lerr := base.List(t.Context(), cells); lerr != nil {
		t.Fatal(lerr)
	}
	if len(cells.Items) == 0 {
		t.Error("cells were not reconciled after the ServiceAccount conflict")
	}
	tgs := &multigresv1alpha1.TableGroupList{}
	if lerr := base.List(t.Context(), tgs); lerr != nil {
		t.Fatal(lerr)
	}
	if len(tgs.Items) == 0 {
		t.Error("tablegroups were not reconciled after the ServiceAccount conflict")
	}

	got := &multigresv1alpha1.MultigresCluster{}
	if gerr := base.Get(
		t.Context(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"},
		got,
	); gerr != nil {
		t.Fatal(gerr)
	}
	cond := meta.FindStatusCondition(
		got.Status.Conditions,
		multigresv1alpha1.ConditionShardServiceAccountReady,
	)
	if cond == nil || cond.Reason != multigresv1alpha1.ReasonShardServiceAccountConflict {
		t.Errorf("persisted condition = %+v, want Conflict", cond)
	}

	close(rec.Events)
	var found bool
	for evt := range rec.Events {
		if strings.Contains(evt, "ShardServiceAccountConflict") {
			found = true
		}
	}
	if !found {
		t.Error("no ShardServiceAccountConflict event")
	}
}
