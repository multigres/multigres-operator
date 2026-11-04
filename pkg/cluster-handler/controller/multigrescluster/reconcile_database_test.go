package multigrescluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcile_Databases(t *testing.T) {
	coreTpl, cellTpl, shardTpl, _, clusterName, namespace := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	tests := map[string]reconcileTestCase{
		"Create: Ultra-Minimalist (Shard Injection)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Labels = nil         // Force tracking-label patch after defaulting.
				c.Spec.Databases = nil // Clear databases, rely on auto-injection.
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()

				// System catalog is always "postgres" db, "default" tablegroup
				tgName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"postgres",
					"default",
				)
				tg := &multigresv1alpha1.TableGroup{}
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: tgName, Namespace: namespace},
					tg,
				); err != nil {
					list := &multigresv1alpha1.TableGroupList{}
					_ = c.List(ctx, list)
					t.Logf("Expected TG name: %s", tgName)
					for _, item := range list.Items {
						t.Logf(
							"Existing TG: %s (db=%s, tg=%s)",
							item.Name,
							item.Spec.DatabaseName,
							item.Spec.TableGroupName,
						)
					}
					t.Fatalf("System Catalog TableGroup not found: %v", err)
				}

				if len(tg.Spec.Shards) != 1 {
					t.Fatalf("Expected 1 shard (injected '0'), got %d", len(tg.Spec.Shards))
				}
				// Verify defaults applied.
				// NOTE: We expect 3 replicas here because 'shardTpl' (the default template in fixtures)
				// defines replicas: 3. The resolver correctly prioritizes the Namespace Default (Level 3)
				// over the Operator Default (Level 4, which is 1).
				if got, want := *tg.Spec.Shards[0].Multiorch.Replicas, int32(3); got != want {
					t.Errorf("Injected shard replicas mismatch. Replicas: %d, Want: %d", got, want)
				}
				if len(tg.Spec.Shards[0].Multiorch.Cells) != 1 ||
					tg.Spec.Shards[0].Multiorch.Cells[0] != "zone-a" {
					t.Errorf(
						"Expected Multiorch to inherit cell 'zone-a', got %v",
						tg.Spec.Shards[0].Multiorch.Cells,
					)
				}
			},
		},
		"Create: Multiorch Skip Defaulting (Explicit Cells)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].TableGroups[0].Shards[0].Spec = &multigresv1alpha1.ShardInlineSpec{
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-custom"},
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			validate: func(t testing.TB, c client.Client) {
				tg := &multigresv1alpha1.TableGroup{}
				tgName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
				)
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					tg,
				); err != nil {
					t.Fatalf("failed to get tablegroup: %v", err)
				}
				if got := tg.Spec.Shards[0].Multiorch.Cells[0]; got != "zone-custom" {
					t.Errorf("Expected explicit cell 'zone-custom', got %s", got)
				}
			},
		},
		"Reconcile: Implicit Cell Sorting": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Cells = []multigresv1alpha1.CellConfig{
					{Name: "zone-b", ZoneID: "use1-az2"},
					{Name: "zone-a", ZoneID: "use1-az1"},
				}
			},
			existingObjects: []client.Object{
				coreTpl, cellTpl,
				&multigresv1alpha1.ShardTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "default-shard", Namespace: namespace},
					Spec: multigresv1alpha1.ShardTemplateSpec{
						Multiorch: &multigresv1alpha1.MultiorchSpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas: ptr.To(int32(3)),
							},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"primary": {
								Cells: []multigresv1alpha1.CellName{"zone-b", "zone-a"},
							},
						},
					},
				},
			},
			validate: func(t testing.TB, c client.Client) {
				ctx := t.Context()
				tg := &multigresv1alpha1.TableGroup{}
				tgName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
				)
				if err := c.Get(
					ctx,
					types.NamespacedName{Name: tgName, Namespace: namespace},
					tg,
				); err != nil {
					t.Fatal(err)
				}
				cells := tg.Spec.Shards[0].Multiorch.Cells
				if len(cells) != 2 {
					t.Fatalf("Expected 2 cells, got %d", len(cells))
				}
				if cells[0] != "zone-a" || cells[1] != "zone-b" {
					t.Errorf("Cells not sorted: %v", cells)
				}
			},
		},
		"Error: Explicit Shard Template Missing": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "missing-shard-tpl"
			},
			existingObjects: []client.Object{coreTpl, cellTpl},
			wantErrMsg:      "failed to resolve shard",
		},
		"Error: List Existing TableGroups Failed": {
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			failureConfig: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*multigresv1alpha1.TableGroupList); ok {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to list existing tablegroups",
		},
		"Error: Resolve ShardTemplate Failed": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				// We must ensure the Cell resolution succeeds (by pointing to an existing template "default-cell")
				// so that it doesn't default to "default" and hit our failure config which captures "default".
				// We want only the Shard resolution (which also defaults to "default") to hit the error.
				c.Spec.Cells[0].CellTemplate = "default-cell"
				c.Spec.MultiadminWeb = &multigresv1alpha1.MultiadminWebConfig{
					TemplateRef: "default-core",
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl},
			failureConfig: &testutil.FailureConfig{
				// The gating namespace read is cluster-scoped (empty
				// key.Namespace); only fail the namespaced "default"
				// template lookup.
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "default" && key.Namespace != "" {
						return errSimulated
					}
					return nil
				},
			},
			wantErrMsg: "failed to resolve shard",
		},
		"Error: Apply TableGroup Failed": {
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(
					name.JoinWithConstraints(name.DefaultConstraints, clusterName, "db1", "tg1"),
					errSimulated,
				),
			},
			wantErrMsg: "failed to apply tablegroup",
		},

		"Error: Set PendingDeletion on Orphan TableGroup Failed": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      clusterName + "-orphan-tg",
						Namespace: namespace,
						Labels:    map[string]string{"multigres.com/cluster": clusterName},
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(clusterName+"-orphan-tg", errSimulated),
			},
			wantErrMsg: "failed to set PendingDeletion on tablegroup",
		},
		"Reconcile: Desired TableGroup Mid-Drain Is Not Applied Over": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				drainingTableGroup(wantedTG(clusterName), namespace, clusterName, false),
			},
			validate: func(t testing.TB, c client.Client) {
				tg := &multigresv1alpha1.TableGroup{}
				key := types.NamespacedName{Name: wantedTG(clusterName), Namespace: namespace}
				if err := c.Get(t.Context(), key, tg); err != nil {
					t.Fatalf(
						"draining TableGroup was deleted before reporting ReadyForDeletion: %v",
						err,
					)
				}
				if tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
					t.Errorf("draining TableGroup lost PendingDeletion")
				}
				if len(tg.Spec.Shards) != 0 {
					t.Errorf(
						"draining TableGroup was re-applied: got %d shards",
						len(tg.Spec.Shards),
					)
				}
			},
		},
		"Reconcile: Desired TableGroup Drained Is Deleted": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				drainingTableGroup(wantedTG(clusterName), namespace, clusterName, true),
			},
			validate: func(t testing.TB, c client.Client) {
				requireTableGroupGone(t, c, wantedTG(clusterName), namespace)
			},
		},
		"Reconcile: Drained Orphan Is Deleted While Another TableGroup Fails To Resolve": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "missing-shard-tpl"
			},
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				drainingTableGroup(clusterName+"-orphan-tg", namespace, clusterName, true),
			},
			wantErrMsg: "failed to resolve shard",
			validate: func(t testing.TB, c client.Client) {
				requireTableGroupGone(t, c, clusterName+"-orphan-tg", namespace)
			},
		},
		// The draining object is skipped before resolution, so its broken
		// spec is never read until the fresh TableGroup is due.
		"Reconcile: Re-Added Drained TableGroup Is Deleted Despite A Missing Template": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "missing-shard-tpl"
			},
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				drainingTableGroup(wantedTG(clusterName), namespace, clusterName, true),
			},
			validate: func(t testing.TB, c client.Client) {
				requireTableGroupGone(t, c, wantedTG(clusterName), namespace)
			},
		},
		"Reconcile: Drained Orphan Is Deleted While Another TableGroup Fails To Apply": {
			existingObjects: []client.Object{
				coreTpl, cellTpl, shardTpl,
				drainingTableGroup(clusterName+"-orphan-tg", namespace, clusterName, true),
			},
			failureConfig: &testutil.FailureConfig{
				OnPatch: testutil.FailOnObjectName(wantedTG(clusterName), errSimulated),
			},
			wantErrMsg: "failed to apply tablegroup",
			validate: func(t testing.TB, c client.Client) {
				requireTableGroupGone(t, c, clusterName+"-orphan-tg", namespace)
				if err := c.Get(
					t.Context(),
					types.NamespacedName{Name: wantedTG(clusterName), Namespace: namespace},
					&multigresv1alpha1.TableGroup{},
				); err == nil {
					t.Errorf("failing TableGroup was created despite its apply failing")
				}
			},
		},
		"Create: Long Names (Truncation Check)": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				longName := multigresv1alpha1.DatabaseName(strings.Repeat("a", 255))
				longTableName := multigresv1alpha1.TableGroupName(strings.Repeat("b", 255))
				c.Spec.Databases[0].Name = longName
				c.Spec.Databases[0].TableGroups[0].Name = longTableName
			},
			wantErrMsg: "",
		},
		"Build TableGroup: Hashing Handles Long Names": {
			preReconcileUpdate: func(t testing.TB, c *multigresv1alpha1.MultigresCluster) {
				c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints: []multigresv1alpha1.EndpointUrl{"http://ext:2379"},
					},
				}
				c.Spec.Multiadmin = nil
				c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{
					{
						Name: multigresv1alpha1.DatabaseName(strings.Repeat("a", 64)),
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Name: "tg1",
								Shards: []multigresv1alpha1.ShardConfig{
									{Name: "0", ShardTemplate: "default-shard"},
								},
							},
						},
					},
				}
			},
			existingObjects: []client.Object{coreTpl, cellTpl, shardTpl},
			wantErrMsg:      "",
		},
	}

	runReconcileTest(t, tests)
}

func TestReconcileDatabases_BuildError_SchemeMismatch(t *testing.T) {
	// 1. Create a scheme that only knows about TableGroup, but NOT MultigresCluster.
	// This will cause SetControllerReference to fail because it cannot look up the GVK for the owner (Cluster).
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(multigresv1alpha1.GroupVersion,
		&multigresv1alpha1.TableGroup{},
		&multigresv1alpha1.TableGroupList{},
		&multigresv1alpha1.CoreTemplate{},
		&multigresv1alpha1.CellTemplate{},
	)
	metav1.AddToGroupVersion(scheme, multigresv1alpha1.GroupVersion)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheme-test-cluster",
			Namespace: "default",
			UID:       "valid-uid", // UID is fine, Scheme is the problem
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://ext:2379"},
				},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{
				{
					Name: "db1",
					TableGroups: []multigresv1alpha1.TableGroupConfig{
						{Name: "tg1"},
					},
				},
			},
		},
	}

	// Fake client needs to know about TableGroup to List them (which returns empty)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &MultigresClusterReconciler{
		Client:   cl,
		Scheme:   scheme, // <--- This scheme is missing MultigresCluster GVK
		Recorder: record.NewFakeRecorder(10),
	}

	res := resolver.NewResolver(cl, "default")

	// Pre-create a shard template so ResolveShard doesn't fail
	shardTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Multiorch: &multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				Cells:         []multigresv1alpha1.CellName{"a"},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Type: "readWrite", Cells: []multigresv1alpha1.CellName{"a"}},
			},
		},
	}
	// We need to add ShardTemplate to the scheme so the client can create it?
	// Actually, the fake client might complain if we try to create an unknown type.
	// So we should add ShardTemplate to the scheme too.
	scheme.AddKnownTypes(multigresv1alpha1.GroupVersion, &multigresv1alpha1.ShardTemplate{})

	if err := cl.Create(t.Context(), shardTpl); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.TemplateDefaults.ShardTemplate = "default-shard"

	// Execution
	_, err := r.reconcileDatabases(t.Context(), cluster, res)

	// User Verification
	if err == nil {
		t.Error("Expected error due to missing MultigresCluster GVK in scheme, got nil")
	} else if !strings.Contains(err.Error(), "failed to build tablegroup") {
		t.Errorf("Expected 'failed to build tablegroup' error, got: %v", err)
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestReconcileDatabases_Direct_Error_GlobalTopoRef tests the error path in reconcileDatabases
// that is normally unreachable in the full Reconcile loop due to caching.
// By calling reconcileDatabases directly, we simulate a scenario where the previous
// steps might have been skipped or reordered, ensuring defensive programming is covered.
func TestReconcileDatabases_Direct_Error_GlobalTopoRef(t *testing.T) {
	scheme := setupScheme()
	_, cellTpl, shardTpl, _, namespace, _ := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	// Setup Cluster to use a failing template
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: namespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				TemplateRef: "topo-fail-db",
			},
			Databases: []multigresv1alpha1.DatabaseConfig{},
		},
	}

	// Setup objects without the failing template
	objs := []client.Object{cellTpl, shardTpl}

	// Setup Client with failure
	clientBuilder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	baseClient := clientBuilder.Build()

	failureConfig := &testutil.FailureConfig{
		OnGet: testutil.FailOnKeyName("topo-fail-db", errSimulated),
	}
	finalClient := testutil.NewFakeClientWithFailures(baseClient, failureConfig)

	// Setup Reconciler
	reconciler := &MultigresClusterReconciler{
		Client:   finalClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	// Setup Resolver
	res := resolver.NewResolver(finalClient, namespace)

	// Execute reconcileDatabases DIRECTLY
	// This is key: we skip reconcileGlobalComponents, so the cache is empty.
	_, err := reconciler.reconcileDatabases(t.Context(), cluster, res)

	// Verify
	if err == nil {
		t.Error("Expected error from reconcileDatabases, got nil")
	} else {
		expectedMsg := "failed to get global topo ref"
		if !strings.Contains(err.Error(), expectedMsg) {
			t.Errorf("Expected error containing %q, got %q", expectedMsg, err.Error())
		}
	}
}

// wantedTG is the object name of the TableGroup the fixture cluster wants
// (db1/tg1).
func wantedTG(clusterName string) string {
	return TableGroupName(clusterName, "db1", "tg1")
}

// drainingTableGroup is a TableGroup of the fixture cluster as it looks after
// the cluster dropped it from spec and started its drain: PendingDeletion set,
// and ReadyForDeletion set once drained is true.
func drainingTableGroup(
	tgName, namespace, clusterName string,
	drained bool,
) *multigresv1alpha1.TableGroup {
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tgName,
			Namespace: namespace,
			Labels:    map[string]string{"multigres.com/cluster": clusterName},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
			},
		},
	}
	if drained {
		tg.Status.Conditions = []metav1.Condition{{
			Type:               multigresv1alpha1.ConditionReadyForDeletion,
			Status:             metav1.ConditionTrue,
			Reason:             "AllShardsReady",
			LastTransitionTime: metav1.Now(),
		}}
	}
	return tg
}

// requireTableGroupGone fails unless the named TableGroup no longer exists.
func requireTableGroupGone(t testing.TB, c client.Client, tgName, namespace string) {
	t.Helper()
	err := c.Get(
		t.Context(),
		types.NamespacedName{Name: tgName, Namespace: namespace},
		&multigresv1alpha1.TableGroup{},
	)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("drained TableGroup %s still present (err %v)", tgName, err)
	}
}

// TestReconcileDatabases_DesiredMidDrainIsPending pins the pending flag on the
// pass that deletes the drained old object of a TableGroup the spec names.
// The pruning loop reports nothing pending once it has deleted it, but the
// TableGroup the spec wants does not exist yet, so the cluster must not count
// itself settled (retireManagedGlobalTopo waits on that).
func TestReconcileDatabases_DesiredMidDrainIsPending(t *testing.T) {
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)
	scheme := setupScheme()
	draining := drainingTableGroup(wantedTG(clusterName), namespace, clusterName, true)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster, draining).
		WithStatusSubresource(draining).
		Build()
	r := &MultigresClusterReconciler{
		Client:   cl,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	pending, err := r.reconcileDatabases(
		t.Context(), baseCluster, resolver.NewResolver(cl, namespace),
	)
	if err != nil {
		t.Fatalf("reconcileDatabases: %v", err)
	}
	if !pending {
		t.Error("want pendingDeletion while the TableGroup the spec names is not yet recreated")
	}
	requireTableGroupGone(t, cl, wantedTG(clusterName), namespace)
}
