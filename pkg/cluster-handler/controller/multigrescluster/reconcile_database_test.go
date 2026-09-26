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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/testkit/assert"
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
				ck := assert.NewCollecting(t)
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

				ck.Require().
					Len(tg.Spec.Shards, 1, "Expected 1 shard (injected '0'), got %d", len(tg.Spec.Shards))
				// Verify defaults applied.
				// NOTE: We expect 3 replicas here because 'shardTpl' (the default template in fixtures)
				// defines replicas: 3. The resolver correctly prioritizes the Namespace Default (Level 3)
				// over the Operator Default (Level 4, which is 1).
				got, want := *tg.Spec.Shards[0].Multiorch.Replicas, int32(3)
				ck.Eq(want, got, "Injected shard replicas mismatch. Replicas")
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
				ck := assert.NewCollecting(t)
				tg := &multigresv1alpha1.TableGroup{}
				tgName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
				)
				ck.Require().NoError(c.Get(
					t.Context(),
					types.NamespacedName{Name: tgName, Namespace: namespace},
					tg,
				), "failed to get tablegroup")
				ck.Eq(
					"zone-custom",
					tg.Spec.Shards[0].Multiorch.Cells[0],
					"Expected explicit cell 'zone-custom', got",
				)
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
				ck := assert.NewCollecting(t)
				ctx := t.Context()
				tg := &multigresv1alpha1.TableGroup{}
				tgName := name.JoinWithConstraints(
					name.DefaultConstraints,
					clusterName,
					"db1",
					"tg1",
				)
				ck.Require().NoError(c.Get(
					ctx,
					types.NamespacedName{Name: tgName, Namespace: namespace},
					tg,
				))
				cells := tg.Spec.Shards[0].Multiorch.Cells
				ck.Require().Len(cells, 2, "Expected 2 cells, got %d", len(cells))
				ck.False(
					cells[0] != "zone-a" || cells[1] != "zone-b",
					"Cells not sorted: %v",
					cells,
				)
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
				OnGet: testutil.FailOnKeyName("default", errSimulated),
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

	assert.NewAborting(t).NoError(cl.Create(t.Context(), shardTpl))
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
		assert.NewCollecting(t).StrContains(err.Error(), expectedMsg, "Expected error containing")
	}
}
