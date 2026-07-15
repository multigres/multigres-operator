//go:build integration
// +build integration

package webhook_test

import (
	"context"
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getPrivilegedClient(t *testing.T) client.Client {
	t.Helper()
	if TestCfg == nil {
		t.Fatal("TestCfg is nil")
	}

	// Ensure the impersonated identity has permissions (cluster-admin)
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "operator-admin-binding"},
		Subjects: []rbacv1.Subject{
			{Kind: "User", Name: "system:serviceaccount:default:multigres-operator", APIGroup: "rbac.authorization.k8s.io"},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	// Use the existing admin client to create the binding
	if err := k8sClient.Create(context.Background(), binding); err != nil {
		// Ignore if already exists, otherwise fail
		if client.IgnoreAlreadyExists(err) != nil {
			t.Fatalf("Failed to create ClusterRoleBinding: %v", err)
		}
	}

	config := *TestCfg
	config.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:default:multigres-operator",
	}
	c, err := client.New(&config, client.Options{Scheme: k8sClient.Scheme()})
	if err != nil {
		t.Fatalf("Failed to create privileged client: %v", err)
	}
	return c
}

func setTestObjectPostgresPasswordSecretRef(obj client.Object) {
	switch typed := obj.(type) {
	case *multigresv1alpha1.Shard:
		setTestShardPostgresPasswordSecretRef(typed)
	case *unstructured.Unstructured:
		spec, ok := typed.Object["spec"].(map[string]interface{})
		if !ok {
			spec = map[string]interface{}{}
			typed.Object["spec"] = spec
		}
		spec["postgresPasswordSecretRef"] = map[string]interface{}{
			"name": "multigres-admin-password",
			"key":  "password",
		}
	}
}

func TestCEL_MultigresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     *multigresv1alpha1.MultigresCluster
		expectError string
	}{
		{
			name: "Invalid Cell: Both Spec and Overrides",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-cell-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{
							Name:   "invalid-cell",
							ZoneID: "use1-az1",
							Spec: &multigresv1alpha1.CellInlineSpec{
								Multigateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
							},
							Overrides: &multigresv1alpha1.CellOverrides{
								Multigateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(2))},
							},
						},
					},
				},
			},
			expectError: "cannot specify both 'spec' and 'overrides'",
		},
		{
			name: "Invalid Cell: Both Spec and CellTemplate",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-cell-template-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{
							Name:         "invalid-cell-template",
							ZoneID:       "use1-az1",
							CellTemplate: "some-template",
							Spec: &multigresv1alpha1.CellInlineSpec{
								Multigateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
							},
						},
					},
				},
			},
			expectError: "cannot specify both 'spec' and 'cellTemplate'",
		},
		{
			name: "Invalid Cell: Neither ZoneID nor Region",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-cell-no-zone",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "no-zone-cell"},
					},
				},
			},
			expectError: "at least one of 'zoneId' or 'region' must be specified",
		},
		{
			name: "Invalid Cell: Both ZoneID and Region",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-zoneid-region-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{
							Name:   "invalid-location",
							ZoneID: "use1-az1",
							Region: "us-east-1",
						},
					},
				},
			},
			expectError: "cannot specify both 'zoneId' and 'region'",
		},
		{
			name: "Invalid Shard: Both Spec and Overrides",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-shard-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "postgres",
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    "default",
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{
										{
											Name: "0",
											Spec: &multigresv1alpha1.ShardInlineSpec{},
											Overrides: &multigresv1alpha1.ShardOverrides{
												Multiorch: &multigresv1alpha1.MultiorchSpec{
													StatelessSpec: multigresv1alpha1.StatelessSpec{
														Replicas: ptr.To(int32(2)),
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: "cannot specify both 'spec' and 'overrides'",
		},
		{
			name: "Invalid Shard: Both Spec and ShardTemplate",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-shard-template-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "postgres",
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    "default",
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{
										{
											Name:          "0",
											ShardTemplate: "some-template",
											Spec:          &multigresv1alpha1.ShardInlineSpec{},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: "cannot specify both 'spec' and 'shardTemplate'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestPostgresPasswordSecretRef(tc.cluster)
			err := k8sClient.Create(ctx, tc.cluster)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			// In envtest, CEL errors usually appear in the error string
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_TopoServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     *multigresv1alpha1.MultigresCluster
		expectError string
	}{
		{
			name: "Invalid GlobalTopoServer: Both Etcd and External",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-toposerver-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd:     &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(1))},
						External: &multigresv1alpha1.ExternalTopoServerSpec{Endpoints: []multigresv1alpha1.EndpointUrl{"http://etcd:2379"}},
					},
					Cells: []multigresv1alpha1.CellConfig{}, // Empty for simplicity
				},
			},
			expectError: "must specify exactly one of 'etcd', 'external', or 'templateRef'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestPostgresPasswordSecretRef(tc.cluster)
			err := k8sClient.Create(ctx, tc.cluster)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_Limits(t *testing.T) {
	t.Parallel()

	// Helper to create a long string
	longString := strings.Repeat("a", 64)
	veryLongString := strings.Repeat("a", 257)

	tests := []struct {
		name        string
		cluster     *multigresv1alpha1.MultigresCluster
		expectError string
	}{
		{
			name: "Invalid Annotation Key Length",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-annotation-key-limit",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							PodAnnotations: map[string]string{
								longString: "value",
							},
						},
					},
				},
			},
			expectError: "annotation keys must be <64 chars",
		},
		{
			name: "Invalid Annotation Value Length",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-annotation-value-limit",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							PodAnnotations: map[string]string{
								"key": veryLongString,
							},
						},
					},
				},
			},
			expectError: "values <256 chars",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestPostgresPasswordSecretRef(tc.cluster)
			err := k8sClient.Create(ctx, tc.cluster)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_Database(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     *multigresv1alpha1.MultigresCluster
		expectError string
	}{
		{
			name: "Invalid Database Name",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-db-name",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "my-app-db", // Must be 'postgres'
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{Name: "default", Default: true},
							},
						},
					},
				},
			},
			expectError: "only the single system database named 'postgres'",
		},
		{
			name: "Invalid TableGroup: Default Group Wrong Name",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-tg-name",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "postgres",
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{Name: "main", Default: true}, // Must be 'default'
							},
						},
					},
				},
			},
			expectError: "the default tablegroup must be named 'default'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestPostgresPasswordSecretRef(tc.cluster)
			err := k8sClient.Create(ctx, tc.cluster)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_Multiadmin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     *multigresv1alpha1.MultigresCluster
		expectError string
	}{
		{
			name: "Invalid Multiadmin: Both Spec and Template",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-multiadmin-conflict",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec:        &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
						TemplateRef: "some-template",
					},
				},
			},
			expectError: "cannot specify both 'spec' and 'templateRef'",
		},
		{
			name: "Invalid Multiadmin: None",
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cel-multiadmin-empty",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{}, // Both empty
				},
			},
			expectError: "must specify either 'spec' or 'templateRef'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestPostgresPasswordSecretRef(tc.cluster)
			err := k8sClient.Create(ctx, tc.cluster)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_ShardImmutability(t *testing.T) {
	t.Parallel()

	shardName := "immutable-shard"
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardName,
			Namespace: testNamespace,
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0",
			Images: multigresv1alpha1.ShardImages{
				Postgres:    "postgres:15",
				Multiorch:   "multiorch:latest",
				Multipooler: "multipooler:latest",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "etcd:2379",
				RootPath:       "/multigres",
				Implementation: "etcd",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"read-write": {
					Type:            "readWrite",
					Cells:           []multigresv1alpha1.CellName{"cell-1"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
		},
	}

	// Create
	privClient := getPrivilegedClient(t)
	setTestShardPostgresPasswordSecretRef(shard)
	if err := privClient.Create(ctx, shard); err != nil {
		t.Fatalf("Failed to create Shard: %v", err)
	}

	// Try to update immutable fields (Validation removed by user, so updates should succeed)
	toUpdate := shard.DeepCopy()
	toUpdate.Spec.DatabaseName = "other-db"
	if err := privClient.Update(ctx, toUpdate); err != nil {
		t.Errorf("Expected success when updating previously immutable databaseName, got error: %v", err)
	}

	// Refetch to avoid conflict
	if err := privClient.Get(ctx, client.ObjectKeyFromObject(shard), shard); err != nil {
		t.Fatal(err)
	}

	toUpdate = shard.DeepCopy()
	toUpdate.Spec.TableGroupName = "other-tg"
	if err := privClient.Update(ctx, toUpdate); err != nil {
		t.Errorf("Expected success when updating previously immutable tableGroupName, got error: %v", err)
	}

	// Refetch to avoid conflict
	if err := privClient.Get(ctx, client.ObjectKeyFromObject(shard), shard); err != nil {
		t.Fatal(err)
	}

	toUpdate = shard.DeepCopy()
	toUpdate.Spec.ShardName = "1"
	if err := privClient.Update(ctx, toUpdate); err != nil {
		t.Errorf("Expected success when updating previously immutable shardName, got error: %v", err)
	}
}

func TestCEL_ExtendedValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		shard       client.Object
		expectError string
	}{
		{
			name: "Invalid Duplicate Cells in Multiorch",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-duplicate-multiorch-cells", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"cell-1", "cell-1"}, // Duplicate
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {Cells: []multigresv1alpha1.CellName{"cell-1"}},
					},
				},
			},
			expectError: "Duplicate value", // Set validation error
		},
		{
			name: "Invalid Duplicate Cells in Pool",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-duplicate-pool-cells", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {
							Cells: []multigresv1alpha1.CellName{"cell-1", "cell-1"}, // Duplicate
						},
					},
				},
			},
			expectError: "Duplicate value",
		},
		{
			name: "Invalid Empty Pool Cells",
			shard: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "multigres.com/v1alpha1",
					"kind":       "Shard",
					"metadata": map[string]interface{}{
						"name":      "cel-empty-pool-cells",
						"namespace": testNamespace,
					},
					"spec": map[string]interface{}{
						"databaseName":   "postgres",
						"tableGroupName": "default",
						"shardName":      "0",
						"images": map[string]interface{}{
							"postgres":    "p",
							"multiorch":   "o",
							"multipooler": "po",
						},
						"globalTopoServer": map[string]interface{}{
							"address":        "etcd",
							"rootPath":       "/",
							"implementation": "etcd",
						},
						"multiorch": map[string]interface{}{
							"replicas": 1, // Inlined StatelessSpec
						},
						"pools": map[string]interface{}{
							"rw": map[string]interface{}{
								"cells":           []interface{}{}, // Explicit empty array to trigger MinItems
								"replicasPerCell": 1,
								"type":            "readWrite",
							},
						},
					},
				},
			},
			expectError: "should have at least 1 items",
		},
		{
			name: "Invalid ReplicasPerCell Limit",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-replicas-per-cell-limit", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {
							Cells:           []multigresv1alpha1.CellName{"cell-1"},
							ReplicasPerCell: ptr.To(int32(33)), // > 32
						},
					},
				},
			},
			expectError: "less than or equal to 32",
		},
		{
			name: "Invalid ReplicasPerCell Zero",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-replicas-per-cell-zero", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {
							Cells:           []multigresv1alpha1.CellName{"cell-1"},
							ReplicasPerCell: ptr.To(int32(0)), // < 1
						},
					},
				},
			},
			expectError: "greater than or equal to 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			privClient := getPrivilegedClient(t)
			setTestObjectPostgresPasswordSecretRef(tc.shard)
			err := privClient.Create(ctx, tc.shard)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" {
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Logf("ACTUAL ERROR: %v", err)
					t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
				}
			}
		})
	}
}

func TestCEL_PoolRuntimeIdentity(t *testing.T) {
	t.Parallel()

	newShard := func(name string, postgresUID, multipoolerUID *int64) *multigresv1alpha1.Shard {
		return &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
				Images: multigresv1alpha1.ShardImages{
					Postgres: "p", Multiorch: "o", Multipooler: "po",
				},
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address: "etcd", RootPath: "/", Implementation: "etcd",
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"rw": {
						Cells:   []multigresv1alpha1.CellName{"cell-1"},
						FSGroup: ptr.To(int64(2000)),
						Postgres: multigresv1alpha1.ContainerConfig{
							RunAsUser:  postgresUID,
							RunAsGroup: ptr.To(int64(0)),
						},
						Multipooler: multigresv1alpha1.ContainerConfig{
							RunAsUser:  multipoolerUID,
							RunAsGroup: ptr.To(int64(0)),
						},
					},
				},
			},
		}
	}

	t.Run("allows different PGDATA UIDs at API layer because PoolSpec may be partial", func(t *testing.T) {
		shard := newShard("cel-runtime-uid-mismatch", ptr.To(int64(1000)), ptr.To(int64(3000)))
		setTestShardPostgresPasswordSecretRef(shard)
		if err := getPrivilegedClient(t).Create(ctx, shard); err != nil {
			t.Fatalf("expected API validation to allow partial PoolSpec identity, got %v", err)
		}
	})

	t.Run("allows multipooler-only UID at API layer because PoolSpec may be partial", func(t *testing.T) {
		shard := newShard("cel-runtime-multipooler-only", nil, ptr.To(int64(1000)))
		setTestShardPostgresPasswordSecretRef(shard)
		if err := getPrivilegedClient(t).Create(ctx, shard); err != nil {
			t.Fatalf("expected API validation to allow partial PoolSpec identity, got %v", err)
		}
	})

	t.Run("accepts root primary and filesystem groups", func(t *testing.T) {
		shard := newShard("cel-runtime-root-group", ptr.To(int64(1000)), ptr.To(int64(1000)))
		setTestShardPostgresPasswordSecretRef(shard)
		if err := getPrivilegedClient(t).Create(ctx, shard); err != nil {
			t.Fatalf("expected group ID 0 to be accepted, got %v", err)
		}
	})
}

func TestCEL_S3ServiceAccountName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		shard       client.Object
		expectError string
		shouldPass  bool
	}{
		{
			name: "Rejected: serviceAccountName with credentialsSecret",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-s3-sa-creds-conflict", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {Type: "readWrite", Cells: []multigresv1alpha1.CellName{"cell-1"}, ReplicasPerCell: ptr.To(int32(1))},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket:             "my-bucket",
							Region:             "us-east-1",
							ServiceAccountName: "multigres-backup",
							CredentialsSecret:  "aws-creds",
						},
					},
				},
			},
			expectError: "serviceAccountName and credentialsSecret are mutually exclusive",
		},
		{
			name: "Rejected: serviceAccountName with useEnvCredentials",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-s3-sa-envcreds-conflict", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {Type: "readWrite", Cells: []multigresv1alpha1.CellName{"cell-1"}, ReplicasPerCell: ptr.To(int32(1))},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket:             "my-bucket",
							Region:             "us-east-1",
							ServiceAccountName: "multigres-backup",
							UseEnvCredentials:  true,
						},
					},
				},
			},
			expectError: "serviceAccountName and useEnvCredentials are mutually exclusive",
		},
		{
			name: "Accepted: serviceAccountName alone",
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "cel-s3-sa-only", Namespace: testNamespace},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "postgres", TableGroupName: "default", ShardName: "0",
					Images:           multigresv1alpha1.ShardImages{Postgres: "p", Multiorch: "o", Multipooler: "po"},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{Address: "etcd", RootPath: "/", Implementation: "etcd"},
					Multiorch:        multigresv1alpha1.MultiorchSpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"rw": {Type: "readWrite", Cells: []multigresv1alpha1.CellName{"cell-1"}, ReplicasPerCell: ptr.To(int32(1))},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket:             "my-bucket",
							Region:             "us-east-1",
							ServiceAccountName: "multigres-backup",
						},
					},
				},
			},
			shouldPass: true,
		},
	}

	privClient := getPrivilegedClient(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setTestObjectPostgresPasswordSecretRef(tc.shard)
			err := privClient.Create(ctx, tc.shard)
			if tc.shouldPass {
				if err != nil {
					t.Fatalf("Expected success, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tc.expectError != "" && !strings.Contains(err.Error(), tc.expectError) {
				t.Logf("ACTUAL ERROR: %v", err)
				t.Errorf("Expected error message to contain %q, got %q", tc.expectError, err.Error())
			}
		})
	}
}

func TestCEL_StatelessReplicasLimit(t *testing.T) {
	t.Parallel()

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-stateless-replicas-limit", Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Multiadmin: &multigresv1alpha1.MultiadminConfig{
				Spec: &multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(129)), // > 128
				},
			},
		},
	}

	setTestPostgresPasswordSecretRef(cluster)

	err := k8sClient.Create(ctx, cluster)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "less than or equal to 128") {
		t.Errorf("Expected error to contain 'less than or equal to 128', got %v", err)
	}
}
