package metadata_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestBuildStandardLabels(t *testing.T) {
	tests := map[string]struct {
		clusterName   string
		componentName string
		want          map[string]string
	}{
		"typical case": {
			clusterName:   "my-etcd-cluster",
			componentName: "etcd",
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "my-etcd-cluster",
				"app.kubernetes.io/component":  "etcd",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
			},
		},
		"empty strings allowed": {
			clusterName:   "",
			componentName: "",
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "",
				"app.kubernetes.io/component":  "",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := metadata.BuildStandardLabels(tc.clusterName, tc.componentName)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildStandardLabels() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMergeLabels(t *testing.T) {
	tests := map[string]struct {
		standardLabels map[string]string
		customLabels   map[string]string
		want           map[string]string
	}{
		"standard labels win on conflicts": {
			standardLabels: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "my-resource",
				"app.kubernetes.io/component":  "etcd",
				"app.kubernetes.io/managed-by": "multigres-operator",
			},
			customLabels: map[string]string{
				"app.kubernetes.io/name":      "user-app",      // conflict
				"app.kubernetes.io/component": "user-override", // conflict
				"env":                         "production",    // no conflict
				"team":                        "platform",      // no conflict
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "my-resource",
				"app.kubernetes.io/component":  "etcd",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"env":                          "production",
				"team":                         "platform",
			},
		},
		"nil maps handled correctly": {
			standardLabels: nil,
			customLabels:   nil,
			want:           map[string]string{},
		},
		"only custom labels": {
			standardLabels: nil,
			customLabels: map[string]string{
				"env":  "dev",
				"team": "platform",
			},
			want: map[string]string{
				"env":  "dev",
				"team": "platform",
			},
		},
		"only standard labels": {
			standardLabels: map[string]string{
				"app.kubernetes.io/name":      "multigres",
				"app.kubernetes.io/component": "gateway",
			},
			customLabels: nil,
			want: map[string]string{
				"app.kubernetes.io/name":      "multigres",
				"app.kubernetes.io/component": "gateway",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := metadata.MergeLabels(tc.standardLabels, tc.customLabels)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("MergeLabels() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAddMultigresLabels(t *testing.T) {
	t.Run("AddCellLabel", func(t *testing.T) {
		labels := map[string]string{"app.kubernetes.io/name": "multigres"}
		metadata.AddCellLabel(labels, "zone1")
		if labels["multigres.com/cell"] != "zone1" {
			t.Errorf("AddCellLabel failed")
		}
	})

	t.Run("AddClusterLabel", func(t *testing.T) {
		labels := map[string]string{"app.kubernetes.io/name": "multigres"}
		metadata.AddClusterLabel(labels, "prod-cluster")
		if labels["multigres.com/cluster"] != "prod-cluster" {
			t.Errorf("AddClusterLabel failed")
		}
	})

	t.Run("AddShardLabel", func(t *testing.T) {
		labels := map[string]string{"app.kubernetes.io/name": "multigres"}
		metadata.AddShardLabel(labels, "shard-0")
		if labels["multigres.com/shard"] != "shard-0" {
			t.Errorf("AddShardLabel failed")
		}
	})

	t.Run("AddDatabaseLabel", func(t *testing.T) {
		labels := map[string]string{"app.kubernetes.io/name": "multigres"}
		metadata.AddDatabaseLabel(labels, "proddb")
		if labels["multigres.com/database"] != "proddb" {
			t.Errorf("AddDatabaseLabel failed")
		}
	})

	t.Run("AddTableGroupLabel", func(t *testing.T) {
		labels := map[string]string{"app.kubernetes.io/name": "multigres"}
		metadata.AddTableGroupLabel(labels, "orders")
		if labels["multigres.com/tablegroup"] != "orders" {
			t.Errorf("AddTableGroupLabel failed")
		}
	})
}

func TestLabelOperations_ComplexScenarios(t *testing.T) {
	tests := map[string]struct {
		setupFunc func() map[string]string
		want      map[string]string
	}{
		"build standard labels then add all multigres labels": {
			setupFunc: func() map[string]string {
				labels := metadata.BuildStandardLabels("my-cluster", "shard-pool")
				metadata.AddCellLabel(labels, "zone1")
				metadata.AddClusterLabel(labels, "prod-cluster")
				metadata.AddShardLabel(labels, "shard-0")
				metadata.AddDatabaseLabel(labels, "proddb")
				metadata.AddTableGroupLabel(labels, "orders")
				return labels
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "my-cluster",
				"app.kubernetes.io/component":  "shard-pool",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cell":           "zone1",
				"multigres.com/cluster":        "prod-cluster",
				"multigres.com/shard":          "shard-0",
				"multigres.com/database":       "proddb",
				"multigres.com/tablegroup":     "orders",
			},
		},
		"merge custom labels then add multigres labels": {
			setupFunc: func() map[string]string {
				standard := metadata.BuildStandardLabels("etcd-1", "etcd")
				custom := map[string]string{
					"env":  "production",
					"team": "platform",
				}
				labels := metadata.MergeLabels(standard, custom)
				metadata.AddCellLabel(labels, "zone2")
				metadata.AddClusterLabel(labels, "main-cluster")
				return labels
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "etcd-1",
				"app.kubernetes.io/component":  "etcd",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cell":           "zone2",
				"multigres.com/cluster":        "main-cluster",
				"env":                          "production",
				"team":                         "platform",
			},
		},
		"chain all label operations for shard pool": {
			setupFunc: func() map[string]string {
				poolName := "shard-0-pool-primary"
				labels := metadata.BuildStandardLabels(poolName, "shard-pool")
				metadata.AddCellLabel(labels, "us-east-1a")
				metadata.AddClusterLabel(labels, "production")
				metadata.AddShardLabel(labels, "0")
				metadata.AddDatabaseLabel(labels, "orders_db")
				metadata.AddTableGroupLabel(labels, "orders_tg")

				custom := map[string]string{
					"monitoring": "prometheus",
					"backup":     "enabled",
				}
				return metadata.MergeLabels(labels, custom)
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "shard-0-pool-primary",
				"app.kubernetes.io/component":  "shard-pool",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cell":           "us-east-1a",
				"multigres.com/cluster":        "production",
				"multigres.com/shard":          "0",
				"multigres.com/database":       "orders_db",
				"multigres.com/tablegroup":     "orders_tg",
				"monitoring":                   "prometheus",
				"backup":                       "enabled",
			},
		},
		"merge with conflicting multigres labels - standard wins": {
			setupFunc: func() map[string]string {
				labels := metadata.BuildStandardLabels("resource", "component")
				metadata.AddCellLabel(labels, "zone1")
				metadata.AddShardLabel(labels, "shard-0")

				conflicting := map[string]string{
					"multigres.com/cell":  "zone-override",
					"multigres.com/shard": "shard-override",
					"custom":              "value",
				}
				return metadata.MergeLabels(labels, conflicting)
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "resource",
				"app.kubernetes.io/component":  "component",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cell":           "zone1",
				"multigres.com/shard":          "shard-0",
				"custom":                       "value",
			},
		},
		"add labels to empty map": {
			setupFunc: func() map[string]string {
				labels := make(map[string]string)
				metadata.AddCellLabel(labels, "zone3")
				metadata.AddShardLabel(labels, "shard-1")
				metadata.AddDatabaseLabel(labels, "testdb")
				return labels
			},
			want: map[string]string{
				"multigres.com/cell":     "zone3",
				"multigres.com/shard":    "shard-1",
				"multigres.com/database": "testdb",
			},
		},
		"overwrite cell label multiple times - last wins": {
			setupFunc: func() map[string]string {
				labels := metadata.BuildStandardLabels("resource", "component")
				metadata.AddCellLabel(labels, "zone1")
				metadata.AddCellLabel(labels, "zone2")
				metadata.AddCellLabel(labels, "zone3")
				return labels
			},
			want: map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   "resource",
				"app.kubernetes.io/component":  "component",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cell":           "zone3",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.setupFunc()
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Label operations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAddExtraLabels(t *testing.T) {
	tests := []struct {
		name     string
		initial  map[string]string
		addFunc  func(map[string]string)
		expected map[string]string
	}{
		{
			name:    "AddPoolLabel",
			initial: map[string]string{},
			addFunc: func(m map[string]string) {
				metadata.AddPoolLabel(m, "pool-1")
			},
			expected: map[string]string{
				"multigres.com/pool": "pool-1",
			},
		},
		{
			name:    "AddZoneIDLabel",
			initial: map[string]string{},
			addFunc: func(m map[string]string) {
				metadata.AddZoneIDLabel(m, "use1-az1")
			},
			expected: map[string]string{
				"multigres.com/zone-id": "use1-az1",
			},
		},
		{
			name:    "AddRegionLabel",
			initial: map[string]string{},
			addFunc: func(m map[string]string) {
				metadata.AddRegionLabel(m, "region-1")
			},
			expected: map[string]string{
				"multigres.com/region": "region-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.addFunc(tt.initial)
			if diff := cmp.Diff(tt.expected, tt.initial); diff != "" {
				t.Errorf("Labels mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetSelectorLabels(t *testing.T) {
	labels := map[string]string{
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/instance":   "inst-1",
		"app.kubernetes.io/managed-by": "operator",
		"app.kubernetes.io/part-of":    "multigres",
		"other-label":                  "value",
	}

	want := map[string]string{
		"app.kubernetes.io/instance": "inst-1",
	}

	got := metadata.GetSelectorLabels(labels)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetSelectorLabels() mismatch (-want +got):\n%s", diff)
	}
}
