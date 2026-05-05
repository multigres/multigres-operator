package multigrescluster

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/multigres/multigres-operator/pkg/util/name"
)

func TestBuildTableGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	dbCfg := multigresv1alpha1.DatabaseConfig{
		Name: "my-db",
	}
	globalTopoRef := multigresv1alpha1.GlobalTopoServerRef{}

	t.Run("Success", func(t *testing.T) {
		tgCfg := &multigresv1alpha1.TableGroupConfig{
			Name: "tg-1",
		}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{
			{Name: "shard-0"},
		}

		got, err := BuildTableGroup(cluster, dbCfg, tgCfg, resolvedShards, globalTopoRef, scheme)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}

		// Calculate expected hash: md5("my-cluster", "my-db", "tg-1") -> "d5708433"
		expectedName := name.JoinWithConstraints(
			name.DefaultConstraints,
			"my-cluster",
			"my-db",
			"tg-1",
		)
		if got.Name != expectedName {
			t.Errorf("Name = %v, want %v", got.Name, expectedName)
		}
		if got.Labels["multigres.com/database"] != "my-db" {
			t.Errorf("Label[database] = %v, want my-db", got.Labels["multigres.com/database"])
		}

		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		}
	})

	t.Run("CustomPostgresSuperuser", func(t *testing.T) {
		c := *cluster
		c.Spec.PostgresSuperuser = "admin"
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-superuser"}
		got, err := BuildTableGroup(&c, dbCfg, tgCfg, nil, globalTopoRef, scheme)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}
		if got.Spec.PostgresSuperuser != "admin" {
			t.Errorf(
				"PostgresSuperuser = %q, want %q",
				got.Spec.PostgresSuperuser,
				"admin",
			)
		}
	})

	t.Run("Name Truncation", func(t *testing.T) {
		longName := strings.Repeat("a", 250) // Very long name
		tgCfg := &multigresv1alpha1.TableGroupConfig{
			Name: multigresv1alpha1.TableGroupName(longName),
		}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{}

		got, err := BuildTableGroup(cluster, dbCfg, tgCfg, resolvedShards, globalTopoRef, scheme)
		if err != nil {
			t.Errorf("BuildTableGroup() error = %v, want nil", err)
		}
		// Should be truncated to 253 chars
		if len(got.Name) > 253 {
			t.Errorf("Expected name length <= 253, got %d", len(got.Name))
		}
		// Confirm it ends with a hash (8 chars)
		// and has the truncation mark "---"
		if !strings.Contains(got.Name, "---") {
			t.Errorf("Expected truncation mark '---', got %s", got.Name)
		}
	})

	t.Run("CellTopologyLabels ZoneID", func(t *testing.T) {
		c := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "az-cell", ZoneID: "use1-az1"}},
			},
		}
		got, err := BuildTableGroup(
			c,
			dbCfg,
			&multigresv1alpha1.TableGroupConfig{Name: "tg"},
			[]multigresv1alpha1.ShardResolvedSpec{{Name: "s0"}},
			globalTopoRef,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}
		if got.Spec.CellTopologyLabels["az-cell"]["topology.k8s.aws/zone-id"] != "use1-az1" {
			t.Errorf(
				"expected topology.k8s.aws/zone-id=use1-az1, got %v",
				got.Spec.CellTopologyLabels["az-cell"],
			)
		}
	})

	t.Run("CellTopologyLabels ZoneID only", func(t *testing.T) {
		c := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "both-cell", ZoneID: "use1-az1"},
				},
			},
		}
		got, err := BuildTableGroup(
			c,
			dbCfg,
			&multigresv1alpha1.TableGroupConfig{Name: "tg"},
			[]multigresv1alpha1.ShardResolvedSpec{{Name: "s0"}},
			globalTopoRef,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}
		labels := got.Spec.CellTopologyLabels["both-cell"]
		if labels["topology.k8s.aws/zone-id"] != "use1-az1" {
			t.Errorf("expected topology.k8s.aws/zone-id=use1-az1, got %v", labels)
		}
	})

	t.Run("CellTopologyLabels Region", func(t *testing.T) {
		regionCluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "region-cell", Region: "us-east-1"},
				},
			},
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-region"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{{Name: "shard-0"}}

		got, err := BuildTableGroup(
			regionCluster,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}
		labels, ok := got.Spec.CellTopologyLabels["region-cell"]
		if !ok {
			t.Fatal("Expected CellTopologyLabels to contain region-cell")
		}
		if labels["topology.kubernetes.io/region"] != "us-east-1" {
			t.Errorf(
				"Expected region label us-east-1, got %s",
				labels["topology.kubernetes.io/region"],
			)
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg1"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{}
		emptyScheme := runtime.NewScheme()

		_, err := BuildTableGroup(
			cluster,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			emptyScheme,
		)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})

	t.Run("Propagates explicit project ref annotation", func(t *testing.T) {
		clusterWithProjectRef := cluster.DeepCopy()
		clusterWithProjectRef.Annotations = map[string]string{
			metadata.AnnotationProjectRef: "proj_123",
		}
		tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "tg-with-project-ref"}
		resolvedShards := []multigresv1alpha1.ShardResolvedSpec{{Name: "shard-0"}}

		got, err := BuildTableGroup(
			clusterWithProjectRef,
			dbCfg,
			tgCfg,
			resolvedShards,
			globalTopoRef,
			scheme,
		)
		if err != nil {
			t.Fatalf("BuildTableGroup() error = %v", err)
		}

		if got.Annotations[metadata.AnnotationProjectRef] != "proj_123" {
			t.Fatalf(
				"annotation %q = %q, want %q",
				metadata.AnnotationProjectRef,
				got.Annotations[metadata.AnnotationProjectRef],
				"proj_123",
			)
		}
	})
}
