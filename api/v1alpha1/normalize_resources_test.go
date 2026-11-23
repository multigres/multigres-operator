package v1alpha1

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNormalizeResourcesOmitsEmptyBlocks(t *testing.T) {
	t.Parallel()

	for name, block := range map[string]corev1.ResourceRequirements{
		"zero":           {},
		"empty requests": {Requests: corev1.ResourceList{}},
		"empty limits":   {Limits: corev1.ResourceList{}},
		"empty claims":   {Claims: []corev1.ResourceClaim{}},
		"all empty": {
			Requests: corev1.ResourceList{},
			Limits:   corev1.ResourceList{},
			Claims:   []corev1.ResourceClaim{},
		},
	} {
		NormalizeResources(&block)
		pool := PoolSpec{
			Postgres:    ContainerConfig{Resources: block},
			Multipooler: ContainerConfig{Resources: block},
		}
		withUser := PoolSpec{Postgres: ContainerConfig{
			Resources: block,
			RunAsUser: new(int64(999)),
		}}
		orch := StatelessSpec{Resources: block}
		etcd := EtcdSpec{Resources: block}
		for what, v := range map[string]any{
			"pool": pool, "pool with runAsUser": withUser, "stateless": orch, "etcd": etcd,
		} {
			out, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			keys := []string{`"resources"`, `"multipooler"`}
			if what != "pool with runAsUser" {
				keys = append(keys, `"postgres"`)
			}
			for _, key := range keys {
				if strings.Contains(string(out), key) {
					t.Errorf("%s %s serializes as %s, want no %s key", name, what, out, key)
				}
			}
		}
	}

	kept := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	NormalizeResources(&kept)
	if len(kept.Limits) != 1 {
		t.Errorf("a block with an entry was changed: %#v", kept)
	}
	out, err := json.Marshal(StatelessSpec{Resources: kept})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"resources":{"limits":{"memory":"1Gi"}}}`; string(out) != want {
		t.Errorf("serializes as %s, want %s", out, want)
	}

	claims := corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{{Name: "gpu"}}}
	NormalizeResources(&claims)
	if len(claims.Claims) != 1 {
		t.Errorf("a block with only a claim was changed: %#v", claims)
	}
}

// TestForApplyKeepsBlocksWithEntries requires every block the ForApply
// methods shape to survive when it has an entry; that emptied blocks are
// dropped is TestChildSpecsApplyNoEmptyBlocks.
func TestForApplyKeepsBlocksWithEntries(t *testing.T) {
	t.Parallel()

	affinity := func() *corev1.Affinity {
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "example.com/pool", Operator: corev1.NodeSelectorOpExists,
					}},
				}},
			},
		}}
	}
	placement := &PodPlacementSpec{Tolerations: []corev1.Toleration{
		{Key: "example.com/dedicated", Operator: corev1.TolerationOpExists},
	}}
	retain := &PVCDeletionPolicy{WhenDeleted: RetainPVCRetentionPolicy}
	enabled := new(true)
	observability := &ObservabilityConfig{OTLPEndpoint: "http://otel:4318"}
	backup := &BackupConfig{
		Type:          BackupTypeFilesystem,
		Filesystem:    &FilesystemBackupConfig{Path: "/data", Storage: StorageSpec{Size: "5Gi"}},
		PgBackRestTLS: &PgBackRestTLSConfig{SecretName: "tls"},
	}
	multiorch := MultiorchSpec{
		StatelessSpec: StatelessSpec{Affinity: affinity()},
		Placement:     placement,
	}
	pools := map[PoolName]PoolSpec{"p": {Affinity: affinity(), PVCDeletionPolicy: retain}}

	shard := (&ShardSpec{
		Multiorch:           multiorch,
		Pools:               pools,
		GatewayClusterViews: &GatewayClusterViewsConfig{Enabled: enabled},
		PVCDeletionPolicy:   retain,
		Backup:              backup,
		InternalTLS:         &InternalTLSConfig{Enabled: enabled},
		Observability:       observability,
		TopologyPruning:     &TopologyPruningConfig{Enabled: enabled},
	}).ForApply()
	tg := (&TableGroupSpec{
		Shards: []ShardResolvedSpec{{
			Multiorch:           multiorch,
			Pools:               pools,
			GatewayClusterViews: &GatewayClusterViewsConfig{Enabled: enabled},
			PVCDeletionPolicy:   retain,
			Backup:              backup,
		}},
		PVCDeletionPolicy: retain,
		Backup:            backup,
		InternalTLS:       &InternalTLSConfig{Enabled: enabled},
		Observability:     observability,
		TopologyPruning:   &TopologyPruningConfig{Enabled: enabled},
	}).ForApply()
	cell := (&CellSpec{
		Multigateway: MultigatewaySpec{
			StatelessSpec: StatelessSpec{Affinity: affinity()},
			Buffer:        &GatewayBufferConfig{Enabled: enabled},
			ClusterViews:  &GatewayClusterViewsConfig{Enabled: enabled},
		},
		MultigatewayPlacement: placement,
		TopoServer: &LocalTopoServerSpec{Etcd: &EtcdSpec{
			Image:             "etcd",
			Maintenance:       &EtcdMaintenanceConfig{AutoCompactionMode: "revision"},
			PVCDeletionPolicy: retain,
		}},
		Observability: observability,
		InternalTLS:   &InternalTLSConfig{Enabled: enabled},
	}).ForApply()
	topo := (&TopoServerSpec{
		Etcd:              &EtcdSpec{Image: "etcd", PVCDeletionPolicy: retain},
		PVCDeletionPolicy: retain,
		Placement:         &TopoServerPlacementSpec{Affinity: affinity()},
	}).ForApply()

	for name, kept := range map[string]bool{
		"shard multiorch affinity":   shard.Multiorch.Affinity != nil,
		"shard multiorch placement":  shard.Multiorch.Placement != nil,
		"shard pool affinity":        shard.Pools["p"].Affinity != nil,
		"shard pool policy":          shard.Pools["p"].PVCDeletionPolicy != nil,
		"shard cluster views":        shard.GatewayClusterViews != nil,
		"shard policy":               shard.PVCDeletionPolicy != nil,
		"shard backup filesystem":    shard.Backup.Filesystem.Path == "/data",
		"shard backup pgbackrestTLS": shard.Backup.PgBackRestTLS != nil,
		"shard internalTLS":          shard.InternalTLS != nil,
		"shard observability":        shard.Observability != nil,
		"shard topologyPruning":      shard.TopologyPruning != nil,
		"tablegroup shard affinity":  tg.Shards[0].Multiorch.Affinity != nil,
		"tablegroup shard views":     tg.Shards[0].GatewayClusterViews != nil,
		"tablegroup shard backup":    tg.Shards[0].Backup.PgBackRestTLS != nil,
		"tablegroup policy":          tg.PVCDeletionPolicy != nil,
		"tablegroup backup":          tg.Backup.Filesystem.Storage.Size == "5Gi",
		"tablegroup internalTLS":     tg.InternalTLS != nil,
		"tablegroup observability":   tg.Observability != nil,
		"tablegroup topologyPruning": tg.TopologyPruning != nil,
		"cell gateway affinity":      cell.Multigateway.Affinity != nil,
		"cell gateway buffer":        cell.Multigateway.Buffer != nil,
		"cell gateway views":         cell.Multigateway.ClusterViews != nil,
		"cell gateway placement":     cell.MultigatewayPlacement != nil,
		"cell etcd maintenance":      cell.TopoServer.Etcd.Maintenance != nil,
		"cell etcd policy":           cell.TopoServer.Etcd.PVCDeletionPolicy != nil,
		"cell observability":         cell.Observability != nil,
		"cell internalTLS":           cell.InternalTLS != nil,
		"topo etcd policy":           topo.Etcd.PVCDeletionPolicy != nil,
		"topo policy":                topo.PVCDeletionPolicy != nil,
		"topo placement affinity":    topo.Placement.Affinity != nil,
	} {
		if !kept {
			t.Errorf("ForApply dropped %s, which has an entry", name)
		}
	}

	// An affinity keeps its non-empty parts and drops the empty ones.
	podAffinity := &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: "kubernetes.io/hostname",
		}},
	}
	for _, tc := range []struct {
		in       corev1.Affinity
		wantNode bool
		wantPod  bool
	}{
		{corev1.Affinity{NodeAffinity: affinity().NodeAffinity, PodAffinity: &corev1.PodAffinity{}}, true, false},
		{corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}, PodAffinity: podAffinity}, false, true},
	} {
		gw := MultigatewaySpec{StatelessSpec: StatelessSpec{Affinity: tc.in.DeepCopy()}}
		gw.shapeForApply()
		got := gw.Affinity
		if got == nil || (got.NodeAffinity != nil) != tc.wantNode ||
			(got.PodAffinity != nil) != tc.wantPod || got.PodAntiAffinity != nil {
			t.Errorf("shapeForApply(%+v) affinity = %+v", tc.in, got)
		}
	}

	// A filesystem backup keeps its required filesystem block, naming the
	// default path; another type drops an emptied one.
	fs := (&BackupConfig{Type: BackupTypeFilesystem, Filesystem: &FilesystemBackupConfig{}}).shapeForApply()
	if fs.Filesystem == nil || fs.Filesystem.Path != DefaultFilesystemBackupPath {
		t.Errorf("emptied filesystem backup = %+v", fs.Filesystem)
	}
	s3 := (&BackupConfig{Type: BackupTypeS3, Filesystem: &FilesystemBackupConfig{}}).shapeForApply()
	if s3.Filesystem != nil {
		t.Errorf("emptied filesystem block of an s3 backup = %+v", s3.Filesystem)
	}
	if (*BackupConfig)(nil).shapeForApply() != nil {
		t.Error("nil backup did not stay nil")
	}
	if shard.Backup == backup || backup.Filesystem.Path != "/data" {
		t.Error("ForApply shared or changed its input")
	}
}

func TestShardResolvedSpecForApply(t *testing.T) {
	t.Parallel()

	empty := corev1.ResourceRequirements{Limits: corev1.ResourceList{}}
	in := ShardResolvedSpec{
		Name:      "s",
		Multiorch: MultiorchSpec{StatelessSpec: StatelessSpec{Resources: *empty.DeepCopy()}},
		Pools: map[PoolName]PoolSpec{
			"b": {
				Cells:    []CellName{"zone-b", "zone-a"},
				Postgres: ContainerConfig{Resources: *empty.DeepCopy()},
			},
			"a": {
				Cells:       []CellName{"zone-a"},
				Multipooler: ContainerConfig{Resources: *empty.DeepCopy()},
			},
		},
	}
	out := (&TableGroupSpec{Shards: []ShardResolvedSpec{in}}).ForApply().Shards[0]

	// An emptied multiorch block would apply as {}, so it names the pool cells.
	if got, want := out.Multiorch.Cells, []CellName{"zone-a", "zone-b"}; !slices.Equal(got, want) {
		t.Errorf("multiorch cells = %v, want %v", got, want)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"resources"`, `"postgres"`, `"multipooler"`} {
		if strings.Contains(string(b), key) {
			t.Errorf("ForApply output %s still carries %s", b, key)
		}
	}
	// The input keeps the explicitly empty blocks.
	if in.Multiorch.Resources.Limits == nil || in.Pools["b"].Postgres.Resources.Limits == nil ||
		in.Multiorch.Cells != nil {
		t.Errorf("ForApply changed its input: %+v", in)
	}

	// A multiorch block with something in it is left as it is.
	set := ShardResolvedSpec{
		Multiorch: MultiorchSpec{StatelessSpec: StatelessSpec{Replicas: new(int32(1))}},
		Pools:     in.Pools,
	}
	got := (&TableGroupSpec{Shards: []ShardResolvedSpec{set}}).ForApply().Shards[0].Multiorch.Cells
	if got != nil {
		t.Errorf("multiorch with replicas got cells %v", got)
	}
}

func TestEtcdAndGatewayForApply(t *testing.T) {
	t.Parallel()

	if got := (&TopoServerSpec{}).ForApply(); got.Etcd != nil {
		t.Errorf("nil etcd became %+v", got.Etcd)
	}
	ts := &TopoServerSpec{Etcd: &EtcdSpec{
		Image:     "etcd",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{}},
	}}
	if got := ts.ForApply(); got.Etcd.Resources.Requests != nil || got.Etcd.Image != "etcd" {
		t.Errorf("TopoServerSpec.ForApply etcd = %+v", got.Etcd)
	}
	if ts.Etcd.Resources.Requests == nil {
		t.Error("TopoServerSpec.ForApply changed its input")
	}
	cell := &CellSpec{Multigateway: MultigatewaySpec{
		StatelessSpec: StatelessSpec{
			Resources: corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{}},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{},
				PodAffinity:  podAffinityOnHost(),
			},
		},
	}}
	got := cell.ForApply()
	if got.Multigateway.Resources.Claims != nil || got.Multigateway.Affinity.NodeAffinity != nil {
		t.Errorf("CellSpec.ForApply multigateway = %+v", got.Multigateway)
	}
	if cell.Multigateway.Resources.Claims == nil || cell.Multigateway.Affinity.NodeAffinity == nil {
		t.Error("CellSpec.ForApply changed its input")
	}
}

func podAffinityOnHost() *corev1.PodAffinity {
	return &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: "kubernetes.io/hostname",
		}},
	}
}
