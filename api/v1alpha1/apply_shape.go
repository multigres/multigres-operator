/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"encoding/json"
	"slices"

	corev1 "k8s.io/api/core/v1"
)

// A server-side apply that sends an empty object ({}) for a block that held
// entries is rejected by the API server, which reads the emptied block as
// null. The ForApply methods shape what a controller applies to a child
// resource so that never happens, field by field: a non-pointer block that
// asks for nothing becomes its zero value, which omitzero leaves out, and a
// pointer block that would encode as {} becomes nil. The fields are listed
// rather than every {} pruned from the payload, because elsewhere an empty
// object means something of its own (an empty label selector selects
// everything). TestChildSpecsApplyNoEmptyBlocks fails for a block that can
// encode as {} and is neither shaped here nor exempted there.
//
// Every consumer reads a shaped field the same whether it is nil or empty,
// with one effect that is not a reading: an affinity is part of the pod
// template, so a workload rendered from a stored empty affinity is rolled
// once when the empty affinity is dropped.
//
// They are for the apply boundary only: what the defaulting webhook writes
// back into a MultigresCluster keeps an emptied block as written. A resources
// block holding an empty requests, limits or claims member (`resources:
// {limits: {}}`, which is what deleting the last entry with kubectl edit
// leaves) means "no requests or limits" and must stay distinguishable from an
// absent one. A bare `resources: {}` is the zero value, the same as an absent
// block, and inherits.

// DefaultFilesystemBackupPath is the backup directory used when a filesystem
// backup names none.
const DefaultFilesystemBackupPath = "/backups"

// ForApply returns a copy of s shaped for applying a TableGroup. Each ForApply
// copies its spec once and shapes the copy in place.
func (s *TableGroupSpec) ForApply() TableGroupSpec {
	out := *s.DeepCopy()
	for i := range out.Shards {
		out.Shards[i].shapeForApply()
	}
	out.PVCDeletionPolicy = emptyToNil(out.PVCDeletionPolicy)
	out.Backup = out.Backup.shapeForApply()
	out.InternalTLS = emptyToNil(out.InternalTLS)
	out.Observability = emptyToNil(out.Observability)
	out.TopologyPruning = emptyToNil(out.TopologyPruning)
	return out
}

// ForApply returns a copy of s shaped for applying a Shard; see
// shapeShardForApply.
func (s *ShardSpec) ForApply() ShardSpec {
	out := *s.DeepCopy()
	shapeShardForApply(&out.Multiorch, out.Pools)
	out.GatewayClusterViews = emptyToNil(out.GatewayClusterViews)
	out.PVCDeletionPolicy = emptyToNil(out.PVCDeletionPolicy)
	out.Backup = out.Backup.shapeForApply()
	out.InternalTLS = emptyToNil(out.InternalTLS)
	out.Observability = emptyToNil(out.Observability)
	out.TopologyPruning = emptyToNil(out.TopologyPruning)
	return out
}

// shapeForApply shapes a TableGroup's shard in place; see shapeShardForApply.
func (s *ShardResolvedSpec) shapeForApply() {
	shapeShardForApply(&s.Multiorch, s.Pools)
	s.GatewayClusterViews = emptyToNil(s.GatewayClusterViews)
	s.PVCDeletionPolicy = emptyToNil(s.PVCDeletionPolicy)
	s.Backup = s.Backup.shapeForApply()
}

// ForApply returns a copy of s shaped for applying a Cell.
func (s *CellSpec) ForApply() CellSpec {
	out := *s.DeepCopy()
	out.Multigateway.shapeForApply()
	out.MultigatewayPlacement = emptyToNil(out.MultigatewayPlacement)
	if out.TopoServer != nil {
		out.TopoServer.Etcd.shapeForApply()
	}
	out.TopoServer = emptyToNil(out.TopoServer)
	out.Observability = emptyToNil(out.Observability)
	out.InternalTLS = emptyToNil(out.InternalTLS)
	return out
}

// ForApply returns a copy of s shaped for applying a TopoServer.
func (s *TopoServerSpec) ForApply() TopoServerSpec {
	out := *s.DeepCopy()
	out.Etcd.shapeForApply()
	out.PVCDeletionPolicy = emptyToNil(out.PVCDeletionPolicy)
	out.Placement = out.Placement.shapeForApply()
	return out
}

// shapeForApply shapes b in place and returns it, or nil when b is nil. A
// filesystem backup must keep its filesystem block, which the CRD requires,
// so an emptied one names the default path, which is what every reader uses
// when the path is empty.
func (b *BackupConfig) shapeForApply() *BackupConfig {
	if b == nil {
		return nil
	}
	b.PgBackRestTLS = emptyToNil(b.PgBackRestTLS)
	if b.Filesystem != nil && len(b.Filesystem.Storage.AccessModes) == 0 {
		// Every reader treats an empty access mode list as unset, and the
		// storage block is omitzero, so a block left with nothing in it is
		// left out.
		b.Filesystem.Storage.AccessModes = nil
	}
	if b.Type == BackupTypeFilesystem {
		if b.Filesystem == nil || serializesEmpty(b.Filesystem) {
			b.Filesystem = &FilesystemBackupConfig{Path: DefaultFilesystemBackupPath}
		}
	} else {
		b.Filesystem = emptyToNil(b.Filesystem)
	}
	return b
}

// shapeShardForApply shapes a shard's multiorch and pools in place. A
// multiorch block that would otherwise be empty names its cells: the field is
// required, so it cannot be left out, and the cells are the union of the pool
// cells, which is what the Shard controller deploys multiorch to when cells
// are not set. The union fits multiorch.cells, which holds at most 50:
// TableGroups and Shards are written only by the operator, from a cluster
// whose validator accepts only pool cells the cluster defines, and a cluster
// defines at most 50 (TestPoolCellUnionFitsMultiorchCells). A shard
// whose pools name no cell still sends multiorch as {}; the Shard controller
// refuses such a shard anyway.
func shapeShardForApply(multiorch *MultiorchSpec, pools map[PoolName]PoolSpec) {
	multiorch.shapeForApply()
	for name, pool := range pools {
		pool.shapeForApply()
		pools[name] = pool
	}
	if len(multiorch.Cells) == 0 && serializesEmpty(multiorch) {
		multiorch.Cells = poolCells(pools)
	}
}

// shapeForApply shapes m in place for applying a Cell.
func (m *MultigatewaySpec) shapeForApply() {
	m.StatelessSpec.shapeForApply()
	m.Buffer = emptyToNil(m.Buffer)
	m.ClusterViews = emptyToNil(m.ClusterViews)
}

// shapeForApply shapes e in place for applying a Cell or a TopoServer. A nil
// e is left nil.
func (e *EtcdSpec) shapeForApply() {
	if e == nil {
		return
	}
	NormalizeResources(&e.Resources)
	e.Maintenance = emptyToNil(e.Maintenance)
	e.PVCDeletionPolicy = emptyToNil(e.PVCDeletionPolicy)
}

// shapeForApply removes p's emptied affinity in place and returns p, or nil
// when p is nil or empty.
func (p *TopoServerPlacementSpec) shapeForApply() *TopoServerPlacementSpec {
	if p != nil {
		p.Affinity = affinityForApply(p.Affinity)
	}
	return emptyToNil(p)
}

func (s *StatelessSpec) shapeForApply() {
	NormalizeResources(&s.Resources)
	s.Affinity = affinityForApply(s.Affinity)
}

func (m *MultiorchSpec) shapeForApply() {
	m.StatelessSpec.shapeForApply()
	m.Placement = emptyToNil(m.Placement)
}

func (p *PoolSpec) shapeForApply() {
	NormalizeResources(&p.Postgres.Resources)
	NormalizeResources(&p.Multipooler.Resources)
	p.Affinity = affinityForApply(p.Affinity)
	p.PVCDeletionPolicy = emptyToNil(p.PVCDeletionPolicy)
}

// affinityForApply removes the emptied node, pod and pod anti-affinity blocks
// of a, then a itself when nothing is left. It changes a in place.
func affinityForApply(a *corev1.Affinity) *corev1.Affinity {
	if a == nil {
		return nil
	}
	a.NodeAffinity = emptyToNil(a.NodeAffinity)
	a.PodAffinity = emptyToNil(a.PodAffinity)
	a.PodAntiAffinity = emptyToNil(a.PodAntiAffinity)
	return emptyToNil(a)
}

// emptyToNil returns nil when p encodes as {}, and p otherwise.
func emptyToNil[T any](p *T) *T {
	if p != nil && serializesEmpty(p) {
		return nil
	}
	return p
}

// serializesEmpty reports whether v encodes as an empty JSON object.
func serializesEmpty(v any) bool {
	b, err := json.Marshal(v)
	return err == nil && string(b) == "{}"
}

// poolCells returns the sorted union of the cells of pools.
func poolCells(pools map[PoolName]PoolSpec) []CellName {
	var cells []CellName
	for _, pool := range pools {
		cells = append(cells, pool.Cells...)
	}
	slices.Sort(cells)
	return slices.Compact(cells)
}
