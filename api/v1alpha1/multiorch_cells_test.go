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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestPoolCellUnionFitsMultiorchCells checks the bound shapeShardForApply
// relies on when it writes the union of a shard's pool cells into
// multiorch.cells: the cluster validator accepts only pool cells the cluster
// defines (pkg/resolver ValidateClusterLogic, tested there), so the union is
// at most the number of cells a MultigresCluster may define, which must not
// exceed what multiorch.cells may hold on a TableGroup or a Shard.
func TestPoolCellUnionFitsMultiorchCells(t *testing.T) {
	t.Parallel()

	clusterCells := crdMaxItems(t, "multigresclusters", "spec", "cells")
	for _, tc := range []struct {
		crd  string
		path []string
	}{
		{"shards", []string{"spec", "multiorch", "cells"}},
		{"tablegroups", []string{"spec", "shards", "[]", "multiorch", "cells"}},
	} {
		if limit := crdMaxItems(t, tc.crd, tc.path...); clusterCells > limit {
			t.Errorf("a cluster may define %d cells, but %s %v holds at most %d",
				clusterCells, tc.crd, tc.path, limit)
		}
	}

	pools := map[PoolName]PoolSpec{}
	for i := range clusterCells {
		cell := CellName(fmt.Sprintf("zone-%02d", i))
		pools[PoolName(fmt.Sprintf("p%d", i%8))] = PoolSpec{
			Cells: append(
				pools[PoolName(fmt.Sprintf("p%d", i%8))].Cells,
				cell,
				CellName("zone-00"),
			),
		}
	}
	var multiorch MultiorchSpec
	shapeShardForApply(&multiorch, pools)
	if len(multiorch.Cells) != clusterCells {
		t.Errorf("union of pools spanning %d cells named %d", clusterCells, len(multiorch.Cells))
	}
}

// crdMaxItems returns the maxItems of the array at path in the v1alpha1
// schema of the CRD for plural; "[]" steps into an array's items.
func crdMaxItems(t *testing.T, plural string, path ...string) int {
	t.Helper()
	file := filepath.Join("..", "..", "config", "crd", "bases", "multigres.com_"+plural+".yaml")
	raw, err := os.ReadFile(file) //nolint:gosec // a CRD of this repository, named by the test
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	for _, v := range crd["spec"].(map[string]any)["versions"].([]any) {
		if v.(map[string]any)["name"] == "v1alpha1" {
			node = v.(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
		}
	}
	for _, step := range path {
		if step == "[]" {
			node = node["items"].(map[string]any)
			continue
		}
		next, ok := node["properties"].(map[string]any)[step].(map[string]any)
		if !ok {
			t.Fatalf("%s: no %s in %v", plural, step, path)
		}
		node = next
	}
	n, ok := node["maxItems"].(float64)
	if !ok {
		t.Fatalf("%s %v has no maxItems", plural, path)
	}
	return int(n)
}
