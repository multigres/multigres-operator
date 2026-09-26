package topo_test

import (
	"path"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"

	"github.com/multigres/testkit/assert"
)

// Inspect the backing record version, not just the returned value: rewriting
// identical protobuf bytes still consumes a new etcd revision.
func TestRegistrationDoesNotRewriteUnchangedRecords(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"cell", "database", "shard-database"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			c := assert.NewAborting(t)
			ctx := t.Context()
			store := newMemoryStore(t)
			recorder := record.NewFakeRecorder(100)
			owner := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
			}
			cell := multigresv1alpha1.CellConfig{Name: "cell1"}
			backup := &multigresv1alpha1.BackupConfig{
				Type:       multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups"},
			}
			shard := newTestShard("shard")
			register := func() error {
				switch kind {
				case "cell":
					return topo.RegisterCellFromSpec(
						ctx,
						store,
						recorder,
						owner,
						cell,
						nil,
						multigresv1alpha1.GlobalTopoServerRef{
							Address:  "topo:2379",
							RootPath: "/global",
						},
					)
				case "database":
					return topo.RegisterDatabaseFromSpec(
						ctx,
						store,
						recorder,
						owner,
						multigresv1alpha1.DatabaseConfig{
							Name: "test-db",
						},
						[]string{"cell1"},
						backup,
						"",
					)
				default:
					return topo.RegisterDatabase(ctx, store, recorder, shard)
				}
			}
			file := path.Join(topoclient.DatabasesPath, "test-db", topoclient.DatabaseFile)
			if kind == "cell" {
				file = path.Join(topoclient.CellsPath, "cell1", topoclient.CellFile)
			}
			conn, err := store.ConnForCell(ctx, topoclient.GlobalCell)
			c.NoError(err)
			version := func() string {
				t.Helper()
				_, v, err := conn.Get(ctx, file)
				c.NoError(err)
				return v.String()
			}
			c.NoError(register())
			initial := version()
			for range 5 {
				c.NoError(register())
				c.Eq(initial, version(), "unchanged registration rewrote record")
			}
			switch kind {
			case "cell":
				cell.Metadata = `{"region":"new"}`
			case "database":
				backup.Filesystem.Path = "/new-backups"
			default:
				shard.Spec.Pools["pool2"] = multigresv1alpha1.PoolSpec{
					Cells: []multigresv1alpha1.CellName{"cell2"},
				}
			}
			c.NoError(register())
			changed := version()
			c.NotEq(initial, changed, "changed registration did not update record")
			c.NoError(register())
			c.Eq(changed, version(), "registration did not converge after change")
		})
	}
}
