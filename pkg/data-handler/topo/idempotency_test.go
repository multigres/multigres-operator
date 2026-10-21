package topo_test

import (
	"path"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

// Inspect the backing record version, not just the returned value: rewriting
// identical protobuf bytes still consumes a new etcd revision.
func TestRegistrationDoesNotRewriteUnchangedRecords(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"cell", "database"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
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
				default:
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
				}
			}
			file := path.Join(topoclient.DatabasesPath, "test-db", topoclient.DatabaseFile)
			if kind == "cell" {
				file = path.Join(topoclient.CellsPath, "cell1", topoclient.CellFile)
			}
			conn, err := store.ConnForCell(ctx, topoclient.GlobalCell)
			if err != nil {
				t.Fatal(err)
			}
			version := func() string {
				t.Helper()
				_, v, err := conn.Get(ctx, file)
				if err != nil {
					t.Fatal(err)
				}
				return v.String()
			}
			if err := register(); err != nil {
				t.Fatal(err)
			}
			initial := version()
			for range 5 {
				if err := register(); err != nil {
					t.Fatal(err)
				}
				if got := version(); got != initial {
					t.Fatalf("unchanged registration rewrote record: %s -> %s", initial, got)
				}
			}
			if kind == "cell" {
				cell.Metadata = `{"region":"new"}`
			} else {
				backup.Filesystem.Path = "/new-backups"
			}
			if err := register(); err != nil {
				t.Fatal(err)
			}
			changed := version()
			if changed == initial {
				t.Fatal("changed registration did not update record")
			}
			if err := register(); err != nil {
				t.Fatal(err)
			}
			if version() != changed {
				t.Fatal("registration did not converge after change")
			}
		})
	}
}
