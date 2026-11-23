package suite

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	topodata "github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

// s3Location is the S3 repository a topology database record names, or the
// zero value for a filesystem record.
type s3Location struct{ Bucket, Region, Endpoint, KeyPrefix string }

func recordS3(db *clustermetadatapb.Database) s3Location {
	s3 := db.GetBackupLocation().GetS3()
	if s3 == nil {
		return s3Location{}
	}
	return s3Location{s3.GetBucket(), s3.GetRegion(), s3.GetEndpoint(), s3.GetKeyPrefix()}
}

// registeredS3 is what RegisterDatabaseFromSpec records for a backup config,
// read back from a topology store of its own.
func (c *C) registeredS3(backup *api.BackupConfig) s3Location {
	c.Helper()
	store, _ := memorytopo.NewServerAndFactory(c.Context(), defaultSimCell)
	defer func() { _ = store.Close() }()
	c.NoError(topodata.RegisterDatabaseFromSpec(
		c.Context(), store, record.NewFakeRecorder(10), &Shard{},
		DatabaseConfig{Name: "postgres"}, []string{defaultSimCell}, backup, "",
	))
	db, err := store.GetDatabase(c.Context(), "postgres")
	c.NoError(err)
	return recordS3(db)
}

// TestBackupRepositoryAtEveryLevel puts the cluster's only S3 block at each
// level of the spec in turn, for a distributed and a Lite cluster, and lets
// the controllers run.
//
// The repository each row must end up on is the one the operator resolved
// before the pgBackRest sidecar existed: for a distributed cluster the record
// RegisterDatabaseFromSpec writes from the database-level merge, for a Lite
// cluster the one it writes from the shard's resolved spec.backup. Where that
// is S3, the sidecar's flags and the live topology record must both be it.
// Where it is the local filesystem although S3 is configured (a tablegroup or
// shard block in a distributed cluster), the cluster must report
// BackupRepositoryValid=False and the Degraded phase, and get no database
// record in topology, no TableGroup and no pod. Those rows are on an external
// global topology, whose store the test can read without a Shard to name it.
func TestBackupRepositoryAtEveryLevel(t *testing.T) {
	for _, mode := range []string{"", api.ModeLite} {
		for _, level := range []string{"cluster", "database", "tablegroup", "shard"} {
			name := "ha-" + level
			if mode == api.ModeLite {
				name = "lite-" + level
			}
			t.Run(name, func(t *testing.T) {
				c := newCase(t)
				block := func(at string) *api.BackupConfig {
					if at != level {
						return nil
					}
					return &api.BackupConfig{Type: api.BackupTypeS3, S3: &api.S3BackupConfig{
						Bucket: level + "-bucket", Region: "us-east-1",
						CredentialsSecret: "s3-credentials",
					}}
				}
				refused := mode != api.ModeLite && (level == "tablegroup" || level == "shard")
				cluster := c.newCluster(name, func(spec *MultigresClusterSpec) {
					if refused {
						spec.GlobalTopoServer = externalTopoSpec(c.externalTopoFor())
					}
					spec.Mode = mode
					spec.Backup = block("cluster")
					spec.Databases = []DatabaseConfig{{
						Name:    "postgres",
						Default: true,
						Backup:  block("database"),
						TableGroups: []TableGroupConfig{{
							Name:    "default",
							Default: true,
							Backup:  block("tablegroup"),
							Shards:  []ShardConfig{{Name: "0-inf", Backup: block("shard")}},
						}},
					}}
				})
				key := client.ObjectKeyFromObject(cluster)
				wantPrefix := "clusters/" + string(cluster.UID)

				if refused {
					// The database-level merge has no backup block, which
					// RegisterDatabaseFromSpec records as a filesystem.
					c.Eq(s3Location{}, c.registeredS3(api.MergeBackupConfig(
						cluster.Spec.Databases[0].Backup, cluster.Spec.Backup)))
					c.Eventually(
						30*time.Second,
						"the cluster to refuse the repository",
						func() error {
							var got MultigresCluster
							if err := c.Get(key, &got); err != nil {
								return err
							}
							cond := meta.FindStatusCondition(
								got.Status.Conditions, api.ConditionBackupRepositoryValid)
							if cond == nil || cond.Status != metav1.ConditionFalse ||
								cond.Reason != api.ReasonBackupRepositoryFilesystemFallback {
								return fmt.Errorf("BackupRepositoryValid is %+v", cond)
							}
							if got.Status.Phase != api.PhaseDegraded {
								return fmt.Errorf("phase is %q", got.Status.Phase)
							}
							return nil
						},
					)
					if db, err := topo.Store(c.externalTopoFor()).
						GetDatabase(c.Context(), "postgres"); err == nil {
						c.Fatalf("a refused cluster registered its database in topology: %v", db)
					}
					var groups TableGroupList
					c.NoError(c.List(&groups))
					c.Eq(0, len(groups.Items))
					var shards ShardList
					c.NoError(c.List(&shards))
					c.Eq(0, len(shards.Items))
					var pods corev1.PodList
					c.NoError(c.List(&pods))
					for _, pod := range pods.Items {
						for _, container := range pod.Spec.InitContainers {
							c.True(container.Name != "pgbackrest",
								"pod "+pod.Name+" was built with a pgBackRest sidecar")
						}
					}
					return
				}

				c.Eventually(
					30*time.Second,
					"a pod's sidecar and topology to name the repository",
					func() error {
						var stored MultigresCluster
						if err := c.Get(key, &stored); err != nil {
							return err
						}
						var shards ShardList
						if err := c.List(&shards); err != nil {
							return err
						}
						if len(shards.Items) != 1 {
							return fmt.Errorf("%d shards", len(shards.Items))
						}
						shard := &shards.Items[0]

						want := c.registeredS3(api.MergeBackupConfig(
							stored.Spec.Databases[0].Backup, stored.Spec.Backup))
						if mode == api.ModeLite {
							want = c.registeredS3(shard.Spec.Backup)
						}
						if want != (s3Location{
							Bucket: level + "-bucket", Region: "us-east-1", KeyPrefix: wantPrefix,
						}) {
							return fmt.Errorf("the earlier resolution gives %+v", want)
						}

						ref := shard.Spec.GlobalTopoServer
						db, err := topo.Store(topoKey{address: ref.Address, root: ref.RootPath}).
							GetDatabase(c.Context(), "postgres")
						if err != nil {
							return fmt.Errorf("database record: %w", err)
						}
						if got := recordS3(db); got != want {
							return fmt.Errorf("topology records %+v, want %+v", got, want)
						}

						var pods corev1.PodList
						if err := c.List(&pods); err != nil {
							return err
						}
						for _, pod := range pods.Items {
							if !metav1.IsControlledBy(&pod, shard) {
								continue
							}
							for _, container := range pod.Spec.InitContainers {
								if container.Name != "pgbackrest" {
									continue
								}
								for _, flag := range []string{
									"--location-type=s3",
									"--s3-bucket=" + want.Bucket,
									"--s3-region=" + want.Region,
									"--s3-key-prefix=" + want.KeyPrefix,
								} {
									if !slices.Contains(container.Args, flag) {
										return fmt.Errorf("pod %s sidecar args %v lack %s",
											pod.Name, container.Args, flag)
									}
								}
								return nil
							}
						}
						return fmt.Errorf("no pod of the shard has a pgBackRest sidecar yet")
					},
				)
			})
		}
	}
}
