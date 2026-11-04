package multigrescluster

import (
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
)

// repository is a backup location as one component is told it: the fields
// multipooler compares between the topology record and the sidecar it finds
// in its pod.
type repository struct {
	Type, Bucket, Region, Endpoint, KeyPrefix, FilesystemPath string
}

// provisioned is a cluster taken through the path a reconcile takes: prefix
// allocation with the finalizer, defaulting, topology registration, and the
// TableGroup, Shard and pod builders, for its first database, tablegroup and
// shard.
type provisioned struct {
	reconciler *MultigresClusterReconciler
	// cluster is the defaulted cluster the builders were given.
	cluster *multigresv1alpha1.MultigresCluster
	shard   *multigresv1alpha1.Shard
	// sidecar is the repository the pod's pgBackRest sidecar is started with.
	// podErr is set instead when the pod builder refuses the shard.
	sidecar repository
	podErr  error
	// pod is the first pool's pod, nil when the builder refused it.
	pod *corev1.Pod
	// topology is the database record reconcileTopology wrote. A Lite
	// cluster's record is written by its shard, so it is empty for Lite.
	topology *clustermetadatapb.Database
}

func recordedRepository(t *testing.T, db *clustermetadatapb.Database) repository {
	t.Helper()
	require.NotNil(t, db)
	if s3 := db.GetBackupLocation().GetS3(); s3 != nil {
		return repository{
			Type:      "s3",
			Bucket:    s3.GetBucket(),
			Region:    s3.GetRegion(),
			Endpoint:  s3.GetEndpoint(),
			KeyPrefix: s3.GetKeyPrefix(),
		}
	}
	return repository{
		Type:           "filesystem",
		FilesystemPath: db.GetBackupLocation().GetFilesystem().GetPath(),
	}
}

// registeredRepository is the repository RegisterDatabaseFromSpec records for
// a backup config, read back from a topology store of its own.
func registeredRepository(t *testing.T, backup *multigresv1alpha1.BackupConfig) repository {
	t.Helper()
	store := newClusterTopologyMemoryStore(t)
	require.NoError(t, topo.RegisterDatabaseFromSpec(
		t.Context(), store, record.NewFakeRecorder(10), &multigresv1alpha1.Shard{},
		multigresv1alpha1.DatabaseConfig{Name: "postgres"}, []string{"cell1"}, backup, "",
	))
	db, err := store.GetDatabase(t.Context(), "postgres")
	require.NoError(t, err)
	return recordedRepository(t, db)
}

func provision(t *testing.T, cluster *multigresv1alpha1.MultigresCluster) provisioned {
	t.Helper()
	ctx := t.Context()
	cluster.Spec.Images.PgBackRestd = "registry/multigres@sha256:abc"
	cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
		},
	}
	cluster.Spec.Cells = []multigresv1alpha1.CellConfig{{Name: "cell1"}}

	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	store := newClusterTopologyMemoryStore(t)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return noCloseStore{store}, nil
		},
	}

	require.NoError(t, r.ensureClusterFinalizer(ctx, cluster))
	// The rest of the reconcile works on the stored object, so a prefix that
	// was allocated but not persisted would be lost here.
	require.NoError(t, c.Get(
		ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, cluster,
	))
	res := resolver.NewResolver(c, cluster.Namespace)
	_, err := res.PopulateClusterDefaults(ctx, cluster)
	require.NoError(t, err)
	p := provisioned{reconciler: r, cluster: cluster}

	_, err = r.reconcileTopology(ctx, cluster, res)
	require.NoError(t, err)
	db := cluster.Spec.Databases[0]
	if cluster.Spec.Mode != multigresv1alpha1.ModeLite {
		p.topology, err = store.GetDatabase(ctx, string(db.Name))
		require.NoError(t, err)
	}

	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	require.NoError(t, err)
	tg, shards, err := r.resolveTableGroup(
		ctx, cluster, res, db, &db.TableGroups[0], globalTopoRef, nil,
	)
	require.NoError(t, err)
	p.shard, err = tablegroup.BuildShard(tg, &shards[0], scheme)
	require.NoError(t, err)
	require.NotEmpty(t, p.shard.Spec.Pools)
	for poolName, pool := range p.shard.Spec.Pools {
		var pod *corev1.Pod
		if cluster.Spec.Mode == multigresv1alpha1.ModeLite {
			pod, p.podErr = shard.BuildLitePod(
				p.shard, string(poolName), string(pool.Cells[0]), pool, scheme,
			)
		} else {
			pod, p.podErr = shard.BuildPoolPod(
				p.shard, string(poolName), string(pool.Cells[0]), pool, 0, scheme,
			)
		}
		if p.podErr != nil {
			return p
		}
		p.pod = pod
		var found bool
		for _, container := range pod.Spec.InitContainers {
			if container.Name != shard.PgBackRestSidecarContainerName {
				continue
			}
			found = true
			flags := map[string]string{}
			for _, arg := range container.Args {
				name, value, _ := strings.Cut(arg, "=")
				flags[name] = value
			}
			p.sidecar = repository{
				Type:           flags["--location-type"],
				Bucket:         flags["--s3-bucket"],
				Region:         flags["--s3-region"],
				Endpoint:       flags["--s3-endpoint"],
				KeyPrefix:      flags["--s3-key-prefix"],
				FilesystemPath: flags["--fs-repo-path"],
			}
		}
		require.True(t, found, "pod has no pgBackRest sidecar")
	}
	return p
}

// provisionedRepositories returns the repository of a distributed cluster's
// first database as the pgBackRest sidecar is started with it and as topology
// records it, which is where multipooler reads it.
func provisionedRepositories(
	t *testing.T,
	cluster *multigresv1alpha1.MultigresCluster,
) (sidecar, topology repository) {
	t.Helper()
	p := provision(t, cluster)
	require.NoError(t, p.podErr)
	return p.sidecar, recordedRepository(t, p.topology)
}

func sidecarRepositoryCluster(
	uid types.UID,
	mutate ...func(*multigresv1alpha1.MultigresCluster),
) *multigresv1alpha1.MultigresCluster {
	cluster := backupPrefixCluster("")
	cluster.UID = uid
	cluster.Spec.Backup.S3.CredentialsSecret = "s3-creds"
	for _, m := range mutate {
		m(cluster)
	}
	return cluster
}

// Two clusters that share a bucket and name no prefix would otherwise be given
// one repository: each would restore the other's backups and archive WAL into
// the other's stanza. The sidecar does the archiving, so it is the sidecar's
// repository that has to differ, and it has to be the one multipooler expects.
func TestSidecarRepositoryIsPerCluster(t *testing.T) {
	sidecarA, topologyA := provisionedRepositories(t, sidecarRepositoryCluster("uid-a"))
	sidecarB, topologyB := provisionedRepositories(t, sidecarRepositoryCluster("uid-b"))

	assert.Equal(t, "clusters/uid-a", sidecarA.KeyPrefix)
	assert.Equal(t, "clusters/uid-b", sidecarB.KeyPrefix)
	assert.Equal(t, sidecarA.Bucket, sidecarB.Bucket, "the clusters share a bucket")
	assert.NotEqual(t, sidecarA, sidecarB)
	assert.Equal(t, topologyA, sidecarA)
	assert.Equal(t, topologyB, sidecarB)
}

func TestSidecarRepositoryMatchesTopology(t *testing.T) {
	withFinalizer := func(c *multigresv1alpha1.MultigresCluster) {
		c.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
	}
	for name, tc := range map[string]struct {
		mutate     []func(*multigresv1alpha1.MultigresCluster)
		wantPrefix string
		wantBucket string
	}{
		"an explicit prefix is used as written": {
			mutate: []func(*multigresv1alpha1.MultigresCluster){
				func(c *multigresv1alpha1.MultigresCluster) {
					c.Spec.Backup.S3.KeyPrefix = "prod/cluster-1"
				},
			},
			wantPrefix: "prod/cluster-1",
		},
		// "/" asks for the bucket root, which pgBackRest is given as no
		// prefix at all. It is the only way a new cluster reaches the root.
		"an explicit / selects the legacy bucket root": {
			mutate: []func(*multigresv1alpha1.MultigresCluster){
				func(c *multigresv1alpha1.MultigresCluster) {
					c.Spec.Backup.S3.KeyPrefix = "/"
				},
			},
			wantPrefix: "",
		},
		"slashes alone do not select the bucket root": {
			mutate: []func(*multigresv1alpha1.MultigresCluster){
				func(c *multigresv1alpha1.MultigresCluster) {
					c.Spec.Backup.S3.KeyPrefix = "//"
				},
			},
			wantPrefix: "clusters/uid-a",
		},
		// A cluster provisioned before prefixes were allocated has its
		// repository at the root and must keep it.
		"a cluster provisioned without an allocation keeps the root": {
			mutate:     []func(*multigresv1alpha1.MultigresCluster){withFinalizer},
			wantPrefix: "",
		},
		// Without admission the stored spec of an initialized cluster can
		// lack the prefix, for example when S3 is configured after the first
		// reconcile. The allocation is then only in the annotation.
		"an allocation recorded only in the annotation is used": {
			mutate: []func(*multigresv1alpha1.MultigresCluster){
				withFinalizer,
				func(c *multigresv1alpha1.MultigresCluster) {
					c.Annotations = map[string]string{
						multigresv1alpha1.AnnotationDefaultBackupKeyPrefix: "clusters/uid-first",
					}
				},
			},
			wantPrefix: "clusters/uid-first",
		},
		"a database that overrides the bucket still gets the allocation": {
			mutate: []func(*multigresv1alpha1.MultigresCluster){
				func(c *multigresv1alpha1.MultigresCluster) {
					c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
						Name:    "postgres",
						Default: true,
						Backup: &multigresv1alpha1.BackupConfig{
							Type: multigresv1alpha1.BackupTypeS3,
							S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "database-bucket"},
						},
					}}
				},
			},
			wantPrefix: "clusters/uid-a",
			wantBucket: "database-bucket",
		},
	} {
		t.Run(name, func(t *testing.T) {
			sidecar, topology := provisionedRepositories(
				t, sidecarRepositoryCluster("uid-a", tc.mutate...),
			)
			wantBucket := tc.wantBucket
			if wantBucket == "" {
				wantBucket = "shared"
			}
			assert.Equal(t, repository{
				Type:      "s3",
				Bucket:    wantBucket,
				Region:    "us-east-1",
				KeyPrefix: tc.wantPrefix,
			}, sidecar)
			assert.Equal(t, topology, sidecar)
		})
	}
}

// s3OnlyAt is a new cluster whose only backup block is an S3 one at the named
// level, in its own bucket.
func s3OnlyAt(mode, level string) *multigresv1alpha1.MultigresCluster {
	block := func(at string) *multigresv1alpha1.BackupConfig {
		if at != level {
			return nil
		}
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: level + "-bucket", Region: "us-east-1", CredentialsSecret: "s3-creds",
			},
		}
	}
	cluster := backupPrefixCluster(mode)
	cluster.Spec.Backup = block("cluster")
	cluster.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
		Name:    "postgres",
		Default: true,
		Backup:  block("database"),
		TableGroups: []multigresv1alpha1.TableGroupConfig{{
			Name:    "default",
			Default: true,
			Backup:  block("tablegroup"),
			Shards: []multigresv1alpha1.ShardConfig{{
				Name:   "0-inf",
				Backup: block("shard"),
			}},
		}},
	}}
	return cluster
}

// The expectation for every row comes from the resolution the operator used
// before the sidecar existed, not from a value written here. For a distributed
// cluster that is the database record reconcileTopology writes from the
// database-level merge. For a Lite cluster it is the record
// RegisterDatabaseFromSpec writes from the shard's resolved spec.backup, which
// is what the Lite shard registered.
//
// Where that resolution lands on S3, the sidecar and the topology record must
// be that repository. Where it lands on the local filesystem although S3 is
// configured (a tablegroup or shard block in a distributed cluster), the
// cluster and the shard must be refused and no pod built.
func TestSidecarRepositoryFollowsEveryBackupLevel(t *testing.T) {
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		for _, level := range []string{"cluster", "database", "tablegroup", "shard"} {
			name := "distributed/" + level
			if mode == multigresv1alpha1.ModeLite {
				name = "lite/" + level
			}
			t.Run(name, func(t *testing.T) {
				p := provision(t, s3OnlyAt(mode, level))

				var oracle repository
				if mode == multigresv1alpha1.ModeLite {
					oracle = registeredRepository(t, p.shard.Spec.Backup)
				} else {
					oracle = recordedRepository(t, p.topology)
				}

				clusterErr := p.reconciler.syncBackupRepositoryCondition(p.cluster)
				cond := meta.FindStatusCondition(
					p.cluster.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
				)
				require.NotNil(t, cond)

				if oracle.Type != "s3" {
					require.ErrorIs(t, clusterErr, resolver.ErrBackupRepository)
					assert.Equal(t, metav1.ConditionFalse, cond.Status)
					assert.Equal(t,
						multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback, cond.Reason)
					require.ErrorIs(t,
						resolver.ValidateSidecarConfig(p.cluster), resolver.ErrBackupRepository,
						"admission must refuse it too")
					require.ErrorContains(t, p.podErr, "local filesystem",
						"no pod may be built with a filesystem location")
					return
				}

				require.NoError(t, clusterErr)
				assert.Equal(t, metav1.ConditionTrue, cond.Status)
				require.NoError(t, resolver.ValidateSidecarConfig(p.cluster))
				require.NoError(t, p.podErr)
				assert.Equal(t, oracle, p.sidecar, "sidecar")
				// What the operator writes to topology for this shard: the
				// record itself for a distributed cluster, and for Lite the
				// location the shard registers.
				assert.Equal(t, oracle,
					registeredRepository(t, p.shard.Spec.SidecarBackupLocation), "topology")
				assert.Equal(t, level+"-bucket", p.sidecar.Bucket)
				assert.Equal(t, "clusters/uid-a", p.sidecar.KeyPrefix)
			})
		}
	}
}

// A cluster with no backup configured anywhere keeps the filesystem
// repository: only S3 configured somewhere makes a filesystem location a
// refusal.
func TestSidecarRepositoryFilesystemWithoutS3(t *testing.T) {
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		cluster := backupPrefixCluster(mode)
		cluster.Spec.Backup = nil
		p := provision(t, cluster)
		require.NoError(t, p.reconciler.syncBackupRepositoryCondition(p.cluster))
		require.NoError(t, p.podErr)
		assert.Equal(t, repository{Type: "filesystem", FilesystemPath: "/backups"}, p.sidecar)
	}
}

// Below an S3 block, a block that selects the filesystem takes the shard's
// backups off S3. In Lite that block decides the repository, so the cluster is
// refused. The Shard built from it carries no trace of the S3 block, so the
// refusal is the cluster's: it applies no TableGroup, and so no Shard, while
// refused.
func TestSidecarRepositoryRefusesFilesystemBelowS3(t *testing.T) {
	cluster := s3OnlyAt(multigresv1alpha1.ModeLite, "cluster")
	cluster.Spec.Databases[0].TableGroups[0].Shards[0].Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
	}
	p := provision(t, cluster)
	require.ErrorIs(t,
		p.reconciler.syncBackupRepositoryCondition(p.cluster), resolver.ErrBackupRepository)
	require.ErrorIs(t, resolver.ValidateSidecarConfig(p.cluster), resolver.ErrBackupRepository)
}

// sidecarSecrets names the cipher and TLS Secrets a pod's pgBackRest sidecar
// is given, empty where the pod has no such volume or the certificate is the
// generated one.
func sidecarSecrets(pod *corev1.Pod) (cipher, tls string) {
	for _, v := range pod.Spec.Volumes {
		switch {
		case v.Name == shard.PgBackRestCipherKeyVolumeName && v.Secret != nil:
			cipher = v.Secret.SecretName
		case v.Name == shard.PgBackRestCertVolumeName && v.Projected != nil:
			for _, src := range v.Projected.Sources {
				if src.Secret != nil {
					tls = src.Secret.Name
				}
			}
		}
	}
	return cipher, tls
}

// encryptedDatabaseLocation is a cluster whose database block is an S3
// location with an endpoint, a cipher and a TLS secret, and whose tablegroup
// and shard blocks are the given ones.
func encryptedDatabaseLocation(
	mode string,
	tablegroup, shardBlock *multigresv1alpha1.BackupConfig,
) *multigresv1alpha1.MultigresCluster {
	cluster := backupPrefixCluster(mode)
	cluster.Spec.Backup = nil
	cluster.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
		Name:    "postgres",
		Default: true,
		Backup:  encryptedLocation(),
		TableGroups: []multigresv1alpha1.TableGroupConfig{{
			Name:    "default",
			Default: true,
			Backup:  tablegroup,
			Shards:  []multigresv1alpha1.ShardConfig{{Name: "0-inf", Backup: shardBlock}},
		}},
	}}
	return cluster
}

func encryptedLocation() *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "db-bucket", Region: "us-east-1", Endpoint: "https://s3.example",
			CredentialsSecret: "s3-creds",
		},
		Encryption:    &multigresv1alpha1.BackupEncryptionConfig{SecretName: "db-cipher"},
		PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "db-tls"},
	}
}

// The reference for a shard's cipher, certificate and bucket is the shard's
// resolved spec.backup. It is the merge of every level, so it is what the
// tablegroup and shard blocks ask for, and it is computed by the resolver
// independently of the sidecar's location.
//
// A distributed cluster's sidecar is configured from the database-level
// location instead. Where the two agree, which includes a lower block that
// repeats the database's, the pod must be built with the reference's cipher
// and certificate, in the repository topology records. Where a lower block
// makes them differ, in any one of the five fields at either level, the
// cluster, admission and the pod builder must all refuse: a sidecar built
// from the database-level location would silently drop what the block asks
// for. A Lite cluster's sidecar location is the shard's resolved config, so
// the same blocks are applied there, not refused.
func TestSidecarFollowsOrRefusesBlocksBelowTheDatabase(t *testing.T) {
	type block = multigresv1alpha1.BackupConfig
	s3Block := func(mut func(*multigresv1alpha1.S3BackupConfig)) *block {
		cfg := &multigresv1alpha1.S3BackupConfig{}
		mut(cfg)
		return &block{Type: multigresv1alpha1.BackupTypeS3, S3: cfg}
	}
	for name, tc := range map[string]struct {
		block *block
		// wantField is the setting the refusal names, empty when the block
		// is accepted.
		wantField string
	}{
		"repeats the database block": {block: encryptedLocation()},
		"sets only the type":         {block: &block{Type: multigresv1alpha1.BackupTypeS3}},
		"another cipher": {
			block: &block{
				Type:       multigresv1alpha1.BackupTypeS3,
				Encryption: &multigresv1alpha1.BackupEncryptionConfig{SecretName: "own-cipher"},
			},
			wantField: "encryption.secretName",
		},
		"another TLS secret": {
			block: &block{
				Type:          multigresv1alpha1.BackupTypeS3,
				PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "own-tls"},
			},
			wantField: "pgbackrestTLS.secretName",
		},
		"another bucket": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Bucket = "own-bucket"
			}),
			wantField: "s3.bucket",
		},
		"another endpoint": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Endpoint = "https://other.example"
			}),
			wantField: "s3.endpoint",
		},
		"another region": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Region = "eu-west-1"
			}),
			wantField: "s3.region",
		},
	} {
		for _, level := range []string{"tablegroup", "shard"} {
			build := func(mode string) *multigresv1alpha1.MultigresCluster {
				if level == "tablegroup" {
					return encryptedDatabaseLocation(mode, tc.block.DeepCopy(), nil)
				}
				return encryptedDatabaseLocation(mode, nil, tc.block.DeepCopy())
			}
			// reference reads the five fields from a shard's resolved
			// spec.backup.
			reference := func(p provisioned) (repo repository, cipher, tls string) {
				b := p.shard.Spec.Backup
				repo = registeredRepository(t, b)
				if b.Encryption != nil {
					cipher = b.Encryption.SecretName
				}
				if b.PgBackRestTLS != nil {
					tls = b.PgBackRestTLS.SecretName
				}
				return repo, cipher, tls
			}

			t.Run("distributed/"+level+"/"+name, func(t *testing.T) {
				p := provision(t, build(""))
				wantRepo, wantCipher, wantTLS := reference(p)
				clusterErr := p.reconciler.syncBackupRepositoryCondition(p.cluster)
				cond := meta.FindStatusCondition(
					p.cluster.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
				)
				require.NotNil(t, cond)

				if tc.wantField != "" {
					location := p.shard.Spec.SidecarBackupLocation
					require.NotNil(t, multigresv1alpha1.UnappliedBackupOverride(
						p.shard.Spec.Backup, location,
					), "the reference and the sidecar's location must differ for this row")
					require.ErrorIs(t, clusterErr, resolver.ErrBackupRepository)
					assert.Equal(t, metav1.ConditionFalse, cond.Status)
					assert.Equal(t,
						multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied, cond.Reason)
					assert.Contains(t, cond.Message, tc.wantField)
					assert.Contains(t, cond.Message, level)
					require.ErrorIs(t,
						resolver.ValidateSidecarConfig(p.cluster), resolver.ErrBackupRepository,
						"admission must refuse it too")
					require.ErrorContains(t, p.podErr, tc.wantField,
						"no pod may be built that drops the block's setting")
					return
				}

				require.NoError(t, clusterErr)
				assert.Equal(t, metav1.ConditionTrue, cond.Status)
				require.NoError(t, resolver.ValidateSidecarConfig(p.cluster))
				require.NoError(t, p.podErr)
				assert.Equal(t, wantRepo, p.sidecar, "sidecar repository")
				assert.Equal(t, recordedRepository(t, p.topology), p.sidecar, "topology")
				cipher, tls := sidecarSecrets(p.pod)
				assert.Equal(t, wantCipher, cipher)
				assert.Equal(t, wantTLS, tls)
				assert.Equal(t, "db-cipher", cipher)
				assert.Equal(t, "db-tls", tls)
				assert.Equal(t, "db-bucket", p.sidecar.Bucket)
			})

			t.Run("lite/"+level+"/"+name, func(t *testing.T) {
				p := provision(t, build(multigresv1alpha1.ModeLite))
				wantRepo, wantCipher, wantTLS := reference(p)
				require.NoError(t, p.reconciler.syncBackupRepositoryCondition(p.cluster))
				require.NoError(t, resolver.ValidateSidecarConfig(p.cluster))
				require.NoError(t, p.podErr)
				assert.Equal(t, wantRepo, p.sidecar, "sidecar repository")
				cipher, tls := sidecarSecrets(p.pod)
				assert.Equal(t, wantCipher, cipher)
				assert.Equal(t, wantTLS, tls)
			})
		}
	}
}
