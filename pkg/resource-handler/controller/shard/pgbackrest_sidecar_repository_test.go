package shard

import (
	"context"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

type keepOpenTopo struct{ topoclient.Store }

func (keepOpenTopo) Close() error { return nil }

func sidecarKeyPrefixFlags(c corev1.Container) []string {
	var flags []string
	for _, arg := range c.Args {
		if strings.HasPrefix(arg, "--s3-key-prefix") {
			flags = append(flags, arg)
		}
	}
	return flags
}

// Multipooler reads the repository from the database record in topology and
// refuses a sidecar started with another one. In Lite the shard controller
// writes that record, so the record and the sidecar's flags are compared here
// for each form a key prefix takes.
func TestLiteSidecarKeyPrefixMatchesTopology(t *testing.T) {
	for name, tc := range map[string]struct {
		keyPrefix string
		wantFlags []string
		wantTopo  string
	}{
		"allocated prefix": {
			keyPrefix: "clusters/uid-a",
			wantFlags: []string{"--s3-key-prefix=clusters/uid-a"},
			wantTopo:  "clusters/uid-a",
		},
		"explicit prefix with a trailing slash": {
			keyPrefix: "prod/cluster-1/",
			wantFlags: []string{"--s3-key-prefix=prod/cluster-1/"},
			wantTopo:  "prod/cluster-1/",
		},
		// pgBackRest is given the bucket root as no prefix at all.
		"legacy bucket root": {keyPrefix: "/"},
		"no prefix":          {},
	} {
		t.Run(name, func(t *testing.T) {
			shard := liteSidecarTestShard()
			shard.Spec.SidecarBackupLocation.S3.KeyPrefix = tc.keyPrefix
			shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": {Cells: []multigresv1alpha1.CellName{"z1"}},
			}

			_, factory := memorytopo.NewServerAndFactory(context.Background(), "z1")
			store := topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			)
			t.Cleanup(func() { _ = store.Close() })
			r := &ShardReconciler{
				Recorder: record.NewFakeRecorder(10),
				CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
					return keepOpenTopo{store}, nil
				},
			}
			registered, err := r.registerLiteDatabase(t.Context(), shard)
			require.NoError(t, err)
			require.True(t, registered)
			db, err := store.GetDatabase(t.Context(), string(shard.Spec.DatabaseName))
			require.NoError(t, err)
			s3 := db.GetBackupLocation().GetS3()
			require.NotNil(t, s3)
			assert.Equal(t, tc.wantTopo, s3.GetKeyPrefix())

			lite := sidecarTestContainer(
				t, buildLiteTestPod(t, shard), PgBackRestSidecarContainerName,
			)
			assert.Equal(t, tc.wantFlags, sidecarKeyPrefixFlags(lite))

			// A pool pod's sidecar is built by the same code and must render
			// the same prefix for the same location.
			pool := sidecarTestShard()
			pool.Spec.SidecarBackupLocation.S3.KeyPrefix = tc.keyPrefix
			assert.Equal(t, tc.wantFlags, sidecarKeyPrefixFlags(sidecarTestContainer(
				t, buildSidecarPod(t, pool), PgBackRestSidecarContainerName,
			)))
		})
	}
}

// The pod builders are the last place a shard's backup location is checked,
// so a refusal here is what keeps a pod from being built. Each shard is built
// by hand: the cluster and tablegroup builders do not produce the refused ones.
func TestPodBuildersRefuseUnusableBackupRepository(t *testing.T) {
	allocated := map[string]string{
		multigresv1alpha1.AnnotationDefaultBackupKeyPrefix: "clusters/uid-a",
	}
	for name, tc := range map[string]struct {
		annotations map[string]string
		keyPrefix   string
		// noSidecarLocation leaves spec.backup on S3 and the sidecar on the
		// filesystem.
		noSidecarLocation bool
		wantReason        string
		wantErr           string
	}{
		"allocated cluster, no prefix": {
			annotations: allocated,
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix,
			wantErr:     "no key prefix",
		},
		"allocated cluster, slashes only": {
			annotations: allocated, keyPrefix: "//",
			wantReason: multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix,
			wantErr:    "no key prefix",
		},
		"s3 backup, sidecar on the filesystem": {
			annotations: allocated, keyPrefix: "clusters/uid-a", noSidecarLocation: true,
			wantReason: multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback,
			wantErr:    "local filesystem",
		},
		"allocated cluster, explicit bucket root": {
			annotations: allocated, keyPrefix: "/",
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
		"allocated cluster, its prefix": {
			annotations: allocated, keyPrefix: "clusters/uid-a",
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
		"cluster without an allocation, no prefix": {
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, shard := range []*multigresv1alpha1.Shard{
				sidecarTestShard(), liteSidecarTestShard(),
			} {
				shard.Annotations = tc.annotations
				shard.Spec.Backup.S3.KeyPrefix = tc.keyPrefix
				shard.Spec.SidecarBackupLocation.S3.KeyPrefix = tc.keyPrefix
				if tc.noSidecarLocation {
					shard.Spec.SidecarBackupLocation = nil
				}

				reason, err := sidecarLocationProblem(shard)
				assert.Equal(t, tc.wantReason, reason)

				var buildErr error
				if shard.Spec.Mode == multigresv1alpha1.ModeLite {
					_, buildErr = BuildLitePod(shard, "main", "z1", newTestPoolSpec(), testScheme())
				} else {
					_, buildErr = BuildPoolPod(
						shard,
						"main",
						"z1",
						newTestPoolSpec(),
						0,
						testScheme(),
					)
				}
				if tc.wantErr == "" {
					require.NoError(t, err)
					require.NoError(t, buildErr)
					continue
				}
				require.ErrorContains(t, err, tc.wantErr)
				require.ErrorContains(t, buildErr, tc.wantErr, "no pod may be built")
			}
		})
	}
}
