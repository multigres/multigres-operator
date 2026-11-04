//go:build integration
// +build integration

package shard

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// The cluster and tablegroup builders cannot produce the refused Shards below:
// they fill the key prefix of a cluster that was allocated one, they do not
// leave the sidecar on the filesystem under an S3 backup, and the cluster
// controller builds nothing from a cluster whose tablegroup or shard block
// sets a cipher or bucket the database's location lacks. A Shard like these
// comes from an edit to the Shard itself or from a spec written by something
// else, so they are built by hand here. The shard controller must refuse them:
// report BackupRepositoryValid=False, emit one event, create no pod, and
// return no error, since only a change to the Shard clears the refusal.
func TestShardRefusesUnusableBackupRepository(t *testing.T) {
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: "default"},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}))

	s3 := func(keyPrefix string) *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "shared", Region: "us-east-1", KeyPrefix: keyPrefix,
				CredentialsSecret: "s3-credentials",
			},
		}
	}
	allocated := map[string]string{
		multigresv1alpha1.AnnotationDefaultBackupKeyPrefix: "clusters/uid-a",
	}

	for name, tc := range map[string]struct {
		annotations map[string]string
		backup      *multigresv1alpha1.BackupConfig
		sidecar     *multigresv1alpha1.BackupConfig
		wantReason  string
		wantMessage string
		// noImage leaves the Shard without a sidecar image, as an operator
		// that does not build the sidecar stored it.
		noImage bool
	}{
		"no sidecar image": {
			backup:      s3("clusters/uid-a"),
			sidecar:     s3("clusters/uid-a"),
			noImage:     true,
			wantReason:  multigresv1alpha1.ReasonBackupRepositorySidecarNotConfigured,
			wantMessage: "no pgbackrestd image",
		},
		// Encryption cannot be the example here: the Shard CRD makes it
		// immutable, and these Shards are edited after creation.
		"a TLS secret the sidecar's location lacks": {
			annotations: allocated,
			backup:      withTLSSecret(s3("clusters/uid-a"), "shard-tls"),
			sidecar:     s3("clusters/uid-a"),
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantMessage: "pgbackrestTLS.secretName",
		},
		"a bucket other than the sidecar's": {
			annotations: allocated,
			backup:      inBucket(s3("clusters/uid-a"), "shard-bucket"),
			sidecar:     s3("clusters/uid-a"),
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantMessage: "s3.bucket",
		},
		"s3 without a key prefix on an allocated cluster": {
			annotations: allocated,
			backup:      s3(""),
			sidecar:     s3(""),
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix,
			wantMessage: "no key prefix",
		},
		"s3 with only slashes on an allocated cluster": {
			annotations: allocated,
			backup:      s3("//"),
			sidecar:     s3("//"),
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix,
			wantMessage: "no key prefix",
		},
		"s3 backup with the sidecar on the filesystem": {
			annotations: allocated,
			backup:      s3("clusters/uid-a"),
			sidecar:     nil,
			wantReason:  multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback,
			wantMessage: "local filesystem",
		},
		// The control: "/" asks for the bucket root, and a cluster with no
		// allocation keeps the root it has always used.
		"explicit bucket root": {
			annotations: allocated,
			backup:      s3("/"),
			sidecar:     s3("/"),
		},
		"no allocation": {
			backup:  s3(""),
			sidecar: s3(""),
		},
	} {
		for _, lite := range []bool{false, true} {
			mode := "pool"
			if lite {
				mode = "lite"
			}
			t.Run(name+"/"+mode, func(t *testing.T) {
				shardName := "repo-" + mode + "-" + sanitize(name)
				obj := storedByPreviousOperator(shardName, lite)
				require.NoError(t, c.Create(ctx, obj))
				shard := &multigresv1alpha1.Shard{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), shard))
				shard.Annotations = tc.annotations
				if !tc.noImage {
					shard.Spec.Images.PgBackRestd = "ghcr.io/multigres/multigres:new"
				}
				shard.Spec.Backup = tc.backup
				shard.Spec.SidecarBackupLocation = tc.sidecar
				require.NoError(t, c.Update(ctx, shard))

				recorder := record.NewFakeRecorder(100)
				r := &ShardReconciler{
					Client: c, APIReader: c, Scheme: scheme, Recorder: recorder,
					CreateTopoStore: newMemoryTopoFactory(),
				}
				res, err := r.Reconcile(ctx, requestOf(shard))

				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
				cond := meta.FindStatusCondition(
					shard.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
				)
				require.NotNil(t, cond)
				pods := &corev1.PodList{}
				require.NoError(t, c.List(ctx, pods, client.InNamespace("default"),
					client.MatchingLabels{metadata.LabelMultigresShard: "0-inf"}))
				var built []string
				for _, pod := range pods.Items {
					if metav1.IsControlledBy(&pod, shard) {
						built = append(built, pod.Name)
					}
				}

				if tc.wantReason == "" {
					assert.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
					if err != nil {
						assert.NotContains(t, err.Error(), "key prefix")
						assert.NotContains(t, err.Error(), "local filesystem")
					}
					return
				}
				require.NoError(t, err)
				assert.Zero(t, res)
				_, err = r.Reconcile(ctx, requestOf(shard))
				require.NoError(t, err)
				assert.Equal(t, metav1.ConditionFalse, cond.Status)
				assert.Equal(t, tc.wantReason, cond.Reason)
				assert.Contains(t, cond.Message, tc.wantMessage)
				assert.Empty(t, built, "a refused shard must get no pod")
				var events []string
				for len(recorder.Events) > 0 {
					events = append(events, <-recorder.Events)
				}
				var refusals int
				for _, e := range events {
					if containsAll(e, "BackupRepositoryInvalid", tc.wantMessage) {
						refusals++
					}
				}
				assert.Equal(t, 1, refusals,
					"want one BackupRepositoryInvalid event over two passes, got %v", events)
			})
		}
	}
}

func withTLSSecret(
	cfg *multigresv1alpha1.BackupConfig,
	secret string,
) *multigresv1alpha1.BackupConfig {
	cfg.PgBackRestTLS = &multigresv1alpha1.PgBackRestTLSConfig{SecretName: secret}
	return cfg
}

func inBucket(cfg *multigresv1alpha1.BackupConfig, bucket string) *multigresv1alpha1.BackupConfig {
	cfg.S3.Bucket = bucket
	return cfg
}

func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, strings.ToLower(name))
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
