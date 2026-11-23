package suite

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestBackupBlockBelowTheDatabase runs a distributed cluster whose tablegroup
// and shard backup blocks repeat its database block, encryption included, and
// then edits the shard block.
//
// Repeated blocks ask for nothing the database-level location lacks, so the
// cluster must run: its pods' sidecars get the database's bucket and cipher.
// Pointing the shard block at another bucket asks for a repository the
// sidecars cannot be given. The controllers run without admission here, so the
// cluster controller is what must refuse: BackupRepositoryValid=False, the
// Degraded phase, and the Shard and its running pods left exactly as they
// were. Putting the bucket back must clear the refusal without touching the
// pods either, since the spec they were built from never changed.
func TestBackupBlockBelowTheDatabase(t *testing.T) {
	c := newCase(t)
	c.NoError(c.Create(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-cipher", Namespace: c.NS},
		StringData: map[string]string{"keys.json": `{"1":"passphrase"}`},
	}))
	location := func() *api.BackupConfig {
		return &api.BackupConfig{
			Type: api.BackupTypeS3,
			S3: &api.S3BackupConfig{
				Bucket: "db-bucket", Region: "us-east-1", CredentialsSecret: "s3-credentials",
			},
			Encryption: &api.BackupEncryptionConfig{SecretName: "backup-cipher"},
		}
	}
	cluster := c.newCluster("override", func(spec *MultigresClusterSpec) {
		spec.Databases = []DatabaseConfig{{
			Name:    "postgres",
			Default: true,
			Backup:  location(),
			TableGroups: []TableGroupConfig{{
				Name:    "default",
				Default: true,
				Backup:  location(),
				Shards:  []ShardConfig{{Name: "0-inf", Backup: location()}},
			}},
		}}
	})
	key := client.ObjectKeyFromObject(cluster)

	repositoryCondition := func() (*metav1.Condition, api.Phase, error) {
		var got MultigresCluster
		if err := c.Get(key, &got); err != nil {
			return nil, "", err
		}
		return meta.FindStatusCondition(
			got.Status.Conditions, api.ConditionBackupRepositoryValid), got.Status.Phase, nil
	}
	// sidecarPods maps each pod that runs a pgBackRest sidecar to its UID,
	// and fails unless every one is given the database's bucket and cipher.
	sidecarPods := func() (map[string]types.UID, error) {
		var pods corev1.PodList
		if err := c.List(&pods); err != nil {
			return nil, err
		}
		out := map[string]types.UID{}
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.InitContainers {
				if container.Name != "pgbackrest" {
					continue
				}
				for _, flag := range []string{
					"--s3-bucket=db-bucket",
					"--cipher-key-file=/secrets/pgbackrest-cipher/keys.json",
				} {
					if !slices.Contains(container.Args, flag) {
						return nil, fmt.Errorf("pod %s sidecar args %v lack %s",
							pod.Name, container.Args, flag)
					}
				}
				if !pod.DeletionTimestamp.IsZero() {
					return nil, fmt.Errorf("pod %s is being deleted", pod.Name)
				}
				out[pod.Name] = pod.UID
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no pod has a pgBackRest sidecar yet")
		}
		return out, nil
	}
	shardBucket := func() string {
		c.Helper()
		var shards ShardList
		c.NoError(c.List(&shards))
		c.Eq(1, len(shards.Items))
		return shards.Items[0].Spec.Backup.S3.Bucket
	}

	// The pods are compared with their later selves, so the snapshot waits
	// until nothing is left to create: the cluster and its Shard have
	// observed the spec as it is, and every replica the Shard asks for
	// exists. A snapshot taken while the pool is still coming up would see
	// the next replica appear and take it for a pod the refusal touched.
	var before map[string]types.UID
	c.Eventually(60*time.Second, "every pod, with the database's bucket and cipher", func() error {
		var got MultigresCluster
		if err := c.Get(key, &got); err != nil {
			return err
		}
		cond := meta.FindStatusCondition(got.Status.Conditions, api.ConditionBackupRepositoryValid)
		if cond == nil || cond.Status != metav1.ConditionTrue {
			return fmt.Errorf("BackupRepositoryValid is %+v", cond)
		}
		if got.Status.Phase != api.PhaseHealthy || got.Status.ObservedGeneration != got.Generation {
			return fmt.Errorf("cluster is %q at generation %d of %d",
				got.Status.Phase, got.Status.ObservedGeneration, got.Generation)
		}
		var shards ShardList
		if err := c.List(&shards); err != nil {
			return err
		}
		if len(shards.Items) != 1 {
			return fmt.Errorf("%d shards, want 1", len(shards.Items))
		}
		shard := &shards.Items[0]
		if shard.Status.ObservedGeneration != shard.Generation {
			return fmt.Errorf("shard has observed generation %d of %d",
				shard.Status.ObservedGeneration, shard.Generation)
		}
		want := 0
		for _, pool := range shard.Spec.Pools {
			if pool.ReplicasPerCell == nil {
				return fmt.Errorf("a pool has no replica count yet")
			}
			want += int(*pool.ReplicasPerCell) * len(pool.Cells)
		}
		pods, err := sidecarPods()
		if err != nil {
			return err
		}
		if want == 0 || len(pods) != want {
			return fmt.Errorf("%d pods with a sidecar, want %d", len(pods), want)
		}
		before = pods
		return nil
	})

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Databases[0].TableGroups[0].Shards[0].Backup.S3.Bucket = "other-bucket"
	})
	c.Eventually(30*time.Second, "the cluster to refuse the shard block's bucket", func() error {
		cond, phase, err := repositoryCondition()
		if err != nil {
			return err
		}
		if cond == nil || cond.Status != metav1.ConditionFalse ||
			cond.Reason != api.ReasonBackupRepositoryOverrideNotApplied {
			return fmt.Errorf("BackupRepositoryValid is %+v", cond)
		}
		if phase != api.PhaseDegraded {
			return fmt.Errorf("phase is %q", phase)
		}
		return nil
	})
	c.Eq("db-bucket", shardBucket(), "the refused block must not reach the Shard")
	during, err := sidecarPods()
	c.NoError(err)
	c.True(maps.Equal(before, during),
		fmt.Sprintf("a refusal must leave the running pods alone: %v became %v", before, during))

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Databases[0].TableGroups[0].Shards[0].Backup.S3.Bucket = "db-bucket"
	})
	c.Eventually(30*time.Second, "the refusal to clear", func() error {
		cond, phase, err := repositoryCondition()
		if err != nil {
			return err
		}
		if cond == nil || cond.Status != metav1.ConditionTrue {
			return fmt.Errorf("BackupRepositoryValid is %+v", cond)
		}
		if phase == api.PhaseDegraded {
			return fmt.Errorf("phase is still %q", phase)
		}
		return nil
	})
	after, err := sidecarPods()
	c.NoError(err)
	c.True(maps.Equal(before, after), fmt.Sprintf(
		"clearing the refusal must not replace pods built from the same spec: %v became %v",
		before, after))
}
