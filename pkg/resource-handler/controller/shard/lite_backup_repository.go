package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/topoclient"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// errLiteDefaultS3Repository refuses a new Lite shard whose S3 backups would
// use the bucket's default repository.
var errLiteDefaultS3Repository = errors.New(multigresv1alpha1.LiteDefaultS3RepositoryMessage)

// errLiteMoveToDefaultS3Repository refuses moving an existing Lite shard into
// a default repository it did not already use.
var errLiteMoveToDefaultS3Repository = errors.New(
	multigresv1alpha1.LiteMoveToDefaultS3RepositoryMessage,
)

const (
	// conditionBackupRepositoryValid reports whether a Lite shard's S3 backups
	// have a repository of their own. It is written only for Lite shards that
	// use the default repository, or that did and no longer do.
	conditionBackupRepositoryValid = "BackupRepositoryValid"

	reasonDefaultS3Repository       = "DefaultS3Repository"
	reasonMoveToDefaultS3Repository = "MoveToDefaultS3Repository"
	reasonExistingDefaultRepository = "ExistingDefaultRepository"
	reasonKeyPrefixSet              = "KeyPrefixSet"
)

// checkLiteBackupRepository refuses to provision a Lite shard whose S3 backups
// have no key prefix. A Lite static leader backs up and expires without a
// backup lease, on the assumption that its repository is its own. A new Lite
// cluster sharing another's repository breaks that: if the repository already
// holds a complete backup, the new pooler restores it and starts with the
// other cluster's data, then archives WAL into the same stanza; otherwise one
// of the two clusters' stanza-create fails. Admission rejects these; this
// check also holds when the webhook is disabled, and records the outcome in
// the BackupRepositoryValid condition.
//
// A shard provisioned before this check may already have backups in the
// default repository and is left alone so they do not move: its database
// record in topology names the same bucket and endpoint with no prefix, or,
// when the record is gone, its data PVC still exists. A shard with neither is
// refused before the record, PVC or pod is created, and an existing shard is
// refused a change that would put it in a default repository it did not use.
func (r *ShardReconciler) checkLiteBackupRepository(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) error {
	if !multigresv1alpha1.UsesDefaultS3Repository(shard.Spec.Backup) {
		// Clear a refusal recorded before the shard got a key prefix. Shards
		// that never had the condition skip the read.
		if meta.FindStatusCondition(
			shard.Status.Conditions,
			conditionBackupRepositoryValid,
		) == nil {
			return nil
		}
		return r.applyGuardCondition(ctx, shard, metav1.Condition{
			Type:    conditionBackupRepositoryValid,
			Status:  metav1.ConditionTrue,
			Reason:  reasonKeyPrefixSet,
			Message: "Backups go to a repository of this cluster's own",
		}, true)
	}

	refuse := func(refusal error, reason string) error {
		r.Recorder.Eventf(shard, "Warning", "ConfigError", "%v", refusal)
		if err := r.applyGuardCondition(ctx, shard, metav1.Condition{
			Type:    conditionBackupRepositoryValid,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: refusal.Error(),
		}, false); err != nil {
			return err
		}
		return refusal
	}
	keep := func() error {
		return r.applyGuardCondition(ctx, shard, metav1.Condition{
			Type:   conditionBackupRepositoryValid,
			Status: metav1.ConditionTrue,
			Reason: reasonExistingDefaultRepository,
			Message: "This cluster was provisioned without spec.backup.s3.keyPrefix and keeps " +
				"its existing default repository",
		}, false)
	}

	db, err := store.GetDatabase(ctx, string(shard.Spec.DatabaseName))
	if err == nil {
		if sameDefaultS3Repository(db.GetBackupLocation().GetS3(), shard.Spec.Backup.S3) {
			return keep()
		}
		return refuse(errLiteMoveToDefaultS3Repository, reasonMoveToDefaultS3Repository)
	}
	var topoErr topoclient.TopoError
	if !errors.As(err, &topoErr) || topoErr.Code != topoclient.NoNode {
		return fmt.Errorf("read Lite database record: %w", err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolDataPVCName(shard, poolName, cellName, 0),
	}
	if err := r.Get(ctx, key, pvc); err == nil {
		return keep()
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("read Lite data PVC: %w", err)
	}

	return refuse(errLiteDefaultS3Repository, reasonDefaultS3Repository)
}

// sameDefaultS3Repository reports whether the registered location is the same
// default repository the spec asks for: S3 with no key prefix, on the same
// bucket and endpoint.
func sameDefaultS3Repository(
	registered *cm.S3Backup,
	desired *multigresv1alpha1.S3BackupConfig,
) bool {
	return registered != nil && desired != nil &&
		strings.Trim(registered.GetKeyPrefix(), "/") == "" &&
		registered.GetBucket() == desired.Bucket &&
		registered.GetEndpoint() == desired.Endpoint
}
