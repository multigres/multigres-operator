package topo

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// RegisterDatabase registers the database metadata in the global topology.
// It creates the database entry or updates it if it already exists.
func RegisterDatabase(
	ctx context.Context,
	store topoclient.Store,
	recorder record.EventRecorder,
	shard *multigresv1alpha1.Shard,
) error {
	logger := log.FromContext(ctx)

	dbName := string(shard.Spec.DatabaseName)
	cells := CollectCells(shard)

	durabilityPolicy, err := GetDurabilityPolicy(shard)
	if err != nil {
		recorder.Eventf(
			shard,
			"Warning",
			"RegistrationFailed",
			"Failed to register database in topology: %v",
			err,
		)
		return fmt.Errorf("failed to register database %s in topology: %w", dbName, err)
	}

	dbMetadata := &clustermetadatapb.Database{
		Name:                      dbName,
		BackupLocation:            GetBackupLocation(shard),
		BootstrapDurabilityPolicy: durabilityPolicy,
		Cells:                     cells,
	}

	if err := store.CreateDatabase(ctx, dbName, dbMetadata); err != nil {
		var topoErr topoclient.TopoError
		if errors.As(err, &topoErr) && topoErr.Code == topoclient.NodeExists {
			if err := store.UpdateDatabaseFields(
				ctx,
				dbName,
				func(existing *clustermetadatapb.Database) error {
					existing.BackupLocation = dbMetadata.BackupLocation
					existing.Cells = dbMetadata.Cells
					existing.BootstrapDurabilityPolicy = dbMetadata.BootstrapDurabilityPolicy
					return nil
				},
			); err != nil {
				return fmt.Errorf("updating existing database %s in topology: %w", dbName, err)
			}
			logger.V(1).Info("Updated existing database in topology", "database", dbName)
			return nil
		}
		recorder.Eventf(
			shard,
			"Warning",
			"RegistrationFailed",
			"Failed to register database in topology: %v",
			err,
		)
		return fmt.Errorf("failed to create database in topology: %w", err)
	}

	logger.Info("Database metadata stored in topology", "database", dbName)
	recorder.Eventf(
		shard,
		"Normal",
		"DatabaseRegistered",
		"Registered database '%s' in topology",
		dbName,
	)
	return nil
}

// UnregisterDatabase removes the database metadata from the global topology.
func UnregisterDatabase(
	ctx context.Context,
	store topoclient.Store,
	recorder record.EventRecorder,
	shard *multigresv1alpha1.Shard,
) error {
	logger := log.FromContext(ctx)

	dbName := string(shard.Spec.DatabaseName)

	if err := store.DeleteDatabase(ctx, dbName, false); err != nil {
		var topoErr topoclient.TopoError
		if errors.As(err, &topoErr) && topoErr.Code == topoclient.NoNode {
			logger.V(1).
				Info("Database does not exist in topology, skipping deletion", "database", dbName)
			return nil
		}
		if !IsTopoUnavailable(err) {
			recorder.Eventf(
				shard,
				"Warning",
				"UnregistrationFailed",
				"Failed to remove database from topology: %v",
				err,
			)
		}
		return fmt.Errorf("failed to delete database %s from topology: %w", dbName, err)
	}

	logger.Info("Database metadata removed from topology", "database", dbName)
	recorder.Eventf(
		shard,
		"Normal",
		"DatabaseUnregistered",
		"Removed database '%s' from topology",
		dbName,
	)
	return nil
}

// GetBackupLocation extracts the backup location from the shard config.
func GetBackupLocation(shard *multigresv1alpha1.Shard) *clustermetadatapb.BackupLocation {
	requireInitialRepoEncryption := shard.Spec.Backup != nil && shard.Spec.Backup.Encryption != nil

	if shard.Spec.Backup != nil &&
		shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeS3 &&
		shard.Spec.Backup.S3 != nil {
		return &clustermetadatapb.BackupLocation{
			Location: &clustermetadatapb.BackupLocation_S3{
				S3: &clustermetadatapb.S3Backup{
					Bucket:            shard.Spec.Backup.S3.Bucket,
					Region:            shard.Spec.Backup.S3.Region,
					Endpoint:          shard.Spec.Backup.S3.Endpoint,
					KeyPrefix:         shard.Spec.Backup.S3.KeyPrefix,
					UseEnvCredentials: shard.Spec.Backup.S3.UseEnvCredentials,
				},
			},
			RequireInitialRepoEncryption: requireInitialRepoEncryption,
		}
	}

	path := "/backups"
	if shard.Spec.Backup != nil &&
		shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeFilesystem &&
		shard.Spec.Backup.Filesystem != nil &&
		shard.Spec.Backup.Filesystem.Path != "" {
		path = shard.Spec.Backup.Filesystem.Path
	}

	return &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: path,
			},
		},
		RequireInitialRepoEncryption: requireInitialRepoEncryption,
	}
}

// GetDurabilityPolicy extracts the durability policy from the shard config.
// Falls back to "AT_LEAST_2" if not set (the default materialized by the webhook resolver).
func GetDurabilityPolicy(
	shard *multigresv1alpha1.Shard,
) (*clustermetadatapb.DurabilityPolicy, error) {
	return buildDurabilityPolicy(shard.Spec.DurabilityPolicy)
}

// buildDurabilityPolicy constructs a fully populated DurabilityPolicy proto from
// a policy name. Upstream multigres' consensus code routes on QuorumType, so
// leaving it at the proto3 zero value (QUORUM_TYPE_UNKNOWN) would cause
// "unsupported quorum type" failures in multiorch's analyzers. Falls back to
// "AT_LEAST_2" when name is empty.
func buildDurabilityPolicy(name string) (*clustermetadatapb.DurabilityPolicy, error) {
	if name == "" {
		name = "AT_LEAST_2"
	}
	policy := &clustermetadatapb.DurabilityPolicy{PolicyName: name}
	switch name {
	case "AT_LEAST_2":
		policy.QuorumType = clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N
		policy.RequiredCount = 2
	case "MULTI_CELL_AT_LEAST_2":
		policy.QuorumType = clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N
		policy.RequiredCount = 2
	default:
		return nil, fmt.Errorf(
			"unsupported durability policy %q (supported: AT_LEAST_2, MULTI_CELL_AT_LEAST_2)",
			name,
		)
	}
	return policy, nil
}
