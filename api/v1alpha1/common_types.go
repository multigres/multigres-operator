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
	corev1 "k8s.io/api/core/v1"
)

// ============================================================================
// Shared Configuration Structs
// ============================================================================
//
// These structs are used across multiple resources (Cluster, Templates, Children)
// to ensure consistency in configuration shapes.

// StatelessSpec defines the desired state for a scalable, stateless component
// like MultiAdmin, MultiOrch, or MultiGateway.
type StatelessSpec struct {
	// Replicas is the desired number of pods.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=128
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources defines the compute resource requirements.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Affinity defines the pod's scheduling constraints.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PodAnnotations are annotations to add to the pods.
	// +optional
	// +kubebuilder:validation:MaxProperties=20
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) < 64 && size(self[k]) < 256)",message="annotation keys must be <64 chars and values <256 chars"
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// PodLabels are additional labels to add to the pods.
	// +optional
	// +kubebuilder:validation:MaxProperties=20
	// +kubebuilder:validation:XValidation:rule="self.all(k, size(k) < 64 && size(self[k]) < 64)",message="label keys and values must be <64 chars"
	PodLabels map[string]string `json:"podLabels,omitempty"`
}

// StorageSpec defines the storage configuration.
type StorageSpec struct {
	// Size of the persistent volume.
	// +kubebuilder:validation:Pattern="^([0-9]+)(.+)$"
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Size string `json:"size,omitempty"`

	// Class is the StorageClass name.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Class string `json:"class,omitempty"`

	// AccessModes contains the desired access modes the volume should have.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes
	// +optional
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// PVCRetentionPolicyType defines when to delete PVCs.
// +kubebuilder:validation:Enum=Retain;Delete
type PVCRetentionPolicyType string

const (
	// RetainPVCRetentionPolicy keeps PVCs when resources are deleted or scaled down.
	RetainPVCRetentionPolicy PVCRetentionPolicyType = "Retain"
	// DeletePVCRetentionPolicy automatically deletes PVCs when resources are deleted or scaled down.
	DeletePVCRetentionPolicy PVCRetentionPolicyType = "Delete"
)

// PVCDeletionPolicy controls PVC lifecycle management for stateful components.
type PVCDeletionPolicy struct {
	// WhenDeleted controls PVC deletion when the MultigresCluster is deleted.
	// - Retain (default): PVCs are kept for manual review and recovery
	// - Delete: PVCs are automatically deleted with the cluster
	// +optional
	// +kubebuilder:default=Retain
	WhenDeleted PVCRetentionPolicyType `json:"whenDeleted,omitempty"`

	// WhenScaled controls PVC deletion when replicas are scaled down.
	// - Delete (default): PVCs are automatically deleted when pods are removed
	// - Retain: PVCs from scaled-down pods are kept for manual recovery
	// +optional
	// +kubebuilder:default=Delete
	WhenScaled PVCRetentionPolicyType `json:"whenScaled,omitempty"`
}

// TopologyPruningConfig controls whether the operator prunes stale topology entries.
type TopologyPruningConfig struct {
	// Enabled controls whether the operator prunes stale topology entries.
	// When false, the operator still registers entries but never removes them.
	// Default: true (nil or empty means enabled).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// ContainerConfig defines generic container configuration.
type ContainerConfig struct {
	// Resources defines the compute resource requirements.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ============================================================================
// Backup Configuration Types
// ============================================================================

// BackupType defines the backup storage backend.
// +kubebuilder:validation:Enum=filesystem;s3
type BackupType string

const (
	// BackupTypeFilesystem uses a local PVC for pgBackRest backup storage.
	BackupTypeFilesystem BackupType = "filesystem"
	// BackupTypeS3 uses S3-compatible object storage for pgBackRest backup storage.
	BackupTypeS3 BackupType = "s3"
)

// BackupConfig defines the pgBackRest backup configuration.
// +kubebuilder:validation:XValidation:rule="self.type != 's3' || has(self.s3)",message="s3 config is required when type is 's3'"
// +kubebuilder:validation:XValidation:rule="self.type != 'filesystem' || has(self.filesystem)",message="filesystem config is required when type is 'filesystem'"
type BackupConfig struct {
	// Type is the backup storage backend (filesystem or s3).
	Type BackupType `json:"type"`

	// Filesystem defines configuration for local/PVC-based backups.
	// Required when type is "filesystem".
	// +optional
	Filesystem *FilesystemBackupConfig `json:"filesystem,omitempty"`

	// S3 defines the S3-compatible storage configuration.
	// Required when type is "s3".
	// +optional
	S3 *S3BackupConfig `json:"s3,omitempty"`

	// PgBackRestTLS configures TLS certificates for pgBackRest inter-node communication.
	// Required for multi-replica shards where standbys connect to the primary's
	// pgBackRest server. When SecretName is set, the operator mounts that Secret directly.
	// When SecretName is empty, the operator auto-generates and rotates certificates.
	// +optional
	PgBackRestTLS *PgBackRestTLSConfig `json:"pgbackrestTLS,omitempty"`

	// Retention configures pgBackRest backup retention.
	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// FilesystemBackupConfig defines settings for filesystem-based backups.
type FilesystemBackupConfig struct {
	// Path is the filesystem directory for backups.
	// Defaults to "/backups".
	// +optional
	Path string `json:"path,omitempty"`

	// Storage defines the PVC configuration for the backup volume.
	// This volume is shared by all pools in the shard (per-cell).
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
}

// S3BackupConfig defines S3-compatible backup storage settings.
// +kubebuilder:validation:XValidation:rule="!(has(self.serviceAccountName) && size(self.serviceAccountName) > 0 && has(self.credentialsSecret) && size(self.credentialsSecret) > 0)",message="serviceAccountName and credentialsSecret are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!(has(self.serviceAccountName) && size(self.serviceAccountName) > 0 && has(self.useEnvCredentials) && self.useEnvCredentials)",message="serviceAccountName and useEnvCredentials are mutually exclusive — IRSA credentials are handled automatically"
type S3BackupConfig struct {
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`
	// +kubebuilder:validation:MinLength=1
	Region            string `json:"region"`
	Endpoint          string `json:"endpoint,omitempty"`
	KeyPrefix         string `json:"keyPrefix,omitempty"`
	UseEnvCredentials bool   `json:"useEnvCredentials,omitempty"`

	// CredentialsSecret is the name of the Secret containing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`

	// ServiceAccountName is the ServiceAccount used for IRSA-based S3 authentication.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// CleanupOnDelete deletes S3 backup data during cluster deletion.
	// +optional
	CleanupOnDelete *bool `json:"cleanupOnDelete,omitempty"`

	// AllowStaleMetadataRecovery allows automatic recovery from stale stanza metadata.
	// WARNING: This may delete existing backup data.
	// +optional
	AllowStaleMetadataRecovery *bool `json:"allowStaleMetadataRecovery,omitempty"`
}

// PgBackRestTLSConfig configures TLS for pgBackRest inter-node communication.
type PgBackRestTLSConfig struct {
	// SecretName is the name of an existing Secret containing pgBackRest TLS certificates.
	// The Secret must contain three keys: ca.crt, tls.crt, tls.key.
	// This is directly compatible with cert-manager Certificate resources.
	// When empty, the operator generates and rotates certificates automatically.
	// +optional
	SecretName string `json:"secretName,omitempty"`
}

// RetentionPolicy configures pgBackRest backup retention.
type RetentionPolicy struct {
	// FullCount is the number of full backups to retain.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=30
	// +optional
	FullCount *int32 `json:"fullCount,omitempty"`

	// DifferentialCount is the number of differential backups to retain per full backup.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=30
	// +optional
	DifferentialCount *int32 `json:"differentialCount,omitempty"`
}

// ============================================================================
// Domain Specific Types (Strong Typing)
// ============================================================================

// DatabaseName is a validated name for a logical database within a MultigresCluster.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=30
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type DatabaseName string

// TableGroupName is a validated name for a table group within a database.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=25
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type TableGroupName string

// ShardName is a validated name for a shard within a table group.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=25
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type ShardName string

// PoolName is a validated name for a connection pool within a shard.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=25
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type PoolName string

// CellName is a validated name for a cell (availability zone unit) within a cluster.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=30
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type CellName string

// InitdbArgs is an opaque string of extra arguments forwarded to initdb
// during PostgreSQL data directory initialization.
// +kubebuilder:validation:MaxLength=512
type InitdbArgs string

// PostgresConfigRef references a ConfigMap containing custom postgresql.conf content.
// The referenced ConfigMap must exist in the same namespace as the MultigresCluster.
type PostgresConfigRef struct {
	// Name is the name of the ConfigMap.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Key is the key within the ConfigMap's data that contains the postgresql.conf content.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// IPAddress is a validated IPv4 or IPv6 address string.
// +kubebuilder:validation:MinLength=3
// +kubebuilder:validation:MaxLength=45
// +kubebuilder:validation:Pattern=`^(([0-9]{1,3}\.){3}[0-9]{1,3})$|^(([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4})$`
type IPAddress string

// Zone is the cloud provider availability zone identifier.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=63
type Zone string

// Region is the cloud provider region identifier.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=63
type Region string

// TemplateRef is a reference to a named template resource (CoreTemplate, CellTemplate, or ShardTemplate).
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=63
type TemplateRef string

// ImageRef is a container image reference.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=512
type ImageRef string

// LogLevel is a validated log level string for multigres components.
// +kubebuilder:validation:Enum=debug;info;warn;error
type LogLevel string

// ComponentLogLevels configures the --log-level flag for each multigres data-plane component.
// All fields are optional; unset fields default to "info" via the admission webhook.
type ComponentLogLevels struct {
	// +optional
	Pgctld LogLevel `json:"pgctld,omitempty"`
	// +optional
	Multipooler LogLevel `json:"multipooler,omitempty"`
	// +optional
	Multiorch LogLevel `json:"multiorch,omitempty"`
	// +optional
	Multiadmin LogLevel `json:"multiadmin,omitempty"`
	// +optional
	Multigateway LogLevel `json:"multigateway,omitempty"`
}

// PoolType constrains the allowed pool access patterns.
// +kubebuilder:validation:Enum=readWrite
type PoolType string

// Phase represents the lifecycle state of a Multigres resource.
// +kubebuilder:validation:Enum=Initializing;Progressing;Healthy;Degraded;Unknown
type Phase string

const (
	PhaseInitializing Phase = "Initializing" // Resource is being created
	PhaseProgressing  Phase = "Progressing"  // Desired state not yet achieved (rolling update)
	PhaseHealthy      Phase = "Healthy"      // Desired state reached
	PhaseDegraded     Phase = "Degraded"     // Resource is failing / crashing
	PhaseUnknown      Phase = "Unknown"
)

const (
	// AnnotationPendingDeletion is set by the TableGroup controller on a Shard
	// that should be gracefully drained before deletion. The shard controller
	// drains all pods and sets ConditionReadyForDeletion when complete.
	AnnotationPendingDeletion = "multigres.com/pending-deletion"

	// ConditionReadyForDeletion is set to True on a Shard once all pods have
	// been drained. The TableGroup controller waits for this condition before
	// calling Delete on the Shard CR.
	ConditionReadyForDeletion = "ReadyForDeletion"
)

// MergePVCDeletionPolicy merges child and parent policies with child taking precedence.
// If child is nil, returns parent. If both nil, returns nil (caller uses default Retain).
func MergePVCDeletionPolicy(child, parent *PVCDeletionPolicy) *PVCDeletionPolicy {
	if child == nil {
		return parent
	}

	// Child exists, create merged policy
	merged := &PVCDeletionPolicy{}

	// Merge WhenDeleted
	if child.WhenDeleted != "" {
		merged.WhenDeleted = child.WhenDeleted
	} else if parent != nil {
		merged.WhenDeleted = parent.WhenDeleted
	}

	// Merge WhenScaled
	if child.WhenScaled != "" {
		merged.WhenScaled = child.WhenScaled
	} else if parent != nil {
		merged.WhenScaled = parent.WhenScaled
	}

	// If merged is empty, return nil (let caller use defaults)
	if merged.WhenDeleted == "" && merged.WhenScaled == "" {
		return nil
	}

	return merged
}

// MergeBackupConfig merges child and parent backup config with child taking precedence.
// Implements deep merge logic where appropriate.
func MergeBackupConfig(child, parent *BackupConfig) *BackupConfig {
	if child == nil && parent == nil {
		return nil
	}
	if child == nil {
		return parent.DeepCopy()
	}
	if parent == nil {
		return child.DeepCopy()
	}

	// Start with parent config as base
	merged := parent.DeepCopy()

	// If child changes the type, it fully replaces the parent config
	if child.Type != "" && child.Type != parent.Type {
		return child.DeepCopy()
	}

	// If types match (or child adopts parent type), merge details
	if child.Type != "" {
		merged.Type = child.Type
	}

	switch merged.Type {
	case BackupTypeFilesystem:
		if merged.Filesystem == nil {
			merged.Filesystem = &FilesystemBackupConfig{}
		}
		if child.Filesystem != nil {
			if child.Filesystem.Path != "" {
				merged.Filesystem.Path = child.Filesystem.Path
			}
			// Storage spec replacement
			if child.Filesystem.Storage.Size != "" {
				merged.Filesystem.Storage.Size = child.Filesystem.Storage.Size
			}
			if child.Filesystem.Storage.Class != "" {
				merged.Filesystem.Storage.Class = child.Filesystem.Storage.Class
			}
			if len(child.Filesystem.Storage.AccessModes) > 0 {
				merged.Filesystem.Storage.AccessModes = child.Filesystem.Storage.AccessModes
			}
		}
	case BackupTypeS3:
		if merged.S3 == nil {
			merged.S3 = &S3BackupConfig{}
		}
		if child.S3 != nil {
			if child.S3.Bucket != "" {
				merged.S3.Bucket = child.S3.Bucket
			}
			if child.S3.Region != "" {
				merged.S3.Region = child.S3.Region
			}
			if child.S3.Endpoint != "" {
				merged.S3.Endpoint = child.S3.Endpoint
			}
			if child.S3.KeyPrefix != "" {
				merged.S3.KeyPrefix = child.S3.KeyPrefix
			}
			// UseEnvCredentials follows child.S3 when the child S3 block is present.
			merged.S3.UseEnvCredentials = child.S3.UseEnvCredentials
			if child.S3.CredentialsSecret != "" {
				merged.S3.CredentialsSecret = child.S3.CredentialsSecret
			}
			if child.S3.ServiceAccountName != "" {
				merged.S3.ServiceAccountName = child.S3.ServiceAccountName
			}
			if child.S3.CleanupOnDelete != nil {
				merged.S3.CleanupOnDelete = child.S3.CleanupOnDelete
			}
			if child.S3.AllowStaleMetadataRecovery != nil {
				merged.S3.AllowStaleMetadataRecovery = child.S3.AllowStaleMetadataRecovery
			}
		}
	}

	if child.PgBackRestTLS != nil {
		merged.PgBackRestTLS = child.PgBackRestTLS.DeepCopy()
	}

	if child.Retention != nil {
		if merged.Retention == nil {
			merged.Retention = &RetentionPolicy{}
		}
		if child.Retention.FullCount != nil {
			merged.Retention.FullCount = child.Retention.FullCount
		}
		if child.Retention.DifferentialCount != nil {
			merged.Retention.DifferentialCount = child.Retention.DifferentialCount
		}
	}

	return merged
}
