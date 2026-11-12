/*
Copyright 2026.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ============================================================================
// RBAC Markers
// ============================================================================
//
// +kubebuilder:rbac:groups=multigres.com,resources=multigresbackups;multigresbackupschedules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=multigresbackups/status;multigresbackupschedules/status,verbs=get;update;patch

// ============================================================================
// Shared types
// ============================================================================

// BackupKind is the pgBackRest backup type (full, differential or incremental). Not to be confused with BackupType, the storage backend.
// +kubebuilder:validation:Enum=full;differential;incremental
type BackupKind string

const (
	BackupKindFull         BackupKind = "full"
	BackupKindDifferential BackupKind = "differential"
	BackupKindIncremental  BackupKind = "incremental"
)

// Strength orders backup types: a backup of a stronger type makes a
// concurrent request for a weaker one redundant (full > differential >
// incremental). Unknown types are the weakest.
func (t BackupKind) Strength() int {
	switch t {
	case BackupKindFull:
		return 3
	case BackupKindDifferential:
		return 2
	case BackupKindIncremental:
		return 1
	default:
		return 0
	}
}

// BackupClusterReference names the MultigresCluster a backup belongs to. The
// cluster must live in the same namespace.
type BackupClusterReference struct {
	// Name of the MultigresCluster.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
}

// BackupTarget selects one shard.
type BackupTarget struct {
	// +kubebuilder:validation:MinLength=1
	Database string `json:"database"`
	// +kubebuilder:validation:MinLength=1
	TableGroup string `json:"tableGroup"`
	// +kubebuilder:validation:MinLength=1
	Shard string `json:"shard"`
}

// BackupRetryPolicy controls retries within one scheduled run.
type BackupRetryPolicy struct {
	// MaxAttempts is the total number of attempts per scheduled run, including
	// the first.
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// BackoffSeconds is the minimum wait between a failed attempt finishing
	// and the next attempt being created.
	// +kubebuilder:default=1800
	// +kubebuilder:validation:Minimum=60
	// +optional
	BackoffSeconds int64 `json:"backoffSeconds,omitempty"`
}

// BackupScheduleTemplate is everything about a schedule except the cluster it
// belongs to. It is shared by MultigresBackupSchedule and
// MultigresCluster.spec.backupSchedules.
type BackupScheduleTemplate struct {
	// Schedule is a standard 5-field cron expression, always evaluated in UTC.
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	// Type of backup each run takes.
	Type BackupKind `json:"type"`

	// Targets restricts the schedule to these shards. Empty means every shard
	// in the cluster.
	// +optional
	// +kubebuilder:validation:MaxItems=1000
	Targets []BackupTarget `json:"targets,omitempty"`

	// Suspend stops new runs from being created. Running attempts continue.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// StartingDeadlineSeconds is how late a run may start. Runs that could not
	// start within this window (for example because the operator was down)
	// are counted as missed. Retries also stop once the window has passed.
	// +kubebuilder:default=21600
	// +kubebuilder:validation:Minimum=0
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// Retry controls retries within one scheduled run.
	// +kubebuilder:default={}
	// +optional
	Retry BackupRetryPolicy `json:"retry,omitempty"`

	// BackupTimeoutSeconds is copied into each attempt's timeoutSeconds.
	// +kubebuilder:default=14400
	// +kubebuilder:validation:Minimum=600
	// +optional
	BackupTimeoutSeconds int64 `json:"backupTimeoutSeconds,omitempty"`

	// ForcePrimary allows the backup to run on the primary when no replica is
	// available.
	// +optional
	ForcePrimary bool `json:"forcePrimary,omitempty"`

	// SuccessfulBackupsHistoryLimit is how many succeeded attempts to keep.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	SuccessfulBackupsHistoryLimit *int32 `json:"successfulBackupsHistoryLimit,omitempty"`

	// FailedBackupsHistoryLimit is how many failed attempts to keep.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=0
	// +optional
	FailedBackupsHistoryLimit *int32 `json:"failedBackupsHistoryLimit,omitempty"`
}

// BackupScheduleEntry declares one schedule inline on a MultigresCluster. The
// cluster controller renders it into a MultigresBackupSchedule named
// <cluster>-<name>.
type BackupScheduleEntry struct {
	// Name is unique within the list.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=30
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	BackupScheduleTemplate `json:",inline"`
}

// Defaults applied when a field is unset, matching the CRD defaults. The
// controllers use these so objects built in code (and objects created before
// a default existed) behave the same as API-server-defaulted ones.
const (
	DefaultBackupStartingDeadlineSeconds int64 = 21600
	DefaultBackupMaxAttempts             int32 = 4
	DefaultBackupBackoffSeconds          int64 = 1800
	DefaultBackupTimeoutSeconds          int64 = 14400
	DefaultSuccessfulBackupsHistoryLimit int32 = 3
	DefaultFailedBackupsHistoryLimit     int32 = 5
)

// StartingDeadline returns StartingDeadlineSeconds or its default.
func (t *BackupScheduleTemplate) StartingDeadline() int64 {
	if t.StartingDeadlineSeconds == nil {
		return DefaultBackupStartingDeadlineSeconds
	}
	return *t.StartingDeadlineSeconds
}

// MaxAttempts returns Retry.MaxAttempts or its default.
func (t *BackupScheduleTemplate) MaxAttempts() int32 {
	if t.Retry.MaxAttempts <= 0 {
		return DefaultBackupMaxAttempts
	}
	return t.Retry.MaxAttempts
}

// BackoffSeconds returns Retry.BackoffSeconds or its default.
func (t *BackupScheduleTemplate) BackoffSeconds() int64 {
	if t.Retry.BackoffSeconds <= 0 {
		return DefaultBackupBackoffSeconds
	}
	return t.Retry.BackoffSeconds
}

// TimeoutSeconds returns BackupTimeoutSeconds or its default.
func (t *BackupScheduleTemplate) TimeoutSeconds() int64 {
	if t.BackupTimeoutSeconds <= 0 {
		return DefaultBackupTimeoutSeconds
	}
	return t.BackupTimeoutSeconds
}

// SuccessfulHistoryLimit returns SuccessfulBackupsHistoryLimit or its default.
func (t *BackupScheduleTemplate) SuccessfulHistoryLimit() int32 {
	if t.SuccessfulBackupsHistoryLimit == nil {
		return DefaultSuccessfulBackupsHistoryLimit
	}
	return *t.SuccessfulBackupsHistoryLimit
}

// FailedHistoryLimit returns FailedBackupsHistoryLimit or its default.
func (t *BackupScheduleTemplate) FailedHistoryLimit() int32 {
	if t.FailedBackupsHistoryLimit == nil {
		return DefaultFailedBackupsHistoryLimit
	}
	return *t.FailedBackupsHistoryLimit
}

// Labels shared by MultigresBackup objects. Cluster, database, tablegroup and
// shard reuse the existing multigres.com/* label keys.
const (
	// LabelBackupSchedule names the MultigresBackupSchedule that created an
	// attempt. Absent on manual backups.
	LabelBackupSchedule = "multigres.com/backup-schedule"
	// LabelBackupTrigger is "scheduled" or "manual".
	LabelBackupTrigger = "multigres.com/trigger"

	BackupTriggerScheduled = "scheduled"
	BackupTriggerManual    = "manual"
)

// ============================================================================
// MultigresBackupSchedule
// ============================================================================

// MultigresBackupScheduleSpec defines when to back up which shards.
// +kubebuilder:validation:XValidation:rule="self.clusterRef == oldSelf.clusterRef",message="clusterRef is immutable"
type MultigresBackupScheduleSpec struct {
	// ClusterRef is the MultigresCluster whose shards are backed up.
	ClusterRef BackupClusterReference `json:"clusterRef"`

	BackupScheduleTemplate `json:",inline"`
}

// BackupScheduleOutcome records what the schedule did for a target's most
// recent scheduled time.
// +kubebuilder:validation:Enum=Created;Skipped;Missed
type BackupScheduleOutcome string

const (
	BackupScheduleOutcomeCreated BackupScheduleOutcome = "Created"
	BackupScheduleOutcomeSkipped BackupScheduleOutcome = "Skipped"
	BackupScheduleOutcomeMissed  BackupScheduleOutcome = "Missed"
)

// BackupScheduleTargetStatus is the schedule's state for one shard.
type BackupScheduleTargetStatus struct {
	BackupTarget `json:",inline"`

	// LastScheduleTime is the most recent scheduled time the controller acted
	// on for this shard.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// LastScheduleOutcome is what happened at LastScheduleTime.
	// +optional
	LastScheduleOutcome BackupScheduleOutcome `json:"lastScheduleOutcome,omitempty"`

	// LastSuccessfulTime is when the newest successful attempt from this
	// schedule completed.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// NextScheduleTime is the next time a run is due.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// Active is the unfinished attempt from this schedule, if any.
	// +optional
	Active *corev1.LocalObjectReference `json:"active,omitempty"`

	// Waiting explains why no run is created for this shard yet, e.g. the
	// shard has not finished bootstrapping.
	// +optional
	Waiting string `json:"waiting,omitempty"`
}

// MultigresBackupScheduleStatus defines the observed state.
type MultigresBackupScheduleStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	// +listType=atomic
	Targets []BackupScheduleTargetStatus `json:"targets,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition reasons for MultigresBackupSchedule Ready.
const (
	BackupScheduleReasonScheduled       = "Scheduled"
	BackupScheduleReasonInvalidSchedule = "InvalidSchedule"
	BackupScheduleReasonClusterNotFound = "ClusterNotFound"
	BackupScheduleReasonTargetNotFound  = "TargetNotFound"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mgbs
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.schedule"
// +kubebuilder:printcolumn:name="Type",type="string",JSONPath=".spec.type"
// +kubebuilder:printcolumn:name="Suspend",type="boolean",JSONPath=".spec.suspend"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// MultigresBackupSchedule creates MultigresBackup attempts on a cron schedule.
type MultigresBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MultigresBackupScheduleSpec   `json:"spec,omitempty"`
	Status MultigresBackupScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MultigresBackupScheduleList contains a list of MultigresBackupSchedule.
type MultigresBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MultigresBackupSchedule `json:"items"`
}

// ============================================================================
// MultigresBackup
// ============================================================================

// MultigresBackupSpec describes one backup attempt. It is immutable.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
type MultigresBackupSpec struct {
	// ClusterRef is the MultigresCluster that owns the shard.
	ClusterRef BackupClusterReference `json:"clusterRef"`

	// +kubebuilder:validation:MinLength=1
	Database string `json:"database"`
	// +kubebuilder:validation:MinLength=1
	TableGroup string `json:"tableGroup"`
	// +kubebuilder:validation:MinLength=1
	Shard string `json:"shard"`

	// Type of backup.
	Type BackupKind `json:"type"`

	// ForcePrimary allows the backup to run on the primary.
	// +optional
	ForcePrimary bool `json:"forcePrimary,omitempty"`

	// TimeoutSeconds bounds how long the attempt may stay Running.
	// +kubebuilder:default=14400
	// +kubebuilder:validation:Minimum=600
	// +optional
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

	// ScheduledTime is the scheduled run this attempt belongs to. Unset for
	// manual backups.
	// +optional
	ScheduledTime *metav1.Time `json:"scheduledTime,omitempty"`

	// Attempt is the attempt number within the scheduled run.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	Attempt int32 `json:"attempt,omitempty"`
}

// Timeout returns TimeoutSeconds or its default.
func (s *MultigresBackupSpec) Timeout() int64 {
	if s.TimeoutSeconds <= 0 {
		return DefaultBackupTimeoutSeconds
	}
	return s.TimeoutSeconds
}

// AttemptNumber returns Attempt or 1.
func (s *MultigresBackupSpec) AttemptNumber() int32 {
	if s.Attempt <= 0 {
		return 1
	}
	return s.Attempt
}

// BackupPhase is the lifecycle phase of a MultigresBackup.
// +kubebuilder:validation:Enum=Pending;Triggering;Running;Succeeded;Failed
type BackupPhase string

const (
	BackupPhasePending    BackupPhase = "Pending"
	BackupPhaseTriggering BackupPhase = "Triggering"
	BackupPhaseRunning    BackupPhase = "Running"
	BackupPhaseSucceeded  BackupPhase = "Succeeded"
	BackupPhaseFailed     BackupPhase = "Failed"
)

// Finished reports whether the phase is terminal.
func (p BackupPhase) Finished() bool {
	return p == BackupPhaseSucceeded || p == BackupPhaseFailed
}

// Failure reasons for MultigresBackup.
const (
	BackupReasonClusterNotFound       = "ClusterNotFound"
	BackupReasonTriggerRejected       = "TriggerRejected"
	BackupReasonTriggerOutcomeUnknown = "TriggerOutcomeUnknown"
	BackupReasonBackupFailed          = "BackupFailed"
	BackupReasonLost                  = "Lost"
	BackupReasonTimeout               = "Timeout"

	// BackupConditionComplete is True once the attempt finished, with the
	// phase as reason.
	BackupConditionComplete = "Complete"
)

// MultigresBackupStatus defines the observed state of one attempt.
type MultigresBackupStatus struct {
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// Reason is set on Failed.
	// +optional
	Reason string `json:"reason,omitempty"`

	// +optional
	Message string `json:"message,omitempty"`

	// JobID is multiadmin's job ID for the backup.
	// +optional
	JobID string `json:"jobID,omitempty"`

	// BackupID is the pgBackRest backup label once the backup completed.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// TriggerTime is when the attempt entered Triggering, saved before the
	// Backup RPC.
	// +optional
	TriggerTime *metav1.Time `json:"triggerTime,omitempty"`

	// StartTime is when multiadmin accepted the backup.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the attempt finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// NotFoundSince is when GetBackupJobStatus first started returning
	// NotFound without a successful status since. Multiadmin's NotFound does
	// not prove the job is gone, so the attempt is only marked Lost after a
	// grace period.
	// +optional
	NotFoundSince *metav1.Time `json:"notFoundSince,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mgb
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Shard",type="string",JSONPath=".spec.shard"
// +kubebuilder:printcolumn:name="Type",type="string",JSONPath=".spec.type"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason"
// +kubebuilder:printcolumn:name="Backup",type="string",JSONPath=".status.backupID"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// MultigresBackup is one backup attempt of one shard, scheduled or manual.
type MultigresBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MultigresBackupSpec   `json:"spec,omitempty"`
	Status MultigresBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MultigresBackupList contains a list of MultigresBackup.
type MultigresBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MultigresBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&MultigresBackup{}, &MultigresBackupList{},
		&MultigresBackupSchedule{}, &MultigresBackupScheduleList{},
	)
}
