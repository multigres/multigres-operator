package common

import "time"

// Label keys used by the multigres operator on managed resources.
const (
	LabelAppManagedBy        = "app.kubernetes.io/managed-by"
	LabelAppComponent        = "app.kubernetes.io/component"
	LabelAppInstance         = "app.kubernetes.io/instance"
	LabelMultigresCluster    = "multigres.com/cluster"
	LabelMultigresCell       = "multigres.com/cell"
	LabelMultigresShard      = "multigres.com/shard"
	LabelMultigresPool       = "multigres.com/pool"
	LabelMultigresDatabase   = "multigres.com/database"
	LabelMultigresTableGroup = "multigres.com/tablegroup"

	ManagedByMultigres = "multigres-operator"
)

// Annotation keys used by the multigres operator.
const (
	AnnotationSpecHash         = "multigres.com/spec-hash"
	AnnotationDrainState       = "drain.multigres.com/state"
	AnnotationDrainRequestedAt = "drain.multigres.com/requested-at"
)

// Drain states matching the operator's state machine.
const (
	DrainStateRequested        = "requested"
	DrainStateDraining         = "draining"
	DrainStateAcknowledged     = "acknowledged"
	DrainStateReadyForDeletion = "ready-for-deletion"
)

// Component names for app.kubernetes.io/component labels.
const (
	ComponentPool         = "shard-pool"
	ComponentMultiorch    = "multiorch"
	ComponentMultigateway = "multigateway"
	ComponentGlobalTopo   = "toposerver"
)

// Well-known ports for multigres components.
const (
	PortMultigatewayHTTP = 15100
	PortMultigatewayGRPC = 15170
	PortMultigatewayPG   = 5432
	PortMultipoolerHTTP  = 15200
	PortMultipoolerGRPC  = 15270
	PortMultiorchHTTP    = 15300
	PortMultiorchGRPC    = 15370
	PortEtcdClient       = 2379
	PortEtcdPeer         = 2380
	PortOperatorHealth   = 8081
	PortOperatorMetrics  = 8443
	PortPostgres         = 5432
)

// PostgreSQL NAMEDATALEN limit for application_name.
const PGNameDataLen = 63

// Default thresholds for observer checks.
const (
	DefaultInterval              = 10 * time.Second
	PendingTimeout               = 60 * time.Second
	NotReadyTimeout              = 30 * time.Second
	TerminatingTimeout           = 90 * time.Second
	RestartWarnThreshold         = 1
	RestartErrorThreshold        = 3
	RestartErrorWindow           = 5 * time.Minute
	DrainRequestedTimeout        = 30 * time.Second
	DrainDrainingTimeout         = 5 * time.Minute
	DrainAcknowledgedTimeout     = 30 * time.Second
	PhaseDegradedTimeout         = 5 * time.Minute
	GenerationDivergeTimeout     = 60 * time.Second
	PrimaryGracePeriod           = 60 * time.Second
	StaleStatusEntryGracePeriod  = 30 * time.Second
	TerminatingResourceTimeout   = 5 * time.Minute
	ConnectivityTimeout          = 5 * time.Second
	GRPCHealthTimeout            = 3 * time.Second
	ConnectivityLatencyThreshold = 500 * time.Millisecond
	ReplicationLagWarnSecs       = 10
	ReplicationLagErrorSecs      = 60
	PodStartupGracePeriod        = 2 * time.Minute
	BackupStalenessWarnAge       = 25 * time.Hour
	BackupStalenessErrorAge      = 49 * time.Hour
	PhaseProgressingTimeout      = 10 * time.Minute
	MetricsProbeTimeout          = 5 * time.Second
)

// ValidBackupTypes lists the known pgBackRest backup types.
var ValidBackupTypes = []string{"full", "diff", "incr"}

// Operator event reasons to monitor.
const (
	EventReasonBackupStale      = "BackupStale"
	EventReasonConfigError      = "ConfigError"
	EventReasonExpandPVCFailed  = "ExpandPVCFailed"
	EventReasonPodReplaced      = "PodReplaced"
	EventReasonStatusError      = "StatusError"
	EventReasonStuckTerminating = "StuckTerminating"
	EventReasonTopologyError    = "TopologyError"
)
