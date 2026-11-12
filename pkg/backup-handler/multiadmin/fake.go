package multiadmin

import (
	"context"
	"fmt"
	"sync"
	"time"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// Fake is an in-memory multiadmin. Each Backup call creates a job in
// PENDING; tests move jobs along with SetJob, or make calls fail with
// BackupErr, StatusErr or Lose.
type Fake struct {
	mu sync.Mutex

	// BackupErr, when set, is returned by every Backup call (no job is
	// created).
	BackupErr error
	// StatusErr, when set, is returned by every GetBackupJobStatus call.
	StatusErr error
	// ListErr, when set, is returned by every GetBackups call.
	ListErr error

	jobs        map[string]*multiadminpb.GetBackupJobStatusResponse
	lost        map[string]bool
	backups     []*multiadminpb.BackupInfo
	nextID      int
	backupCalls []*multiadminpb.BackupRequest
	statusCalls []*multiadminpb.GetBackupJobStatusRequest
}

var _ Client = (*Fake)(nil)

// NewFake returns an empty Fake.
func NewFake() *Fake {
	return &Fake{
		jobs: make(map[string]*multiadminpb.GetBackupJobStatusResponse),
		lost: make(map[string]bool),
	}
}

// Backup implements Client.
func (f *Fake) Backup(
	_ context.Context,
	req *multiadminpb.BackupRequest,
	_ ...grpc.CallOption,
) (*multiadminpb.BackupResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backupCalls = append(f.backupCalls, proto.Clone(req).(*multiadminpb.BackupRequest))
	if f.BackupErr != nil {
		return nil, f.BackupErr
	}
	f.nextID++
	id := fmt.Sprintf("job-%d", f.nextID)
	f.jobs[id] = &multiadminpb.GetBackupJobStatusResponse{
		JobId:      id,
		JobType:    multiadminpb.JobType_JOB_TYPE_BACKUP,
		Status:     multiadminpb.JobStatus_JOB_STATUS_PENDING,
		Database:   req.Database,
		TableGroup: req.TableGroup,
		Shard:      req.Shard,
		BackupType: req.Type,
	}
	return &multiadminpb.BackupResponse{JobId: id}, nil
}

// GetBackupJobStatus implements Client. A lost job returns NotFound, like
// multiadmin after a restart when the pooler has no record of it.
func (f *Fake) GetBackupJobStatus(
	_ context.Context,
	req *multiadminpb.GetBackupJobStatusRequest,
	_ ...grpc.CallOption,
) (*multiadminpb.GetBackupJobStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls = append(
		f.statusCalls,
		proto.Clone(req).(*multiadminpb.GetBackupJobStatusRequest),
	)
	if f.StatusErr != nil {
		return nil, f.StatusErr
	}
	job, ok := f.jobs[req.JobId]
	if !ok || f.lost[req.JobId] {
		return nil, status.Errorf(codes.NotFound, "job %s not found", req.JobId)
	}
	return proto.Clone(job).(*multiadminpb.GetBackupJobStatusResponse), nil
}

// SetJob sets a job's status, backup ID and error message.
func (f *Fake) SetJob(id string, s multiadminpb.JobStatus, backupID, errMsg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		job = &multiadminpb.GetBackupJobStatusResponse{JobId: id}
		f.jobs[id] = job
	}
	job.Status = s
	job.BackupId = backupID
	job.ErrorMessage = errMsg
}

// GetBackups implements Client. It lists the backups added with SetBackup or
// CompleteJob for the request's database and table group.
func (f *Fake) GetBackups(
	_ context.Context,
	req *multiadminpb.GetBackupsRequest,
	_ ...grpc.CallOption,
) (*multiadminpb.GetBackupsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	resp := &multiadminpb.GetBackupsResponse{}
	for _, b := range f.backups {
		if b.Database == req.Database && b.TableGroup == req.TableGroup {
			resp.Backups = append(resp.Backups, proto.Clone(b).(*multiadminpb.BackupInfo))
		}
	}
	return resp, nil
}

// SetBackup adds a backup to the listing, replacing any with the same ID.
func (f *Fake) SetBackup(info *multiadminpb.BackupInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, b := range f.backups {
		if b.BackupId == info.BackupId {
			f.backups[i] = proto.Clone(info).(*multiadminpb.BackupInfo)
			return
		}
	}
	f.backups = append(f.backups, proto.Clone(info).(*multiadminpb.BackupInfo))
}

// CompleteJob marks a job COMPLETED with backupID and lists that backup as
// COMPLETE with full metadata, as multiadmin does after a successful backup.
func (f *Fake) CompleteJob(id, backupID string) {
	f.SetJob(id, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, backupID, "")
	f.mu.Lock()
	job := f.jobs[id]
	f.mu.Unlock()
	f.SetBackup(CompleteBackupInfo(job.Database, job.TableGroup, job.Shard, id, backupID))
}

// FakeBackupSizeBytes is the size CompleteBackupInfo reports.
const FakeBackupSizeBytes = 123456789

// CompleteBackupInfo returns a COMPLETE listing entry with every metadata
// field set.
func CompleteBackupInfo(
	database, tableGroup, shard, jobID, backupID string,
) *multiadminpb.BackupInfo {
	start := time.Date(2026, 11, 7, 4, 0, 2, 0, time.UTC)
	return &multiadminpb.BackupInfo{
		BackupId:        backupID,
		Database:        database,
		TableGroup:      tableGroup,
		Shard:           shard,
		Type:            "full",
		Status:          multiadminpb.BackupStatus_BACKUP_STATUS_COMPLETE,
		BackupSizeBytes: FakeBackupSizeBytes,
		StartLsn:        "0/3000028",
		StopLsn:         "0/3000138",
		PgVersion:       "17",
		StartTimestamp:  timestamppb.New(start),
		StopTimestamp:   timestamppb.New(start.Add(41 * time.Minute)),
		JobId:           jobID,
	}
}

// Lose makes a job answer NotFound.
func (f *Fake) Lose(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lost[id] = true
}

// BackupCalls returns a copy of every Backup request received.
func (f *Fake) BackupCalls() []*multiadminpb.BackupRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*multiadminpb.BackupRequest(nil), f.backupCalls...)
}

// StatusCalls returns a copy of every GetBackupJobStatus request received.
func (f *Fake) StatusCalls() []*multiadminpb.GetBackupJobStatusRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*multiadminpb.GetBackupJobStatusRequest(nil), f.statusCalls...)
}

// FakeFactory returns the same Fake for every cluster.
type FakeFactory struct{ Fake *Fake }

// ClientFor implements Factory.
func (f FakeFactory) ClientFor(
	context.Context,
	*multigresv1alpha1.MultigresCluster,
) (Client, error) {
	return f.Fake, nil
}
