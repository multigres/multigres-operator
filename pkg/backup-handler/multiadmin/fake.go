package multiadmin

import (
	"context"
	"fmt"
	"sync"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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

	jobs        map[string]*multiadminpb.GetBackupJobStatusResponse
	lost        map[string]bool
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
