package backuphealth_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
)

func TestEvaluateBackups_Healthy(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	recentID := time.Now().Add(-1 * time.Hour).Format("20060102-150405")
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: recentID,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if !result.Healthy {
		t.Errorf("expected healthy, got message: %s", result.Message)
	}
	if result.LastBackupType != "full" {
		t.Errorf("expected type=full, got %s", result.LastBackupType)
	}
	if result.LastBackupTime == nil {
		t.Error("expected LastBackupTime to be set")
	}
}

func TestEvaluateBackups_Stale(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	oldID := time.Now().Add(-48 * time.Hour).Format("20060102-150405")
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: oldID,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "diff",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if result.Healthy {
		t.Error("expected unhealthy for 48h-old backup")
	}
	if result.LastBackupType != "diff" {
		t.Errorf("expected type=diff, got %s", result.LastBackupType)
	}
}

func TestEvaluateBackups_NoBackups(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{}
	result := backuphealth.EvaluateBackups(shard, nil)
	if result.Healthy {
		t.Error("expected unhealthy when no backups")
	}
	if result.Message != "No backups found" {
		t.Errorf("unexpected message: %s", result.Message)
	}
}

func TestEvaluateBackups_NoCompleted(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{}
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: "20260224-120000",
			Status:   multipoolermanagerdata.BackupMetadata_INCOMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if result.Healthy {
		t.Error("expected unhealthy when no completed backups")
	}
	if result.Message != "No completed backups found" {
		t.Errorf("unexpected message: %s", result.Message)
	}
}

func TestEvaluateBackups_SelectsMostRecent(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	recentID := time.Now().Add(-1 * time.Hour).Format("20060102-150405")
	olderID := time.Now().Add(-5 * time.Hour).Format("20060102-150405")
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: olderID,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentID,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "incr",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if result.LastBackupType != "incr" {
		t.Errorf("expected most recent backup type=incr, got %s", result.LastBackupType)
	}
}

func TestEvaluateBackups_DifferentialOnOldFull(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	// Weekly full, daily differentials: the full is five days old and the
	// differential on top of it is one hour old. The shard has a fresh backup.
	fullLabel := time.Now().Add(-5*24*time.Hour).UTC().Format("20060102-150405") + "F"
	diffTime := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)
	diffLabel := fullLabel + "_" + diffTime.Format("20060102-150405") + "D"

	tests := map[string]*multipoolermanagerdata.BackupMetadata{
		"with stop timestamp": {
			BackupId:      diffLabel,
			Status:        multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:          "diff",
			StopTimestamp: timestamppb.New(diffTime.Add(3 * time.Second)),
		},
		"without stop timestamp": {
			BackupId: diffLabel,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "diff",
		},
	}

	for name, diff := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backups := []*multipoolermanagerdata.BackupMetadata{
				diff,
				{
					BackupId: fullLabel,
					Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
					Type:     "full",
				},
			}

			result := backuphealth.EvaluateBackups(shard, backups)
			if !result.Healthy {
				t.Fatalf("expected healthy for 1h-old differential, got: %s", result.Message)
			}
			if result.LastBackupType != "diff" {
				t.Errorf("expected type=diff, got %s", result.LastBackupType)
			}
			if age := time.Since(result.LastBackupTime.Time); age > 2*time.Hour {
				t.Errorf("expected LastBackupTime from the differential, got age %s", age)
			}
		})
	}
}

func TestEvaluateBackups_PrefersStopTimestamp(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{}
	stop := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			// The label says 48h ago; the stop timestamp is authoritative.
			BackupId:      time.Now().Add(-48*time.Hour).UTC().Format("20060102-150405") + "F",
			Status:        multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:          "full",
			StopTimestamp: timestamppb.New(stop),
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if !result.Healthy {
		t.Fatalf("expected healthy, got: %s", result.Message)
	}
	if !result.LastBackupTime.Time.Equal(stop) {
		t.Errorf("LastBackupTime = %v, want %v", result.LastBackupTime.Time, stop)
	}
}

func TestEvaluateBackups_SkipsUnparseableWhenAnotherIsUsable(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{}
	recentID := time.Now().Add(-1*time.Hour).UTC().Format("20060102-150405") + "F"
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: "not-a-timestamp",
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentID,
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if !result.Healthy {
		t.Fatalf("expected healthy, got: %s", result.Message)
	}
}

func TestApply(t *testing.T) {
	t.Parallel()

	t.Run("sets healthy condition", func(t *testing.T) {
		t.Parallel()

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Generation: 5},
		}
		now := metav1.Now()
		result := &backuphealth.Result{
			Healthy:        true,
			LastBackupTime: &now,
			LastBackupType: "full",
			Message:        "backup is healthy",
		}

		backuphealth.Apply(shard, result)

		if len(shard.Status.Conditions) != 1 {
			t.Fatalf("expected 1 condition, got %d", len(shard.Status.Conditions))
		}
		c := shard.Status.Conditions[0]
		if c.Type != backuphealth.ConditionHealthy {
			t.Errorf(
				"expected condition type %s, got %s",
				backuphealth.ConditionHealthy,
				c.Type,
			)
		}
		if c.Status != metav1.ConditionTrue {
			t.Errorf("expected True, got %s", c.Status)
		}
		if c.Reason != "BackupRecent" {
			t.Errorf("expected reason BackupRecent, got %s", c.Reason)
		}
		if shard.Status.LastBackupType != "full" {
			t.Errorf("expected LastBackupType=full, got %s", shard.Status.LastBackupType)
		}
	})

	t.Run("sets unhealthy condition", func(t *testing.T) {
		t.Parallel()

		shard := &multigresv1alpha1.Shard{}
		result := &backuphealth.Result{
			Healthy: false,
			Message: "backup is stale",
		}

		backuphealth.Apply(shard, result)

		if len(shard.Status.Conditions) != 1 {
			t.Fatalf("expected 1 condition, got %d", len(shard.Status.Conditions))
		}
		c := shard.Status.Conditions[0]
		if c.Status != metav1.ConditionFalse {
			t.Errorf("expected False, got %s", c.Status)
		}
		if c.Reason != "BackupStale" {
			t.Errorf("expected reason BackupStale, got %s", c.Reason)
		}
	})

	t.Run("nil result is no-op", func(t *testing.T) {
		t.Parallel()

		shard := &multigresv1alpha1.Shard{}
		backuphealth.Apply(shard, nil)
		if len(shard.Status.Conditions) != 0 {
			t.Error("expected no conditions for nil result")
		}
	})
}

func TestParseTime(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"valid": {
			input: "20260224-143055",
			want:  time.Date(2026, 2, 24, 14, 30, 55, 0, time.UTC),
		},
		"with suffix": {
			input: "20260224-143055F123456",
			want:  time.Date(2026, 2, 24, 14, 30, 55, 0, time.UTC),
		},
		"differential uses its own time, not the base full's": {
			input: "20261031-113826F_20261104-113819D",
			want:  time.Date(2026, 11, 4, 11, 38, 19, 0, time.UTC),
		},
		"incremental uses its own time, not the base backup's": {
			input: "20261031-113826F_20261105-020000I",
			want:  time.Date(2026, 11, 5, 2, 0, 0, 0, time.UTC),
		},
		"empty segment after underscore": {input: "20261031-113826F_", want: time.Time{}},
		"too short":                      {input: "20260224", want: time.Time{}},
		"empty":                          {input: "", want: time.Time{}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := backuphealth.ParseTime(tc.input)
			if !got.Equal(tc.want) {
				t.Errorf("ParseTime(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseTime_InvalidFormat(t *testing.T) {
	t.Parallel()
	got := backuphealth.ParseTime("ABCDEFG-HIJKLMN")
	if !got.IsZero() {
		t.Errorf("expected zero time for invalid format, got %v", got)
	}
}

func TestEvaluateBackups_MalformedBackupID(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: "not-a-timestamp",
			Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	if result.Healthy {
		t.Error("expected unhealthy for malformed backup ID")
	}
	if result.LastBackupTime != nil {
		t.Errorf("expected nil LastBackupTime, got %v", result.LastBackupTime)
	}
}

type mockTopoStore struct {
	topoclient.Store
	getMultipoolersByCellFunc func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error)
}

func (m *mockTopoStore) GetMultipoolersByCell(
	ctx context.Context,
	cellName string,
	opt *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	if m.getMultipoolersByCellFunc != nil {
		return m.getMultipoolersByCellFunc(ctx, cellName, opt)
	}
	return nil, nil
}

type mockMultipoolerClient struct {
	rpcclient.MultipoolerClient
	getBackupsFunc func(ctx context.Context, pooler *clustermetadata.Multipooler, in *multipoolermanagerdata.GetBackupsRequest) (*multipoolermanagerdata.GetBackupsResponse, error)
}

func (m *mockMultipoolerClient) GetBackups(
	ctx context.Context,
	pooler *clustermetadata.Multipooler,
	in *multipoolermanagerdata.GetBackupsRequest,
) (*multipoolermanagerdata.GetBackupsResponse, error) {
	if m.getBackupsFunc != nil {
		return m.getBackupsFunc(ctx, pooler, in)
	}
	return nil, nil
}

func TestEvaluate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	t.Run("No primary found", func(t *testing.T) {
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil
			},
		}
		rpc := &mockMultipoolerClient{}

		res, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res != nil {
			t.Errorf("expected nil result, got %v", res)
		}
	})

	t.Run("Primary found but GetBackups fails", func(t *testing.T) {
		primaryInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id: &clustermetadata.ID{Name: "primary-1"},
				RoutingState: &clustermetadata.RoutingState{
					Role: clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
				},
			},
		}
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primaryInfo}, nil
			},
		}
		rpc := &mockMultipoolerClient{
			getBackupsFunc: func(ctx context.Context, pooler *clustermetadata.Multipooler, in *multipoolermanagerdata.GetBackupsRequest) (*multipoolermanagerdata.GetBackupsResponse, error) {
				return nil, fmt.Errorf("fake rpc error")
			},
		}

		_, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("Primary found and EvaluateBackups runs", func(t *testing.T) {
		primaryInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id: &clustermetadata.ID{Name: "primary-1"},
				RoutingState: &clustermetadata.RoutingState{
					Role: clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
				},
			},
		}
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primaryInfo}, nil
			},
		}

		recentID := time.Now().Add(-1 * time.Hour).Format("20060102-150405")
		rpc := &mockMultipoolerClient{
			getBackupsFunc: func(ctx context.Context, pooler *clustermetadata.Multipooler, in *multipoolermanagerdata.GetBackupsRequest) (*multipoolermanagerdata.GetBackupsResponse, error) {
				return &multipoolermanagerdata.GetBackupsResponse{
					Backups: []*multipoolermanagerdata.BackupMetadata{
						{
							BackupId: recentID,
							Status:   multipoolermanagerdata.BackupMetadata_COMPLETE,
							Type:     "full",
						},
					},
				}, nil
			},
		}

		res, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected result, got nil")
		}
		if !res.Healthy {
			t.Errorf("expected healthy true, got false")
		}
	})

	t.Run("FindPrimaryPooler error", func(t *testing.T) {
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("fake topo list error")
			},
		}
		rpc := &mockMultipoolerClient{}

		_, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		if err == nil {
			t.Errorf("expected find pooler error")
		}
	})
}
