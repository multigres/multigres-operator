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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"

	"github.com/multigres/testkit/assert"
)

func TestEvaluateBackups_Healthy(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

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
	c.Eq("full", result.LastBackupType, "expected type=full, got")
	c.NotNil(result.LastBackupTime, "expected LastBackupTime to be set")
}

func TestEvaluateBackups_Stale(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

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
	c.False(result.Healthy, "expected unhealthy for 48h-old backup")
	c.Eq("diff", result.LastBackupType, "expected type=diff, got")
}

func TestEvaluateBackups_NoBackups(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	shard := &multigresv1alpha1.Shard{}
	result := backuphealth.EvaluateBackups(shard, nil)
	c.False(result.Healthy, "expected unhealthy when no backups")
	c.Eq("No backups found", result.Message, "unexpected message")
}

func TestEvaluateBackups_NoCompleted(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	shard := &multigresv1alpha1.Shard{}
	backups := []*multipoolermanagerdata.BackupMetadata{
		{
			BackupId: "20260224-120000",
			Status:   multipoolermanagerdata.BackupMetadata_INCOMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackups(shard, backups)
	c.False(result.Healthy, "expected unhealthy when no completed backups")
	c.Eq("No completed backups found", result.Message, "unexpected message")
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
	assert.NewCollecting(t).
		Eq("incr", result.LastBackupType, "expected most recent backup type=incr, got")
}

func TestApply(t *testing.T) {
	t.Parallel()

	t.Run("sets healthy condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewCollecting(t)

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

		ck.Require().
			Len(shard.Status.Conditions, 1, "expected 1 condition, got %d", len(shard.Status.Conditions))
		c := shard.Status.Conditions[0]
		ck.Eq(backuphealth.ConditionHealthy, c.Type, "expected condition type")
		ck.Eq(metav1.ConditionTrue, c.Status, "expected True, got")
		ck.Eq("BackupRecent", c.Reason, "expected reason BackupRecent, got")
		ck.Eq("full", shard.Status.LastBackupType, "expected LastBackupType=full, got")
	})

	t.Run("sets unhealthy condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewCollecting(t)

		shard := &multigresv1alpha1.Shard{}
		result := &backuphealth.Result{
			Healthy: false,
			Message: "backup is stale",
		}

		backuphealth.Apply(shard, result)

		ck.Require().
			Len(shard.Status.Conditions, 1, "expected 1 condition, got %d", len(shard.Status.Conditions))
		c := shard.Status.Conditions[0]
		ck.Eq(metav1.ConditionFalse, c.Status, "expected False, got")
		ck.Eq("BackupStale", c.Reason, "expected reason BackupStale, got")
	})

	t.Run("nil result is no-op", func(t *testing.T) {
		t.Parallel()

		shard := &multigresv1alpha1.Shard{}
		backuphealth.Apply(shard, nil)
		assert.NewCollecting(t).
			Empty(shard.Status.Conditions, "expected no conditions for nil result")
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
		"too short": {input: "20260224", want: time.Time{}},
		"empty":     {input: "", want: time.Time{}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := backuphealth.ParseTime(tc.input)
			assert.NewCollecting(t).
				True(got.Equal(tc.want), "ParseTime(%q) = %v, want %v", tc.input, got, tc.want)
		})
	}
}

func TestParseTime_InvalidFormat(t *testing.T) {
	t.Parallel()
	got := backuphealth.ParseTime("ABCDEFG-HIJKLMN")
	assert.NewCollecting(t).True(got.IsZero(), "expected zero time for invalid format, got %v", got)
}

func TestEvaluateBackups_MalformedBackupID(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

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
	c.False(result.Healthy, "expected unhealthy for malformed backup ID")
	c.Nil(result.LastBackupTime, "expected nil LastBackupTime, got")
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
		c := assert.NewCollecting(t)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil
			},
		}
		rpc := &mockMultipoolerClient{}

		res, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		c.NoError(err, "unexpected error")
		c.Nil(res, "expected nil result, got")
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
		assert.NewCollecting(t).Error(err, "expected error, got nil")
	})

	t.Run("Primary found and EvaluateBackups runs", func(t *testing.T) {
		c := assert.NewCollecting(t)
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
		c.NoError(err, "unexpected error")
		c.Require().NotNil(res, "expected result, got nil")
		c.True(res.Healthy, "expected healthy true, got false")
	})

	t.Run("FindPrimaryPooler error", func(t *testing.T) {
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("fake topo list error")
			},
		}
		rpc := &mockMultipoolerClient{}

		_, err := backuphealth.Evaluate(ctx, store, rpc, shard)
		assert.NewCollecting(t).Error(err, "expected find pooler error")
	})
}
