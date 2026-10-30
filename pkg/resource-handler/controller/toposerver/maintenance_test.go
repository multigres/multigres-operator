package toposerver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

type fakeEtcdMaintenance struct {
	statuses           map[string]*clientv3.StatusResponse
	members            []*pb.Member
	defragged          []string
	moved              []uint64
	statusErr          error
	healthErr          error
	defragErr          error
	skipLeaderTransfer bool
	afterDefrag        func()
}

func (f *fakeEtcdMaintenance) Status(
	_ context.Context,
	ep string,
) (*clientv3.StatusResponse, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	return f.statuses[ep], nil
}

func (f *fakeEtcdMaintenance) Members(context.Context) (*clientv3.MemberListResponse, error) {
	return &clientv3.MemberListResponse{Members: f.members}, nil
}
func (f *fakeEtcdMaintenance) Health(context.Context, string) error { return f.healthErr }
func (f *fakeEtcdMaintenance) Defragment(_ context.Context, ep string) error {
	f.defragged = append(f.defragged, ep)
	if f.afterDefrag != nil {
		f.afterDefrag()
	}
	return f.defragErr
}

func (f *fakeEtcdMaintenance) MoveLeader(_ context.Context, _ string, id uint64) error {
	f.moved = append(f.moved, id)
	if f.skipLeaderTransfer {
		return nil
	}
	for _, s := range f.statuses {
		s.Leader = id
	}
	return nil
}
func (f *fakeEtcdMaintenance) Close() {}

func maintenanceFixture(
	t *testing.T,
) (*TopoServerReconciler, *multigresv1alpha1.TopoServer, *fakeEtcdMaintenance) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	ts := certTestTopoServer(nil)
	ts.Generation = 1
	ts.Spec.Etcd.Maintenance = &multigresv1alpha1.EtcdMaintenanceConfig{
		DefragmentationEnabled: ptr.To(true),
	}
	sts, err := BuildStatefulSet(ts, scheme)
	if err != nil {
		t.Fatal(err)
	}
	sts.UID, sts.Generation = "sts-uid", 1
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: 1,
		Replicas:           3,
		ReadyReplicas:      3,
		UpdatedReplicas:    3,
		CurrentRevision:    "rev1",
		UpdateRevision:     "rev1",
	}
	objects := []client.Object{ts, sts}
	f := &fakeEtcdMaintenance{
		statuses: map[string]*clientv3.StatusResponse{},
		members:  []*pb.Member{{ID: 1}, {ID: 2}, {ID: 3}},
	}
	for i, ep := range maintenanceEndpoints(ts) {
		f.statuses[ep] = &clientv3.StatusResponse{
			Header:      &pb.ResponseHeader{ClusterId: 123, MemberId: uint64(i + 1)},
			Leader:      1,
			DbSize:      400 << 20,
			DbSizeInUse: 100 << 20,
		}
		objects = append(objects, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", ts.Name, i),
				Namespace: ts.Namespace,
				Labels:    map[string]string{appsv1.StatefulSetRevisionLabel: "rev1"},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet")),
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{
						Type:               corev1.PodReady,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
					},
				},
			},
		})
	}
	c := newFakeBase(t, scheme, objects...)
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(ts), ts); err != nil {
		t.Fatal(err)
	}
	r := &TopoServerReconciler{
		// APIReader must stay the unwrapped fake: the "reservation conflict"
		// case layers an interceptor over it, which needs a client.WithWatch.
		Client:                   client.WithFieldOwner(c, ownership.ManagerTopoServer),
		APIReader:                c,
		Scheme:                   scheme,
		Recorder:                 record.NewFakeRecorder(100),
		MaintenanceClientFactory: func(context.Context, *multigresv1alpha1.TopoServer) (EtcdMaintenanceClient, error) { return f, nil },
	}
	return r, ts, f
}

func TestEtcdMaintenanceSerializesMembersAndRestarts(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	if err := r.reconcileMaintenance(t.Context(), ts); err != nil {
		t.Fatal(err)
	}
	if len(f.defragged) != 1 || len(f.moved) != 1 {
		t.Fatalf("defrags=%v leader transfers=%v", f.defragged, f.moved)
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Status.EtcdMaintenance == nil || fresh.Status.EtcdMaintenance.InProgress {
		t.Fatal("completed reservation not persisted")
	}
	// A new controller instance, or a stale reconcile, must obey the persisted interval.
	r2 := *r
	if err := r2.reconcileMaintenance(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if len(f.defragged) != 1 {
		t.Fatal("maintenance repeated inside the interval")
	}
}

func TestEtcdMaintenanceHealthGates(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*TopoServerReconciler, *multigresv1alpha1.TopoServer, *fakeEtcdMaintenance)
		wantErr bool
	}{
		{"disabled", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, _ *fakeEtcdMaintenance) {
			ts.Spec.Etcd.Maintenance = nil
		}, false},
		{"single member", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, _ *fakeEtcdMaintenance) {
			ts.Spec.Etcd.Replicas = ptr.To(int32(1))
		}, false},
		{"no fragmentation", func(_ *TopoServerReconciler, _ *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			for _, s := range f.statuses {
				s.DbSizeInUse = s.DbSize
			}
		}, false},
		{"small fragmentation", func(_ *TopoServerReconciler, _ *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			for _, s := range f.statuses {
				s.DbSize = 100 << 20
				s.DbSizeInUse = 10 << 20
			}
		}, false},
		{"low fragmentation ratio", func(_ *TopoServerReconciler, _ *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			for _, s := range f.statuses {
				s.DbSize = 1 << 30
				s.DbSizeInUse = 900 << 20
			}
		}, false},
		{"linearizable read fails", func(_ *TopoServerReconciler, _ *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			f.healthErr = errors.New("no quorum")
		}, true},
		{"leader disagreement", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			f.statuses[maintenanceEndpoints(ts)[1]].Leader = 2
		}, true},
		{"member alarm", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			f.statuses[maintenanceEndpoints(ts)[1]].Errors = []string{"NOSPACE"}
		}, true},
		{"foreign cluster", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			f.statuses[maintenanceEndpoints(ts)[1]].Header.ClusterId = 999
		}, true},
		{"duplicate member", func(_ *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, f *fakeEtcdMaintenance) {
			f.statuses[maintenanceEndpoints(ts)[1]].Header.MemberId = 1
		}, true},
		{"rolling update", func(r *TopoServerReconciler, ts *multigresv1alpha1.TopoServer, _ *fakeEtcdMaintenance) {
			sts := &appsv1.StatefulSet{}
			_ = r.Get(t.Context(), client.ObjectKeyFromObject(ts), sts)
			sts.Status.UpdateRevision = "rev2"
			if err := r.Status().Update(t.Context(), sts); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"reservation conflict", func(r *TopoServerReconciler, _ *multigresv1alpha1.TopoServer, _ *fakeEtcdMaintenance) {
			r.Client = client.WithFieldOwner(interceptor.NewClient(r.APIReader.(client.WithWatch), interceptor.Funcs{SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				return apierrors.NewConflict(multigresv1alpha1.GroupVersion.WithResource("toposervers").GroupResource(), "topo", errors.New("conflict"))
			}}), ownership.ManagerTopoServer)
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, ts, f := maintenanceFixture(t)
			test.mutate(r, ts, f)
			err := r.reconcileMaintenance(t.Context(), ts)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v", err)
			}
			if len(f.defragged) != 0 {
				t.Fatalf("unsafe defragmentation: %v", f.defragged)
			}
		})
	}
}

func TestEtcdMaintenanceRejectsInvalidMemberResponses(t *testing.T) {
	statusErr := errors.New("status RPC unavailable")
	for _, tc := range []struct {
		name      string
		mutate    func(*fakeEtcdMaintenance)
		wantError string
		wantCause error
	}{
		{
			name:      "missing member",
			mutate:    func(f *fakeEtcdMaintenance) { f.members = f.members[:2] },
			wantError: "etcd membership differs from expected replicas",
		},
		{
			name:      "extra member",
			mutate:    func(f *fakeEtcdMaintenance) { f.members = append(f.members, &pb.Member{ID: 4}) },
			wantError: "etcd membership differs from expected replicas",
		},
		{
			name:      "learner member",
			mutate:    func(f *fakeEtcdMaintenance) { f.members[1].IsLearner = true },
			wantError: "etcd membership includes an unready voting member",
		},
		{
			name:      "zero member ID",
			mutate:    func(f *fakeEtcdMaintenance) { f.members[1].ID = 0 },
			wantError: "etcd membership includes an unready voting member",
		},
		{
			name:      "status RPC failure",
			mutate:    func(f *fakeEtcdMaintenance) { f.statusErr = statusErr },
			wantError: "status: status RPC unavailable",
			wantCause: statusErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ts, f := maintenanceFixture(t)
			tc.mutate(f)
			err := r.reconcileMaintenance(t.Context(), ts)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected %q, got %v", tc.wantError, err)
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Errorf("error %v does not wrap %v", err, tc.wantCause)
			}
			if len(f.moved) != 0 || len(f.defragged) != 0 {
				t.Errorf(
					"maintenance changed unhealthy members: transfers=%v defrags=%v",
					f.moved,
					f.defragged,
				)
			}
			fresh := &multigresv1alpha1.TopoServer{}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
				t.Fatal(err)
			}
			if fresh.Status.EtcdMaintenance != nil {
				t.Error("failed health checks created a maintenance reservation")
			}
		})
	}
}

func TestEtcdMaintenanceIncompleteLeadershipTransfer(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	f.skipLeaderTransfer = true
	err := r.reconcileMaintenance(t.Context(), ts)
	if err == nil || err.Error() != "etcd leadership transfer has not completed" {
		t.Fatalf("expected incomplete leadership transfer, got %v", err)
	}
	if len(f.moved) != 1 || f.moved[0] != 2 {
		t.Errorf("leadership transfer attempts = %v, want [2]", f.moved)
	}
	if len(f.defragged) != 0 {
		t.Errorf(
			"defragmented a member whose leadership transfer did not complete: %v",
			f.defragged,
		)
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
		t.Fatal(err)
	}
	state := fresh.Status.EtcdMaintenance
	if state == nil || !state.InProgress || state.Endpoint != maintenanceEndpoints(ts)[0] {
		t.Fatalf("incomplete transfer did not retain the target reservation: %+v", state)
	}
	r2 := *r
	if err := r2.reconcileMaintenance(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if len(f.moved) != 1 || len(f.defragged) != 0 {
		t.Error("maintenance restarted while the transfer reservation remained active")
	}
}

func TestEtcdMaintenanceInterruptedOperation(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	f.defragErr = context.DeadlineExceeded
	if err := r.reconcileMaintenance(t.Context(), ts); err == nil {
		t.Fatal("expected timeout")
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
		t.Fatal(err)
	}
	if !fresh.Status.EtcdMaintenance.InProgress {
		t.Fatal("uncertain operation released its reservation")
	}
	waiting, err := r.resumeMaintenance(t.Context(), fresh)
	if err != nil || !waiting {
		t.Fatalf("resume immediately: waiting=%v err=%v", waiting, err)
	}
	fresh.Status.EtcdMaintenance.LastAttemptTime = metav1.NewTime(time.Now().Add(-3 * time.Minute))
	if err := r.Status().Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	f.healthErr = errors.New("previous member still unavailable")
	if waiting, err = r.resumeMaintenance(t.Context(), fresh); !waiting || err == nil {
		t.Fatalf("unhealthy resume: waiting=%v err=%v", waiting, err)
	}
	f.healthErr = nil
	if waiting, err = r.resumeMaintenance(t.Context(), fresh); waiting || err != nil {
		t.Fatalf("healthy resume: waiting=%v err=%v", waiting, err)
	}
	if err := r.reconcileMaintenance(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if len(f.defragged) != 1 {
		t.Fatal("interrupted operation started another defrag")
	}
}

func TestEtcdMaintenancePostHealthFailureKeepsReservation(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	f.afterDefrag = func() { f.healthErr = errors.New("member unhealthy after defrag") }
	if err := r.reconcileMaintenance(t.Context(), ts); err == nil {
		t.Fatal("expected post-defrag health failure")
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh); err != nil {
		t.Fatal(err)
	}
	if !fresh.Status.EtcdMaintenance.InProgress {
		t.Fatal("failed post-check released reservation")
	}
}
