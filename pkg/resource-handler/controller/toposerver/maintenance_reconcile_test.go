package toposerver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/multigres/testkit/etcdsim"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

func TestReconcileRepairsMaintenanceDependencies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		age        time.Duration
		tls        bool
		unhealthy  bool
		wantActive bool
	}{
		{name: "operation still running", age: time.Minute, wantActive: true},
		{name: "TLS operation still running", age: time.Minute, tls: true, wantActive: true},
		{name: "member still unhealthy", age: 3 * time.Minute, unhealthy: true, wantActive: true},
		{name: "member recovered", age: 3 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ts, etcd := maintenanceFixture(t)
			if err := policyv1.AddToScheme(r.Scheme); err != nil {
				t.Fatal(err)
			}
			key := client.ObjectKeyFromObject(ts)
			before := &appsv1.StatefulSet{}
			if err := r.Get(t.Context(), key, before); err != nil {
				t.Fatal(err)
			}
			if tc.tls {
				ts.Spec.TLS = &multigresv1alpha1.TopoTLSConfig{
					Enabled: ptr.To(true), IssuerName: "topology-issuer",
				}
				if err := r.Update(t.Context(), ts); err != nil {
					t.Fatal(err)
				}
				desired, err := BuildStatefulSet(ts, r.Scheme)
				if err != nil {
					t.Fatal(err)
				}
				before.Spec = desired.Spec
				if err := r.Update(t.Context(), before); err != nil {
					t.Fatal(err)
				}
			}
			ts.Status.EtcdMaintenance = &multigresv1alpha1.EtcdMaintenanceStatus{
				LastAttemptTime: metav1.NewTime(time.Now().Add(-tc.age)),
				Endpoint:        maintenanceEndpoints(ts)[0],
				InProgress:      true,
			}
			if err := r.Status().Update(t.Context(), ts); err != nil {
				t.Fatal(err)
			}
			ts.Spec.Etcd.Image = "etcd:pending-rollout"
			if err := r.Update(t.Context(), ts); err != nil {
				t.Fatal(err)
			}

			healthErr := errors.New("member is still unavailable")
			if tc.unhealthy {
				etcd.healthErr = healthErr
			}
			connectionAttempts := 0
			r.MaintenanceClientFactory = func(ctx context.Context, current *multigresv1alpha1.TopoServer) (EtcdMaintenanceClient, error) {
				connectionAttempts++
				svc := &corev1.Service{}
				serviceKey := client.ObjectKey{
					Namespace: current.Namespace,
					Name:      current.Name + "-headless",
				}
				if err := r.APIReader.Get(ctx, serviceKey, svc); err != nil {
					return nil, fmt.Errorf("member DNS requires the headless Service: %w", err)
				}
				return etcd, nil
			}

			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			if tc.unhealthy {
				if !errors.Is(err, healthErr) {
					t.Errorf("expected member health error, got %v", err)
				}
			} else if err != nil {
				t.Errorf("Reconcile() error = %v", err)
			}
			if tc.wantActive && result.RequeueAfter != statusRecheckDelay {
				t.Errorf("requeue = %v, want %v", result.RequeueAfter, statusRecheckDelay)
			}
			for _, name := range []string{ts.Name + "-headless", ts.Name} {
				svc := &corev1.Service{}
				if err := r.Get(
					t.Context(),
					client.ObjectKey{Namespace: ts.Namespace, Name: name},
					svc,
				); err != nil {
					t.Errorf("maintenance blocked Service repair: %v", err)
					continue
				}
				if !metav1.IsControlledBy(svc, ts) {
					t.Errorf("Service %s is not owned by the TopoServer", name)
				}
				if name == ts.Name+"-headless" &&
					(svc.Spec.ClusterIP != corev1.ClusterIPNone || !svc.Spec.PublishNotReadyAddresses) {
					t.Error("headless Service does not publish member DNS during recovery")
				}
			}
			if tc.tls {
				cert, err := certs.Get(
					t.Context(),
					r.Client,
					ts.Namespace,
					multigresv1alpha1.TopoServerCertName(ts.Name),
				)
				if err != nil || cert == nil {
					t.Errorf(
						"maintenance blocked Certificate repair: certificate=%v error=%v",
						cert,
						err,
					)
				}
			}

			fresh := &multigresv1alpha1.TopoServer{}
			if err := r.Get(t.Context(), key, fresh); err != nil {
				t.Fatal(err)
			}
			if fresh.Status.EtcdMaintenance == nil ||
				fresh.Status.EtcdMaintenance.InProgress != tc.wantActive {
				t.Errorf(
					"maintenance state = %+v, want active=%v",
					fresh.Status.EtcdMaintenance,
					tc.wantActive,
				)
			}
			after := &appsv1.StatefulSet{}
			if err := r.Get(t.Context(), key, after); err != nil {
				t.Fatal(err)
			}
			if tc.wantActive {
				if diff := cmp.Diff(before.Spec, after.Spec); diff != "" {
					t.Errorf("StatefulSet changed during maintenance (-before +after):\n%s", diff)
				}
			} else if after.Spec.Template.Spec.Containers[0].Image != string(ts.Spec.Etcd.Image) {
				t.Error("StatefulSet update did not resume after maintenance recovery")
			}
			if (connectionAttempts > 0) != (tc.age >= maintenanceTimeout) {
				t.Errorf("maintenance connection attempts = %d", connectionAttempts)
			}
			if len(etcd.defragged) != 0 {
				t.Error("recovery started another defragmentation")
			}
		})
	}
}

func TestMaintenanceClientFactoryIsUsed(t *testing.T) {
	r, ts, _ := maintenanceFixture(t)
	stub := &fakeEtcdMaintenance{}
	var seen *multigresv1alpha1.TopoServer
	r.MaintenanceClientFactory = func(
		_ context.Context,
		current *multigresv1alpha1.TopoServer,
	) (EtcdMaintenanceClient, error) {
		seen = current
		return stub, nil
	}
	got, err := r.maintenanceClient(t.Context(), ts)
	if err != nil {
		t.Fatal(err)
	}
	if got != EtcdMaintenanceClient(stub) || seen != ts {
		t.Fatalf("factory not used: got %v, factory saw %v", got, seen)
	}

	want := errors.New("no sim registered")
	r.MaintenanceClientFactory = func(
		context.Context,
		*multigresv1alpha1.TopoServer,
	) (EtcdMaintenanceClient, error) {
		return nil, want
	}
	if _, err := r.maintenanceClient(t.Context(), ts); !errors.Is(err, want) {
		t.Fatalf("factory error not returned: %v", err)
	}
}

// A nil factory must build real per-member clients, one per endpoint, never
// the injected path.
func TestNilMaintenanceClientFactoryBuildsRealClients(t *testing.T) {
	r, ts, _ := maintenanceFixture(t)
	r.MaintenanceClientFactory = nil
	c, err := r.maintenanceClient(t.Context(), ts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mc, ok := c.(*memberClients)
	if !ok {
		t.Fatalf("nil factory returned %T, want *memberClients", c)
	}
	if got, want := len(mc.clients), len(maintenanceEndpoints(ts)); got != want {
		t.Fatalf("real clients = %d, want %d", got, want)
	}
}

// Each etcdsim fault knob, driven through the real maintenance reconcile, must
// meet the operator's matching refusal. It is the guard that keeps the
// simulator's behaviour in step with maintenance.go: if a refusal's text moves
// or a knob stops tripping it, this fails.
func TestEtcdSimFaultsMeetOperatorRefusals(t *testing.T) {
	injected := errors.New("injected")
	for _, tc := range []struct {
		name      string
		build     func(endpoints []string) *etcdsim.Cluster
		fault     func(*etcdsim.Cluster)
		wantError string
		wantCause error
		// wantAt is the index of the endpoint the refusal must name, nil when
		// it names none. Without it an alarm row passes on a malformed status
		// from any other member; the len(s.Errors) check is what pins the arm.
		wantAt    *int
		wantMoved int
	}{
		{
			name:      "membership differs",
			fault:     func(c *etcdsim.Cluster) { c.RemoveMember("etcd-2") },
			wantError: "etcd membership differs from expected replicas",
		},
		{
			name: "learner is an unready voting member",
			build: func(endpoints []string) *etcdsim.Cluster {
				members := make([]etcdsim.Member, len(endpoints))
				for i, ep := range endpoints {
					members[i] = etcdsim.Member{
						ID: uint64(i + 1), Name: fmt.Sprintf("etcd-%d", i), Endpoint: ep,
						Healthy: true, Learner: i == 1, DBSize: etcdsim.DefaultDBSize,
					}
				}
				return etcdsim.New(members...)
			},
			wantError: "etcd membership includes an unready voting member",
		},
		{
			name:      "unhealthy member fails the linearizable read",
			fault:     func(c *etcdsim.Cluster) { c.SetHealthy("etcd-1", false) },
			wantError: "linearizable read",
		},
		{
			name:      "alarm",
			fault:     func(c *etcdsim.Cluster) { c.SetAlarm("etcd-1", "NOSPACE") },
			wantError: "is not healthy",
			wantAt:    new(1),
		},
		{
			name:      "one-shot status RPC failure",
			fault:     func(c *etcdsim.Cluster) { c.FailNext("Status", injected) },
			wantError: "status: injected",
			wantCause: injected,
			wantAt:    new(0),
		},
		{
			name:      "members disagree on leader",
			fault:     func(c *etcdsim.Cluster) { c.SplitLeaderView("etcd-1", 2) },
			wantError: "etcd members disagree on leader or cluster identity",
		},
		{
			name: "leader is not an expected member",
			fault: func(c *etcdsim.Cluster) {
				for _, n := range []string{"etcd-0", "etcd-1", "etcd-2"} {
					c.SplitLeaderView(n, 99)
				}
			},
			wantError: "etcd leader is not an expected member",
		},
		{
			name:      "leadership transfer does not complete",
			fault:     func(c *etcdsim.Cluster) { c.StallLeaderMove(true) },
			wantError: "etcd leadership transfer has not completed",
			wantMoved: 1,
		},
		{
			name:      "one-shot membership RPC failure",
			fault:     func(c *etcdsim.Cluster) { c.FailNext("Members", injected) },
			wantError: "injected",
			wantCause: injected,
		},
		{
			name:      "one-shot defragment failure",
			fault:     func(c *etcdsim.Cluster) { c.FailNext("Defragment", injected) },
			wantError: "defragmenting",
			wantCause: injected,
			wantMoved: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ts, _ := maintenanceFixture(t)
			endpoints := MaintenanceEndpoints(ts)
			sim := etcdsim.NewHealthyAt(endpoints...)
			if tc.build != nil {
				sim = tc.build(endpoints)
			}
			if tc.fault != nil {
				tc.fault(sim)
			}
			r.MaintenanceClientFactory = func(
				context.Context,
				*multigresv1alpha1.TopoServer,
			) (EtcdMaintenanceClient, error) {
				return sim.Client(), nil
			}
			err := r.reconcileMaintenance(t.Context(), ts)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected %q, got %v", tc.wantError, err)
			}
			if tc.wantAt != nil && !strings.Contains(err.Error(), endpoints[*tc.wantAt]) {
				t.Errorf("error %q does not name endpoint %s", err, endpoints[*tc.wantAt])
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Errorf("error %v does not wrap %v", err, tc.wantCause)
			}
			if got := len(sim.MoveLeaderLog()); got != tc.wantMoved {
				t.Errorf("leader transfers = %d, want %d", got, tc.wantMoved)
			}
			if tc.name != "one-shot defragment failure" && len(sim.DefragLog()) != 0 {
				t.Errorf("defragmented despite refusal: %v", sim.DefragLog())
			}
		})
	}
}

func TestEtcdSimHealthyClusterDefragments(t *testing.T) {
	r, ts, _ := maintenanceFixture(t)
	endpoints := MaintenanceEndpoints(ts)
	sim := etcdsim.NewHealthyAt(endpoints...)
	r.MaintenanceClientFactory = func(
		context.Context,
		*multigresv1alpha1.TopoServer,
	) (EtcdMaintenanceClient, error) {
		return sim.Client(), nil
	}
	if err := r.reconcileMaintenance(t.Context(), ts); err != nil {
		t.Fatal(err)
	}
	if got := sim.DefragLog(); len(got) != 1 || got[0] != endpoints[0] {
		t.Fatalf("defrag log = %v, want [%s]", got, endpoints[0])
	}
	if got := sim.MoveLeaderLog(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("leader transfers = %v, want [2]", got)
	}
}
