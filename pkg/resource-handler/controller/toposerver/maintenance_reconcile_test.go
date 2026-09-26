package toposerver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"

	"github.com/multigres/testkit/assert"
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
			c := assert.NewCollecting(t)
			r, ts, etcd := maintenanceFixture(t)
			c.Require().NoError(policyv1.AddToScheme(r.Scheme))
			key := client.ObjectKeyFromObject(ts)
			before := &appsv1.StatefulSet{}
			c.Require().NoError(r.Get(t.Context(), key, before))
			if tc.tls {
				ts.Spec.TLS = &multigresv1alpha1.TopoTLSConfig{
					Enabled: ptr.To(true), IssuerName: "topology-issuer",
				}
				c.Require().NoError(r.Update(t.Context(), ts))
				desired, err := BuildStatefulSet(ts, r.Scheme)
				c.Require().NoError(err)
				before.Spec = desired.Spec
				c.Require().NoError(r.Update(t.Context(), before))
			}
			ts.Status.EtcdMaintenance = &multigresv1alpha1.EtcdMaintenanceStatus{
				LastAttemptTime: metav1.NewTime(time.Now().Add(-tc.age)),
				Endpoint:        maintenanceEndpoints(ts)[0],
				InProgress:      true,
			}
			c.Require().NoError(r.Status().Update(t.Context(), ts))
			ts.Spec.Etcd.Image = "etcd:pending-rollout"
			c.Require().NoError(r.Update(t.Context(), ts))

			healthErr := errors.New("member is still unavailable")
			if tc.unhealthy {
				etcd.healthErr = healthErr
			}
			connectionAttempts := 0
			r.newMaintenanceClient = func(ctx context.Context, current *multigresv1alpha1.TopoServer) (etcdMaintenanceClient, error) {
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
				c.ErrorIs(err, healthErr, "expected member health error, got")
			} else if err != nil {
				t.Errorf("Reconcile() error = %v", err)
			}
			c.False(
				tc.wantActive && result.RequeueAfter != statusRecheckDelay,
				"requeue = %v, want %v",
				result.RequeueAfter,
				statusRecheckDelay,
			)
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
				c.True(
					metav1.IsControlledBy(svc, ts),
					"Service %s is not owned by the TopoServer",
					name,
				)
				c.False(name == ts.Name+"-headless" &&
					(svc.Spec.ClusterIP != corev1.ClusterIPNone || !svc.Spec.PublishNotReadyAddresses), "headless Service does not publish member DNS during recovery")
			}
			if tc.tls {
				cert, err := certs.Get(
					t.Context(),
					r.Client,
					ts.Namespace,
					multigresv1alpha1.TopoServerCertName(ts.Name),
				)
				c.False(
					err != nil || cert == nil,
					"maintenance blocked Certificate repair: certificate=%v error=%v",
					cert,
					err,
				)
			}

			fresh := &multigresv1alpha1.TopoServer{}
			c.Require().NoError(r.Get(t.Context(), key, fresh))
			if fresh.Status.EtcdMaintenance == nil ||
				fresh.Status.EtcdMaintenance.InProgress != tc.wantActive {
				t.Errorf(
					"maintenance state = %+v, want active=%v",
					fresh.Status.EtcdMaintenance,
					tc.wantActive,
				)
			}
			after := &appsv1.StatefulSet{}
			c.Require().NoError(r.Get(t.Context(), key, after))
			if tc.wantActive {
				c.EqDiff(before.Spec, after.Spec, "StatefulSet changed during maintenance")
			} else if after.Spec.Template.Spec.Containers[0].Image != string(ts.Spec.Etcd.Image) {
				t.Error("StatefulSet update did not resume after maintenance recovery")
			}
			c.Eq(
				(tc.age >= maintenanceTimeout),
				(connectionAttempts > 0),
				"maintenance connection attempts = %d",
				connectionAttempts,
			)
			c.Empty(etcd.defragged, "recovery started another defragmentation")
		})
	}
}
