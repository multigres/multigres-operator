package multigrescluster

import (
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestResolvedTopoAuthProblem(t *testing.T) {
	authOn := &multigresv1alpha1.TopoServerAuthSpec{Enabled: true}
	authOff := &multigresv1alpha1.TopoServerAuthSpec{Enabled: false}
	managed := func(
		auth *multigresv1alpha1.TopoServerAuthSpec,
	) *multigresv1alpha1.GlobalTopoServerSpec {
		return &multigresv1alpha1.GlobalTopoServerSpec{
			Auth: auth,
			Etcd: &multigresv1alpha1.EtcdSpec{},
		}
	}
	external := &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"https://external-etcd:2379"},
		},
	}
	childRequestedAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
		Spec:       multigresv1alpha1.TopoServerSpec{Auth: authOn},
	}
	childActivatedAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
		Status:     multigresv1alpha1.TopoServerStatus{AuthEnabled: true},
	}
	childWithoutAuth := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-global-topo"},
	}
	withProjectRef := map[string]string{metadata.AnnotationProjectRef: "project"}
	withTLS := topoTLSCluster("cluster", "default", withProjectRef)
	withTLSNoRef := topoTLSCluster("cluster", "default", nil)
	withoutTLS := topoTLSCluster("cluster", "default", withProjectRef)
	withoutTLS.Spec.TopoTLS = nil
	withoutTLSNoRef := topoTLSCluster("cluster", "default", nil)
	withoutTLSNoRef.Spec.TopoTLS = nil

	tests := map[string]struct {
		cluster      *multigresv1alpha1.MultigresCluster
		resolved     *multigresv1alpha1.GlobalTopoServerSpec
		existing     *multigresv1alpha1.TopoServer
		wantReason   string
		wantInAdvice []string
	}{
		"auth with topology TLS": {
			cluster:  withTLS,
			resolved: managed(authOn),
		},
		"auth with topology TLS on a child that already has it": {
			cluster:  withTLS,
			resolved: managed(authOn),
			existing: childActivatedAuth,
		},
		"auth without topology TLS": {
			cluster:    withoutTLS,
			resolved:   managed(authOn),
			wantReason: reasonTopologyAuthRequiresTLS,
			wantInAdvice: []string{
				"spec.globalTopoServer.auth", "spec.topoTLS", "CoreTemplate",
			},
		},
		"auth without a project ref": {
			cluster:    withTLSNoRef,
			resolved:   managed(authOn),
			wantReason: reasonTopologyAuthRequiresProjectRef,
			wantInAdvice: []string{
				"spec.globalTopoServer.auth", "CoreTemplate", metadata.AnnotationProjectRef,
			},
		},
		"auth without a project ref on a child that already has it": {
			cluster:    withTLSNoRef,
			resolved:   managed(authOn),
			existing:   childActivatedAuth,
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"auth without topology TLS or a project ref reports TLS first": {
			cluster:    withoutTLSNoRef,
			resolved:   managed(authOn),
			wantReason: reasonTopologyAuthRequiresTLS,
		},
		"no auth without a project ref keeps fallback roots": {
			cluster:  withTLSNoRef,
			resolved: managed(nil),
		},
		"auth disabled without a project ref": {
			cluster:  withTLSNoRef,
			resolved: managed(authOff),
		},
		"external server without a project ref": {
			cluster:  withTLSNoRef,
			resolved: external,
		},
		"auth dropped after the child requested it": {
			cluster:    withTLS,
			resolved:   managed(nil),
			existing:   childRequestedAuth,
			wantReason: reasonTopologyAuthCannotBeDisabled,
			wantInAdvice: []string{
				"spec.globalTopoServer.auth", "CoreTemplate", "cluster-global-topo",
			},
		},
		"auth set to false after the child activated it": {
			cluster:    withTLS,
			resolved:   managed(authOff),
			existing:   childActivatedAuth,
			wantReason: reasonTopologyAuthCannotBeDisabled,
		},
		"move to external from a child with auth": {
			cluster:  withTLS,
			resolved: external,
			existing: childRequestedAuth,
		},
		"no child yet": {
			cluster:  withTLS,
			resolved: managed(nil),
		},
		"auth never requested": {
			cluster:  withoutTLS,
			resolved: managed(nil),
			existing: childWithoutAuth,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reason, message := resolvedTopoAuthProblem(tc.cluster, tc.resolved, tc.existing)
			assert.Equal(t, tc.wantReason, reason)
			if tc.wantReason == "" {
				assert.Empty(t, message)
				return
			}
			for _, want := range tc.wantInAdvice {
				assert.Contains(t, message, want)
			}
		})
	}
}

func TestReconcileTemplateAuthBlocksUntilFixed(t *testing.T) {
	tests := map[string]struct {
		prepare    func(*multigresv1alpha1.MultigresCluster)
		fix        func(*multigresv1alpha1.MultigresCluster, *multigresv1alpha1.CoreTemplate)
		wantErr    string
		wantReason string
	}{
		"without topoTLS": {
			prepare: func(c *multigresv1alpha1.MultigresCluster) { c.Spec.TopoTLS = nil },
			fix: func(_ *multigresv1alpha1.MultigresCluster, tpl *multigresv1alpha1.CoreTemplate) {
				tpl.Spec.GlobalTopoServer.Auth = nil
			},
			wantErr:    "spec.topoTLS",
			wantReason: reasonTopologyAuthRequiresTLS,
		},
		"without project ref": {
			prepare: func(*multigresv1alpha1.MultigresCluster) {},
			fix: func(c *multigresv1alpha1.MultigresCluster, _ *multigresv1alpha1.CoreTemplate) {
				c.Annotations = map[string]string{metadata.AnnotationProjectRef: "project"}
			},
			wantErr:    metadata.AnnotationProjectRef,
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := topoTLSCluster("cluster", "default", nil)
			tc.prepare(cluster)
			cluster.Generation = 3
			template := &multigresv1alpha1.CoreTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
				Spec: multigresv1alpha1.CoreTemplateSpec{
					GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
						Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			}
			scheme := setupScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, template).
				WithStatusSubresource(cluster).Build()
			recorder := record.NewFakeRecorder(100)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: recorder,
				CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{Store: newClusterTopologyMemoryStore(t)}, nil
				},
			}
			key := client.ObjectKeyFromObject(cluster)
			childKey := client.ObjectKey{Name: "cluster-global-topo", Namespace: "default"}

			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.ErrorContains(t, err, tc.wantErr)

			require.NoError(t, c.Get(t.Context(), key, cluster))
			assert.Equal(t, multigresv1alpha1.PhaseDegraded, cluster.Status.Phase)
			condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tc.wantReason, condition.Reason)
			assert.Equal(t, cluster.Generation, condition.ObservedGeneration)
			assert.Contains(t, condition.Message, "CoreTemplate")
			assert.Contains(t, drainEvents(recorder), "Warning "+tc.wantReason)

			err = c.Get(t.Context(), childKey, &multigresv1alpha1.TopoServer{})
			assert.True(
				t,
				apierrors.IsNotFound(err),
				"child TopoServer must not be applied, got %v",
				err,
			)

			tc.fix(cluster, template)
			require.NoError(t, c.Update(t.Context(), cluster))
			require.NoError(t, c.Update(t.Context(), template))
			_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			require.NoError(t, c.Get(t.Context(), key, cluster))
			condition = meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionTrue, condition.Status)
			assert.NotContains(t, cluster.Status.Message, tc.wantErr)
			require.NoError(t, c.Get(t.Context(), childKey, &multigresv1alpha1.TopoServer{}))
		})
	}
}

func drainEvents(recorder *record.FakeRecorder) string {
	var events string
	for {
		select {
		case event := <-recorder.Events:
			events += event + "\n"
		default:
			return events
		}
	}
}

func authTopoServer(
	name, namespace string,
	spec multigresv1alpha1.TopoServerSpec,
) *multigresv1alpha1.TopoServer {
	return &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       spec,
	}
}

func TestReferencedAuthManagedTopoServerRequiresProjectRef(t *testing.T) {
	managedAuth := multigresv1alpha1.TopoServerSpec{
		Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
		Etcd: &multigresv1alpha1.EtcdSpec{},
	}
	plain := multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}}
	activated := authTopoServer("activated", "stores", plain)
	activated.Status.AuthEnabled = true

	tests := map[string]struct {
		annotations map[string]string
		endpoints   []multigresv1alpha1.EndpointUrl
		wantReason  string
	}{
		"auth-managed server without a project ref": {
			endpoints:  []multigresv1alpha1.EndpointUrl{"https://shared.stores.svc:2379"},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"server that activated auth without a project ref": {
			endpoints:  []multigresv1alpha1.EndpointUrl{"https://activated.stores.svc:2379"},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"pod endpoint of an auth-managed server": {
			endpoints: []multigresv1alpha1.EndpointUrl{
				"https://shared-0.shared-headless.stores.svc:2379",
			},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"headless Service of an auth-managed server": {
			endpoints:  []multigresv1alpha1.EndpointUrl{"https://shared-headless.stores.svc:2379"},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"auth-managed server named with the headless suffix": {
			endpoints: []multigresv1alpha1.EndpointUrl{
				"https://suffixed-headless.stores.svc:2379",
			},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"pod of an auth-managed server named with the headless suffix": {
			endpoints: []multigresv1alpha1.EndpointUrl{
				"https://suffixed-headless-0.suffixed-headless-headless.stores.svc:2379",
			},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"auth-managed server with a project ref": {
			annotations: map[string]string{metadata.AnnotationProjectRef: "project"},
			endpoints:   []multigresv1alpha1.EndpointUrl{"https://shared.stores.svc:2379"},
		},
		"server without auth keeps fallback roots": {
			endpoints: []multigresv1alpha1.EndpointUrl{"https://plain.stores.svc:2379"},
		},
		"server that does not exist": {
			endpoints: []multigresv1alpha1.EndpointUrl{"https://missing.stores.svc:2379"},
		},
		"external etcd that is not a TopoServer": {
			endpoints: []multigresv1alpha1.EndpointUrl{"https://etcd.example.com:2379"},
		},
		"second endpoint is auth-managed": {
			endpoints: []multigresv1alpha1.EndpointUrl{
				"https://plain.stores.svc:2379", "https://shared.stores.svc:2379",
			},
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := topoTLSCluster("cluster", "default", tc.annotations)
			scheme := setupScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
				authTopoServer("shared", "stores", managedAuth),
				authTopoServer("suffixed-headless", "stores", managedAuth),
				authTopoServer("plain", "stores", plain),
				activated,
			).Build()
			r := &MultigresClusterReconciler{Client: c, Scheme: scheme}

			reason, message := r.projectRefRequiredByReferencedTopo(
				t.Context(),
				cluster,
				tc.endpoints,
			)
			assert.Equal(t, tc.wantReason, reason)
			if tc.wantReason != "" {
				assert.Contains(t, message, metadata.AnnotationProjectRef)
				assert.Contains(t, message, "stores/")
			} else {
				assert.Empty(t, message)
			}
		})
	}
}

func TestReconcileSharedAuthTopoServerWithoutProjectRef(t *testing.T) {
	managedAuth := multigresv1alpha1.TopoServerSpec{
		Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
		Etcd: &multigresv1alpha1.EtcdSpec{},
	}
	external := func(address string) *multigresv1alpha1.GlobalTopoServerSpec {
		return &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{
					multigresv1alpha1.EndpointUrl(address),
				},
			},
		}
	}
	localExternal := func(address string) *multigresv1alpha1.CellInlineSpec {
		return &multigresv1alpha1.CellInlineSpec{
			LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{
						multigresv1alpha1.EndpointUrl(address),
					},
				},
			},
		}
	}

	tests := map[string]struct {
		global     *multigresv1alpha1.GlobalTopoServerSpec
		cellLocal  *multigresv1alpha1.CellInlineSpec
		wantReason string
	}{
		"global endpoint names an auth-managed server": {
			global:     external("https://shared.stores.svc:2379"),
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"cell-local endpoint names an auth-managed server": {
			global:     external("https://plain.stores.svc:2379"),
			cellLocal:  localExternal("https://shared.stores.svc:2379"),
			wantReason: reasonTopologyAuthRequiresProjectRef,
		},
		"servers without auth": {
			global:    external("https://plain.stores.svc:2379"),
			cellLocal: localExternal("https://plain.stores.svc:2379"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := topoTLSCluster("cluster", "default", nil)
			cluster.Generation = 2
			cluster.Spec.TopoTLS = nil
			cluster.Spec.GlobalTopoServer = tc.global
			cluster.Spec.Cells = []multigresv1alpha1.CellConfig{{
				Name: "zone1", ZoneID: "usw1-az1", Spec: tc.cellLocal,
			}}
			scheme := setupScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
				cluster,
				authTopoServer("shared", "stores", managedAuth),
				authTopoServer("plain", "stores", multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{},
				}),
			).WithStatusSubresource(cluster).Build()
			recorder := record.NewFakeRecorder(100)
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: recorder,
				CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{Store: newClusterTopologyMemoryStore(t)}, nil
				},
			}
			key := client.ObjectKeyFromObject(cluster)

			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, c.Get(t.Context(), key, cluster))
			condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			if tc.wantReason == "" {
				require.NoError(t, err)
				require.NotNil(t, condition)
				assert.Equal(t, metav1.ConditionTrue, condition.Status)
				return
			}
			require.ErrorContains(t, err, metadata.AnnotationProjectRef)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, tc.wantReason, condition.Reason)
			assert.Equal(t, multigresv1alpha1.PhaseDegraded, cluster.Status.Phase)
			assert.Contains(t, drainEvents(recorder), "Warning "+tc.wantReason)

			cluster.Annotations = map[string]string{metadata.AnnotationProjectRef: "project"}
			require.NoError(t, c.Update(t.Context(), cluster))
			_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			require.NoError(t, c.Get(t.Context(), key, cluster))
			condition = meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionTrue, condition.Status)
		})
	}
}
