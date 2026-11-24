package toposerver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/multigres/testkit/etcdsim"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const authFixtureOperatorNamespace = "operator-system"

type authGrantMaintenance struct {
	EtcdMaintenanceClient
	*fakeEtcdAuth
}

func (*authGrantMaintenance) Close() {}

func authReconcileFixture(
	t *testing.T,
) (*TopoServerReconciler, *api.TopoServer, *api.MultigresCluster, *fakeEtcdAuth) {
	t.Helper()
	cluster := authGrantCluster("project", "cluster")
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
		Spec: api.TopoServerSpec{
			Auth: &api.TopoServerAuthSpec{Enabled: true},
			Etcd: &api.EtcdSpec{},
			TLS:  &api.TopoTLSConfig{Enabled: ptr.To(true)},
		},
	}
	objects := []client.Object{cluster, ts}
	for _, key := range []client.ObjectKey{
		{Namespace: ts.Namespace, Name: api.TopoServerCertSecretName(ts.Name)},
		topology.AdminSecretKey(authFixtureOperatorNamespace, ts),
	} {
		objects = append(objects, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Data: map[string][]byte{
				corev1.TLSCertKey:       []byte("cert"),
				corev1.TLSPrivateKeyKey: []byte("key"),
				"ca.crt":                []byte("ca"),
			},
		})
	}
	r := authGrantReconciler(t, objects...)
	r.OperatorNamespace = authFixtureOperatorNamespace
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(ts), ts))
	auth := newFakeEtcdAuth()
	r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
		stored := &api.TopoServer{}
		require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(ts), stored))
		require.NotEmpty(
			t,
			stored.Status.AuthIdentities,
			"status.authIdentities must be saved before contacting etcd",
		)
		return &authGrantMaintenance{fakeEtcdAuth: auth}, nil
	}
	return r, ts, cluster, auth
}

func authConfirmationReader(
	t *testing.T,
	r *TopoServerReconciler,
	objects ...client.Object,
) client.Client {
	t.Helper()
	var secrets corev1.SecretList
	require.NoError(t, r.List(t.Context(), &secrets))
	for i := range secrets.Items {
		objects = append(objects, &secrets.Items[i])
	}
	return authGrantReconciler(t, objects...).Client
}

func requireAuthorizationCondition(
	t *testing.T,
	ts *api.TopoServer,
	status metav1.ConditionStatus,
	reason string,
) {
	t.Helper()
	condition := meta.FindStatusCondition(ts.Status.Conditions, "AuthorizationReady")
	require.NotNil(t, condition)
	require.Equal(t, status, condition.Status)
	require.Equal(t, reason, condition.Reason)
	require.Equal(t, ts.Generation, condition.ObservedGeneration)
}

func TestReconcileTopologyAuthWaitsForPreconditions(t *testing.T) {
	managed := api.TopoServerSpec{
		Auth: &api.TopoServerAuthSpec{Enabled: true},
		Etcd: &api.EtcdSpec{},
		TLS:  &api.TopoTLSConfig{Enabled: ptr.To(true)},
	}
	tests := map[string]struct {
		spec       api.TopoServerSpec
		owned      bool
		objects    []client.Object
		wantReady  bool
		wantErr    string
		wantReason string
	}{
		"auth not requested": {
			wantReady:  true,
			wantReason: "ManagementDisabled",
		},
		"no managed etcd with TLS": {
			spec:       api.TopoServerSpec{Auth: &api.TopoServerAuthSpec{Enabled: true}},
			wantErr:    "requires managed etcd with TLS",
			wantReason: "ReconcileFailed",
		},
		"no project identities on an owned server": {
			spec:       managed,
			owned:      true,
			wantErr:    "no project identities",
			wantReason: "ReconcileFailed",
		},
		"credential secrets missing": {
			spec:       managed,
			objects:    []client.Object{authGrantCluster("one", "one")},
			wantReason: "PendingCredentials",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r := authGrantReconciler(t, tt.objects...)
			r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
				t.Fatal("must not contact etcd before grants and credentials are complete")
				return nil, nil
			}
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
				Spec:       tt.spec,
			}
			if tt.owned {
				ts.OwnerReferences = controllerRef(
					"MultigresCluster",
					authGrantCluster("one", "one"),
				)
			}
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.Equal(t, tt.wantReady, ready)
			require.False(t, ts.Status.AuthEnabled)
			requireAuthorizationCondition(t, ts, metav1.ConditionFalse, tt.wantReason)
		})
	}
}

func TestTopologyAuthPrunesRemovedOrMovedProjects(t *testing.T) {
	for _, movement := range []string{"removed", "moved", "interrupted deletion"} {
		t.Run(movement, func(t *testing.T) {
			ctx := context.Background()
			r, ts, cluster, auth := authReconcileFixture(t)
			ready, err := r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			previous := append([]string(nil), ts.Status.AuthIdentities...)
			require.Equal(t, []string{"/multigres/project"}, previous)
			auth.users["unowned-client"] = []string{"unowned-role"}
			auth.roles["unowned-role"] = nil
			if movement == "moved" {
				cluster.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
					"https://other.stores:2379",
				}
				require.NoError(t, r.Update(ctx, cluster))
			} else {
				require.NoError(t, r.Delete(ctx, cluster))
			}
			if movement == "interrupted deletion" {
				auth.failRoleDelete = true
				ready, err = r.reconcileTopologyAuth(ctx, ts)
				require.ErrorContains(t, err, "role deletion interrupted")
				require.False(t, ready)
				require.True(t, auth.enabled)
				stored := &api.TopoServer{}
				require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(ts), stored))
				require.Equal(t, previous, stored.Status.AuthIdentities)
				auth.failRoleDelete = false
			}
			ready, err = r.reconcileTopologyAuth(ctx, ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.True(t, auth.enabled)
			for _, identity := range previous {
				require.NotContains(t, auth.users, identity)
				require.NotContains(t, auth.roles, identity)
			}
			require.Contains(t, auth.users, "root")
			require.Contains(t, auth.users, operatorEtcdUsername)
			require.Contains(t, auth.users, "unowned-client")
			require.Contains(t, auth.roles, "unowned-role")
			require.Empty(t, ts.Status.AuthIdentities)
			stored := &api.TopoServer{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(ts), stored))
			require.Empty(t, stored.Status.AuthIdentities)
		})
	}
}

func TestTopologyAuthPrunesIdentitiesRecordedBeforeFailedGrant(t *testing.T) {
	ctx := context.Background()
	r, ts, cluster, auth := authReconcileFixture(t)
	auth.failGrant = true
	ready, err := r.reconcileTopologyAuth(ctx, ts)
	require.ErrorContains(t, err, "grant failed")
	require.False(t, ready)
	require.False(t, auth.enabled)
	require.Equal(t, []string{"/multigres/project"}, ts.Status.AuthIdentities)
	require.NoError(t, r.Delete(ctx, cluster))
	auth.failGrant = false
	ready, err = r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.Empty(t, ts.Status.AuthIdentities)
	require.Len(t, auth.roles, 1)
	require.Contains(t, auth.roles, "root")
}

func TestTopologyAuthDoesNotPruneForUnknownOwnerKind(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	tracked := slices.Clone(ts.Status.AuthIdentities)
	require.NotEmpty(t, tracked)
	ts.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "example.com/v1",
		Kind:       "UnexpectedOwner",
		Name:       "owner",
		UID:        "owner-uid",
		Controller: ptr.To(true),
	}}
	auth.mutations = nil

	ready, err = r.reconcileTopologyAuth(t.Context(), ts)
	require.ErrorContains(t, err, `unsupported topology server owner kind "UnexpectedOwner"`)
	require.False(t, ready)
	require.Equal(t, tracked, ts.Status.AuthIdentities)
	require.Empty(t, auth.mutations)
}

func TestTopologyAuthRemovalConfirmationFailsBeforeMutation(t *testing.T) {
	for _, scenario := range []string{"no API reader", "no recorded cluster", "cell list error", "template get error", "resolution error"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, first, auth := authReconcileFixture(t)
			second := authGrantCluster("second", "second")
			require.NoError(t, r.Create(t.Context(), second))
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			previous := slices.Clone(ts.Status.AuthIdentities)
			if scenario == "no recorded cluster" {
				ts.Status.AuthIdentityClusters = nil
			}
			previousVersion := ts.ResourceVersion
			require.NoError(t, r.Delete(t.Context(), second))
			if scenario == "template get error" {
				second.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "unavailable"}
			}
			if scenario == "resolution error" {
				second.Spec.Cells = []api.CellConfig{{Name: "zone", CellTemplate: "missing"}}
			}
			authoritativeObjects := []client.Object{first.DeepCopy(), second}
			if scenario == "template get error" {
				authoritativeObjects = append(authoritativeObjects, &api.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name: "second-zone", Namespace: second.Namespace,
						Labels: map[string]string{metadata.LabelMultigresCluster: second.Name},
					},
					Spec: api.CellSpec{GlobalTopoServer: api.GlobalTopoServerRef{
						Address: "shared.stores:2379", RootPath: "/multigres/second/global",
					}},
				})
			}
			authoritative := authConfirmationReader(t, r, authoritativeObjects...)
			secretReads, journalPatches, maintenanceClients := 0, 0, 0
			onGet := func(key client.ObjectKey) error {
				if key.Name == api.TopoServerCertSecretName(ts.Name) ||
					key.Name == topology.AdminSecretKey(authFixtureOperatorNamespace, ts).Name {
					secretReads++
				}
				if key.Name == "unavailable" {
					return errors.New("authoritative template unavailable")
				}
				return nil
			}
			r.APIReader = testutil.NewFakeClientWithFailures(authoritative, &testutil.FailureConfig{
				OnGet: onGet,
				OnList: func(list client.ObjectList) error {
					if _, cells := list.(*api.CellList); cells && scenario == "cell list error" {
						return errors.New("authoritative list unavailable")
					}
					return nil
				},
			})
			if scenario == "no API reader" {
				r.APIReader = nil
			}
			r.Client = testutil.NewFakeClientWithFailures(r.Client, &testutil.FailureConfig{
				OnGet:         onGet,
				OnStatusPatch: func(client.Object) error { journalPatches++; return nil },
			})
			factory := r.MaintenanceClientFactory
			r.MaintenanceClientFactory = func(ctx context.Context, ts *api.TopoServer) (EtcdMaintenanceClient, error) {
				maintenanceClients++
				return factory(ctx, ts)
			}
			auth.mutations = nil
			ready, err = r.reconcileTopologyAuth(t.Context(), ts)
			require.ErrorContains(t, err, "topology identity removal")
			require.False(t, ready)
			require.Zero(t, secretReads)
			require.Zero(t, journalPatches)
			require.Zero(t, maintenanceClients)
			require.Empty(t, auth.mutations)
			require.Equal(t, previous, ts.Status.AuthIdentities)
			require.True(t, ts.Status.AuthEnabled)
			requireAuthorizationCondition(t, ts, metav1.ConditionFalse, "ReconcileFailed")
			stored := &api.TopoServer{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), stored))
			require.Equal(t, previous, stored.Status.AuthIdentities)
			require.Equal(t, previousVersion, stored.ResourceVersion)
		})
	}
}

func TestTopologyAuthSkipsConfirmationWithoutTrackedRemoval(t *testing.T) {
	for _, scenario := range []string{"unchanged", "new identity", "new range"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, _, _ := authReconcileFixture(t)
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			switch scenario {
			case "new identity":
				require.NoError(t, r.Create(t.Context(), authGrantCluster("second", "second")))
			case "new range":
				second := authGrantCluster("project", "second")
				second.Spec.GlobalTopoServer.External.RootPath = "/multigres/project/second"
				require.NoError(t, r.Create(t.Context(), second))
			}
			listCalls := 0
			r.APIReader = testutil.NewFakeClientWithFailures(r.Client, &testutil.FailureConfig{
				OnList: func(client.ObjectList) error {
					listCalls++
					return errors.New("authoritative discovery must not run")
				},
			})
			ready, err = r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.Zero(t, listCalls)
		})
	}
}

func TestTopologyAuthActivationIsSticky(t *testing.T) {
	for _, scenario := range []string{"missing credentials", "incomplete credentials", "discovery failure", "root outside project", "etcd failure", "one bad project"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, cluster, auth := authReconcileFixture(t)
			_, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			previous := slices.Clone(ts.Status.AuthIdentities)
			previousVersion := ts.ResourceVersion
			reason := "ReconcileFailed"
			switch scenario {
			case "missing credentials", "incomplete credentials":
				secret := &corev1.Secret{}
				require.NoError(t, r.Get(t.Context(), client.ObjectKey{
					Namespace: authFixtureOperatorNamespace,
					Name:      topology.AdminSecretKey(authFixtureOperatorNamespace, ts).Name,
				}, secret))
				if scenario == "missing credentials" {
					require.NoError(t, r.Delete(t.Context(), secret))
				} else {
					delete(secret.Data, "ca.crt")
					require.NoError(t, r.Update(t.Context(), secret))
				}
				reason = "PendingCredentials"
			case "discovery failure":
				require.NoError(t, r.Create(t.Context(), &api.Cell{
					ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: ts.Namespace},
					Spec: api.CellSpec{GlobalTopoServer: api.GlobalTopoServerRef{
						Address: "shared.stores:2379",
					}},
				}))
			case "root outside project":
				cluster.Spec.GlobalTopoServer.External.RootPath = "/outside/project"
				require.NoError(t, r.Update(t.Context(), cluster))
				reason = "ProjectRefRequired"
			case "one bad project":
				bad := authGrantCluster("bad", "bad")
				bad.Spec.GlobalTopoServer.External.ClientCertSecret = "unmanaged"
				require.NoError(t, r.Create(t.Context(), bad))
			case "etcd failure":
				r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
					return nil, errors.New("etcd unavailable")
				}
			}
			auth.mutations = nil
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			if reason == "PendingCredentials" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.False(t, ready)
			require.True(t, ts.Status.AuthEnabled)
			require.True(t, auth.enabled)
			require.Empty(t, auth.mutations)
			require.Equal(t, previous, ts.Status.AuthIdentities)
			stored := &api.TopoServer{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), stored))
			require.Equal(t, previous, stored.Status.AuthIdentities)
			require.Equal(
				t,
				previousVersion,
				stored.ResourceVersion,
				"a failed reconcile must not write status.authIdentities",
			)
			requireAuthorizationCondition(t, ts, metav1.ConditionFalse, reason)
		})
	}
}

func TestTopologyAuthContinuesFromTrackedIdentitiesAlone(t *testing.T) {
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	ts.Status.AuthEnabled = false
	ts.Spec.Auth = nil
	require.True(t, topology.AuthManaged(ts))
	auth.users = map[string][]string{}
	auth.roles = newFakeEtcdAuth().roles
	auth.mutations = nil
	ready, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	require.True(t, ready)
	require.True(t, ts.Status.AuthEnabled)
	require.NotEmpty(t, auth.mutations)
	requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")
}

func TestUpdateStatusPreservesAuthorizationReadiness(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := map[bool]string{
			false: "initial activation gates readiness",
			true:  "activated server keeps serving readiness",
		}[active]
		t.Run(name, func(t *testing.T) {
			ts := &api.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "stores"},
				Spec:       api.TopoServerSpec{Auth: &api.TopoServerAuthSpec{Enabled: true}},
				Status: api.TopoServerStatus{
					AuthEnabled: active,
					Conditions: []metav1.Condition{
						{
							Type: "AuthorizationReady", Status: metav1.ConditionFalse,
							Reason: "ReconcileFailed", Message: "etcd temporarily unavailable",
							LastTransitionTime: metav1.Now(),
						},
						{
							Type: "StorageClassValid", Status: metav1.ConditionTrue,
							Reason: "Valid", Message: "valid", LastTransitionTime: metav1.Now(),
						},
					},
				},
			}
			r := authGrantReconciler(t, ts)
			require.NoError(t, appsv1.AddToScheme(r.Client.Scheme()))
			require.NoError(t, r.Create(t.Context(), &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: ts.Name, Namespace: ts.Namespace},
				Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(1))},
				Status:     appsv1.StatefulSetStatus{Replicas: 1, ReadyReplicas: 1},
			}))
			r.Recorder = record.NewFakeRecorder(10)
			var patched *api.TopoServer
			r.Client = testutil.NewFakeClientWithFailures(
				r.Client,
				&testutil.FailureConfig{
					OnStatusPatch: func(obj client.Object) error {
						patched = obj.(*api.TopoServer).DeepCopy()
						return nil
					},
				},
			)
			require.NoError(t, r.updateStatus(t.Context(), ts))
			require.NotNil(t, patched)
			requireAuthorizationCondition(t, patched, metav1.ConditionFalse, "ReconcileFailed")
			require.Len(t, patched.Status.Conditions, 2)
			require.Nil(t, meta.FindStatusCondition(patched.Status.Conditions, "StorageClassValid"))
			ready := meta.FindStatusCondition(patched.Status.Conditions, "Ready")
			require.NotNil(t, ready)
			require.Equal(t, active, ready.Status == metav1.ConditionTrue)
			require.Equal(t, active, patched.Status.AuthEnabled)
		})
	}
}

func TestReconcileRequeuesAuthorizationAfterAuthSpecRemoved(t *testing.T) {
	r, ts, _, _ := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(t.Context(), ts)
	require.NoError(t, err)
	ts.Spec.Etcd.Replicas = ptr.To(int32(1))
	ts.Spec.Auth = nil
	require.NoError(t, r.Update(t.Context(), ts))
	require.NoError(t, r.Status().Update(t.Context(), ts))
	r.Scheme = r.Client.Scheme()
	require.NoError(t, appsv1.AddToScheme(r.Scheme))
	require.NoError(t, policyv1.AddToScheme(r.Scheme))
	require.NoError(t, storagev1.AddToScheme(r.Scheme))
	r.Scheme.AddKnownTypeWithName(certs.GVK, &unstructured.Unstructured{})
	r.Recorder = record.NewFakeRecorder(100)
	sts, err := BuildStatefulSet(ts, r.Scheme)
	require.NoError(t, err)
	require.NoError(t, r.Create(t.Context(), sts))
	sts.Status = appsv1.StatefulSetStatus{
		Replicas: 1, ReadyReplicas: 1, ObservedGeneration: sts.Generation,
	}
	require.NoError(t, r.Status().Update(t.Context(), sts))
	result, err := r.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(ts),
	})
	require.NoError(t, err)
	require.Equal(t, maintenanceInterval, result.RequeueAfter)
	stored := &api.TopoServer{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), stored))
	require.True(t, stored.Status.AuthEnabled)
	requireAuthorizationCondition(t, stored, metav1.ConditionTrue, "Reconciled")
}

func TestTopologyAuthConfirmsRemovalByReadingRecordedClusters(t *testing.T) {
	tests := map[string]struct {
		apiObjects func(first, second *api.MultigresCluster) []client.Object
		apiGetErr  error
		wantPruned bool
		wantErr    string
	}{
		"cluster deleted": {
			apiObjects: func(first, _ *api.MultigresCluster) []client.Object {
				return []client.Object{first}
			},
			wantPruned: true,
		},
		"cluster no longer references the server": {
			apiObjects: func(first, second *api.MultigresCluster) []client.Object {
				second.Spec.GlobalTopoServer.External.Endpoints = []api.EndpointUrl{
					"https://elsewhere.stores:2379",
				}
				return []client.Object{first, second}
			},
			wantPruned: true,
		},
		"cache lagging behind the API server": {
			apiObjects: func(first, second *api.MultigresCluster) []client.Object {
				return []client.Object{first, second}
			},
		},
		"cluster read fails": {
			apiObjects: func(first, second *api.MultigresCluster) []client.Object {
				return []client.Object{first, second}
			},
			apiGetErr: errors.New("api server unavailable"),
			wantErr:   "api server unavailable",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r, ts, first, auth := authReconcileFixture(t)
			second := authGrantCluster("second", "second")
			require.NoError(t, r.Create(t.Context(), second))
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.Equal(t, map[string]string{
				"/multigres/project": "projects/cluster",
				"/multigres/second":  "projects/second",
			}, ts.Status.AuthIdentityClusters)
			require.NoError(t, r.Delete(t.Context(), second))

			authoritative := authConfirmationReader(
				t, r, tt.apiObjects(first.DeepCopy(), second.DeepCopy())...,
			)
			clusterReads, lists := 0, 0
			r.APIReader = testutil.NewFakeClientWithFailures(authoritative, &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "second" {
						clusterReads++
						return tt.apiGetErr
					}
					return nil
				},
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*api.MultigresClusterList); ok {
						lists++
					}
					return nil
				},
			})
			auth.mutations = nil
			ready, err = r.reconcileTopologyAuth(t.Context(), ts)
			require.Equal(t, 1, clusterReads)
			require.Zero(t, lists, "confirmation must not list every cluster")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.False(t, ready)
				require.Empty(t, auth.mutations)
				require.Len(t, ts.Status.AuthIdentities, 2)
				return
			}
			require.NoError(t, err)
			require.True(t, ready)
			if tt.wantPruned {
				require.Equal(t, []string{"/multigres/project"}, ts.Status.AuthIdentities)
				require.Equal(
					t,
					map[string]string{"/multigres/project": "projects/cluster"},
					ts.Status.AuthIdentityClusters,
				)
				require.NotContains(t, auth.users, "/multigres/second")
			} else {
				require.Len(t, ts.Status.AuthIdentities, 2)
				require.Contains(t, auth.users, "/multigres/second")
			}
		})
	}
}

func TestReconcileRunsMaintenanceDespiteAuthorizationRefusal(t *testing.T) {
	ctx := t.Context()
	r, ts, _, auth := authReconcileFixture(t)
	_, err := r.reconcileTopologyAuth(ctx, ts)
	require.NoError(t, err)
	require.NoError(t, r.Status().Update(ctx, ts))
	ts.Spec.Etcd.Replicas = ptr.To(int32(3))
	ts.Spec.Etcd.Maintenance = &api.EtcdMaintenanceConfig{DefragmentationEnabled: ptr.To(true)}
	require.NoError(t, r.Update(ctx, ts))
	r.Scheme = r.Client.Scheme()
	require.NoError(t, appsv1.AddToScheme(r.Scheme))
	require.NoError(t, policyv1.AddToScheme(r.Scheme))
	require.NoError(t, storagev1.AddToScheme(r.Scheme))
	r.Scheme.AddKnownTypeWithName(certs.GVK, &unstructured.Unstructured{})
	r.Recorder = record.NewFakeRecorder(100)
	sts, err := BuildStatefulSet(ts, r.Scheme)
	require.NoError(t, err)
	sts.UID = "sts-uid"
	require.NoError(t, r.Create(ctx, sts))
	sts.Status = appsv1.StatefulSetStatus{
		Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 3, ObservedGeneration: sts.Generation,
		CurrentRevision: "rev1", UpdateRevision: "rev1",
	}
	require.NoError(t, r.Status().Update(ctx, sts))
	for i := range 3 {
		require.NoError(t, r.Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-%d", ts.Name, i), Namespace: ts.Namespace,
				Labels: map[string]string{appsv1.StatefulSetRevisionLabel: "rev1"},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet")),
				},
			},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
			}}},
		}))
	}
	require.NoError(t, r.Create(ctx, fallbackRootCluster("projects", "noref")))
	sim := etcdsim.NewHealthyAt(maintenanceEndpoints(ts)...)
	r.MaintenanceClientFactory = func(context.Context, *api.TopoServer) (EtcdMaintenanceClient, error) {
		return &authGrantMaintenance{EtcdMaintenanceClient: sim.Client(), fakeEtcdAuth: auth}, nil
	}

	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ts)})
	var refused *projectRefRequiredError
	require.ErrorAs(t, err, &refused)
	require.ErrorContains(t, err, "projects/noref")
	require.Len(t, sim.DefragLog(), 1)
}
