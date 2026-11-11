package toposerver

import (
	"context"
	"errors"
	"slices"
	"testing"

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
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

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
	for _, name := range []string{
		api.TopoServerCertSecretName(ts.Name),
		api.TopoServerAdminCertSecretName(ts.Name),
	} {
		objects = append(objects, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ts.Namespace},
			Data: map[string][]byte{
				corev1.TLSCertKey:       []byte("cert"),
				corev1.TLSPrivateKeyKey: []byte("key"),
				"ca.crt":                []byte("ca"),
			},
		})
	}
	r := authGrantReconciler(t, objects...)
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
		"no project identities": {
			spec:       managed,
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

func TestTopologyAuthConfirmsTrackedIdentityOmissions(t *testing.T) {
	for _, scenario := range []string{"partial cache omission", "authoritative partial removal", "same cardinality replacement", "complete authoritative result wins", "template resolved through API reader"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, first, auth := authReconcileFixture(t)
			second := authGrantCluster("second", "second")
			require.NoError(t, r.Create(t.Context(), second))
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			previous := slices.Clone(ts.Status.AuthIdentities)
			require.Len(t, previous, 2)
			require.NoError(t, r.Delete(t.Context(), second))

			objects := []client.Object{first.DeepCopy(), second.DeepCopy()}
			if scenario == "authoritative partial removal" {
				objects = objects[:1]
			}
			if scenario == "same cardinality replacement" ||
				scenario == "complete authoritative result wins" {
				require.NoError(
					t,
					r.Create(t.Context(), authGrantCluster("cached-only", "cached-only")),
				)
				cached, err := desiredEtcdGrants(t.Context(), r.Client, ts)
				require.NoError(t, err)
				require.Len(
					t,
					cached,
					len(previous),
					"replacement must trigger confirmation despite equal counts",
				)
			}
			if scenario == "complete authoritative result wins" {
				first = first.DeepCopy()
				first.Spec.GlobalTopoServer.External.RootPath = "/multigres/project/confirmed-global"
				objects = []client.Object{
					first,
					authGrantCluster("authoritative-only", "authoritative-only"),
				}
			}
			if scenario == "template resolved through API reader" {
				owner := second.DeepCopy()
				owner.Namespace = ts.Namespace
				owner.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "core"}
				ts.OwnerReferences = controllerRef("MultigresCluster", owner)
				objects = []client.Object{first.DeepCopy(), owner, &api.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "core", Namespace: ts.Namespace},
					Spec: api.CoreTemplateSpec{
						GlobalTopoServer: &api.TopoServerSpec{Etcd: &api.EtcdSpec{}},
					},
				}}
			}
			authoritative := authConfirmationReader(t, r, objects...)
			want, err := desiredEtcdGrants(t.Context(), authoritative, ts)
			require.NoError(t, err)
			listCalls, templateGets := 0, 0
			r.APIReader = testutil.NewFakeClientWithFailures(authoritative, &testutil.FailureConfig{
				OnList: func(client.ObjectList) error { listCalls++; return nil },
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "core" {
						templateGets++
					}
					return nil
				},
			})
			auth.mutations = nil
			ready, err = r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			require.Equal(t, 2, listCalls, "confirm the complete cluster and Cell discovery once")
			if scenario == "template resolved through API reader" {
				require.Len(t, want, len(previous))
				require.Positive(t, templateGets, "resolver must read templates through APIReader")
			}
			require.True(t, ts.Status.AuthEnabled)
			requireAuthorizationCondition(t, ts, metav1.ConditionTrue, "Reconciled")
			wantNames := make([]string, 0, len(want))
			for _, grant := range want {
				wantNames = append(wantNames, grant.Name)
				require.Equal(t, []string{grant.Name}, auth.users[grant.Name])
				permissions := make([]etcdPermission, 0, len(auth.roles[grant.Name]))
				for _, permission := range auth.roles[grant.Name] {
					permissions = append(permissions, etcdPermission{
						Key:      string(permission.Key),
						RangeEnd: string(permission.RangeEnd),
						Type:     permission.PermType,
					})
				}
				require.ElementsMatch(t, grant.Permissions, permissions)
			}
			require.Equal(t, wantNames, ts.Status.AuthIdentities)
			require.Len(t, auth.users, len(want)+2)
			require.Len(t, auth.roles, len(want)+1)
			for _, identity := range previous {
				if !slices.Contains(wantNames, identity) {
					require.NotContains(t, auth.users, identity)
					require.NotContains(t, auth.roles, identity)
				}
			}
			stored := &api.TopoServer{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), stored))
			require.Equal(t, wantNames, stored.Status.AuthIdentities)
		})
	}
}

func TestTopologyAuthConfirmationFailsBeforeMutation(t *testing.T) {
	for _, scenario := range []string{"no API reader", "cluster list error", "cell list error", "template get error", "resolution error"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, first, auth := authReconcileFixture(t)
			second := authGrantCluster("second", "second")
			require.NoError(t, r.Create(t.Context(), second))
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			previous := slices.Clone(ts.Status.AuthIdentities)
			previousVersion := ts.ResourceVersion
			require.NoError(t, r.Delete(t.Context(), second))
			if scenario == "template get error" {
				second.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{TemplateRef: "unavailable"}
			}
			if scenario == "resolution error" {
				second.Spec.GlobalTopoServer.External.RootPath = "/outside/project"
			}
			authoritative := authConfirmationReader(t, r, first.DeepCopy(), second)
			secretReads, journalPatches, maintenanceClients := 0, 0, 0
			onGet := func(key client.ObjectKey) error {
				if key.Name == api.TopoServerCertSecretName(ts.Name) ||
					key.Name == api.TopoServerAdminCertSecretName(ts.Name) {
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
					_, clusters := list.(*api.MultigresClusterList)
					_, cells := list.(*api.CellList)
					if scenario == "cluster list error" && clusters ||
						scenario == "cell list error" && cells {
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
	for _, scenario := range []string{"unchanged", "addition", "permission change"} {
		t.Run(scenario, func(t *testing.T) {
			r, ts, cluster, _ := authReconcileFixture(t)
			ready, err := r.reconcileTopologyAuth(t.Context(), ts)
			require.NoError(t, err)
			require.True(t, ready)
			switch scenario {
			case "addition":
				require.NoError(t, r.Create(t.Context(), authGrantCluster("second", "second")))
			case "permission change":
				cluster.Spec.GlobalTopoServer.External.RootPath = "/multigres/project/changed-global"
				require.NoError(t, r.Update(t.Context(), cluster))
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
	for _, scenario := range []string{"missing credentials", "incomplete credentials", "discovery failure", "etcd failure", "one bad project"} {
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
					Namespace: ts.Namespace,
					Name:      api.TopoServerAdminCertSecretName(ts.Name),
				}, secret))
				if scenario == "missing credentials" {
					require.NoError(t, r.Delete(t.Context(), secret))
				} else {
					delete(secret.Data, "ca.crt")
					require.NoError(t, r.Update(t.Context(), secret))
				}
				reason = "PendingCredentials"
			case "discovery failure":
				cluster.Spec.GlobalTopoServer.External.RootPath = "/outside/project"
				require.NoError(t, r.Update(t.Context(), cluster))
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
	require.True(t, topologyAuthManaged(ts))
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
	require.Equal(t, authRecheckInterval, result.RequeueAfter)
	stored := &api.TopoServer{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), stored))
	require.True(t, stored.Status.AuthEnabled)
	requireAuthorizationCondition(t, stored, metav1.ConditionTrue, "Reconciled")
}
