package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// markMoveFinished gives cluster the status updateStatus computes once every
// child is Healthy at the current generation.
func markMoveFinished(cluster *multigresv1alpha1.MultigresCluster) {
	cluster.Generation = 2
	cluster.Status.ObservedGeneration = 2
	cluster.Status.Phase = multigresv1alpha1.PhaseHealthy
}

type retireFixture struct {
	cluster *multigresv1alpha1.MultigresCluster
	ts      *multigresv1alpha1.TopoServer
	objs    []client.Object

	// identity stands in for the etcd behind each address. The default gives
	// every address its own etcd.
	identity func(ref multigresv1alpha1.GlobalTopoServerRef) (topo.EtcdIdentity, error)
	events   []string

	// stored, when set, changes the API server's copy of the cluster after the
	// pass has evaluated f.cluster: an edit that landed mid-reconcile.
	stored func(c *multigresv1alpha1.MultigresCluster)
}

// distinctEtcds is a ReadEtcdIdentity stand-in that gives every address its
// own single-member etcd, named and addressed after nothing the operator
// manages.
func distinctEtcds(
	_ context.Context,
	_ string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topo.EtcdIdentity, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ref.Address))
	id := h.Sum64() | 1
	return singleMember(id, id+1, fmt.Sprintf("etcd-%x", id),
		fmt.Sprintf("https://etcd-%x.example:2380", id)), nil
}

// singleMember is a consistent one-member etcd: the member that answers Status
// is the one listed.
func singleMember(clusterID, memberID uint64, name string, peerURLs ...string) topo.EtcdIdentity {
	return topo.EtcdIdentity{
		ClusterID: clusterID, StatusClusterID: clusterID, StatusMemberID: memberID,
		Members: []topo.EtcdMember{{ID: memberID, Name: name, PeerURLs: peerURLs}},
	}
}

// managedEtcd is the fixture's managed etcd as its own members describe it:
// named after their pods, with peer URLs under the headless Service.
func managedEtcd() topo.EtcdIdentity {
	return singleMember(0xaa, 0xa0, "test-global-topo-0",
		"https://test-global-topo-0.test-global-topo-headless.default.svc.cluster.local:2380")
}

// isManagedRef reports whether ref is the fixture's managed etcd's own Service.
func isManagedRef(ref multigresv1alpha1.GlobalTopoServerRef) bool {
	return strings.Contains(ref.Address, "test-global-topo.default.svc")
}

// newRetireFixture is a cluster moved to an external global topology whose
// managed global TopoServer, with three etcd PVCs, is still present, plus a
// cell-local TopoServer's PVC and a PVC whose name only looks like the global
// TopoServer's.
func newRetireFixture(whenDeleted multigresv1alpha1.PVCRetentionPolicyType) *retireFixture {
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "cluster-uid"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"https://shared-topo:2379"},
				},
			},
		},
	}
	markMoveFinished(cluster)

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-global-topo",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: cluster.Name,
				metadata.LabelAppComponent:     metadata.ComponentGlobalTopo,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: multigresv1alpha1.GroupVersion.String(),
				Kind:       "MultigresCluster",
				Name:       cluster.Name,
				UID:        cluster.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}},
	}
	// As BuildGlobalTopoServer leaves it before the move: the TopoServer's own
	// policy is the cluster's. Tests that need them to differ set them apart.
	if whenDeleted != "" {
		cluster.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: whenDeleted,
		}
		ts.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{WhenDeleted: whenDeleted}
	}

	objs := []client.Object{cluster, ts}
	for _, name := range []string{
		"data-test-global-topo-0",
		"data-test-global-topo-1",
		"data-test-global-topo-2",
		"data-test-zone-a-0",
		"data-test-global-topo-orphaned-0",
	} {
		objs = append(objs, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		})
	}
	return &retireFixture{
		cluster: cluster,
		ts:      ts,
		objs:    objs,
		identity: func(ref multigresv1alpha1.GlobalTopoServerRef) (topo.EtcdIdentity, error) {
			if isManagedRef(ref) {
				return managedEtcd(), nil
			}
			return distinctEtcds(context.Background(), "", ref)
		},
	}
}

// pod is a component pod of the fixture's cluster started with args.
func (f *retireFixture) pod(name, component string, args ...string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelAppInstance:  f.cluster.Name,
				metadata.LabelAppComponent: component,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Args: args}}},
	}
}

// retire runs one retirement pass over objs and reports which of the
// fixture's TopoServer and PVCs remain.
func (f *retireFixture) retire(
	t *testing.T,
	extra ...client.Object,
) (requeue bool, tsExists bool, pvcs map[string]bool) {
	t.Helper()
	scheme := setupScheme()
	objs := append(slices.Clone(f.objs), extra...)
	if f.stored != nil {
		stored := f.cluster.DeepCopy()
		f.stored(stored)
		objs[slices.Index(objs, client.Object(f.cluster))] = stored
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	recorder := record.NewFakeRecorder(10)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
		ReadEtcdIdentity: func(
			_ context.Context,
			_ string,
			ref multigresv1alpha1.GlobalTopoServerRef,
		) (topo.EtcdIdentity, error) {
			return f.identity(ref)
		},
	}
	defer func() {
		close(recorder.Events)
		for e := range recorder.Events {
			f.events = append(f.events, e)
		}
	}()

	result, err := r.retireManagedGlobalTopo(
		t.Context(),
		f.cluster,
		resolver.NewResolver(c, "default"),
	)
	if err != nil {
		t.Fatalf("retireManagedGlobalTopo() error = %v", err)
	}

	err = c.Get(t.Context(), client.ObjectKeyFromObject(f.ts), &multigresv1alpha1.TopoServer{})
	switch {
	case err == nil:
		tsExists = true
	case !apierrors.IsNotFound(err):
		t.Fatalf("get TopoServer: %v", err)
	}

	list := &corev1.PersistentVolumeClaimList{}
	if err := c.List(t.Context(), list, client.InNamespace("default")); err != nil {
		t.Fatalf("list PVCs: %v", err)
	}
	pvcs = map[string]bool{}
	for _, pvc := range list.Items {
		pvcs[pvc.Name] = true
	}
	return result.RequeueAfter > 0, tsExists, pvcs
}

func TestRetireManagedGlobalTopo_PVCPolicy(t *testing.T) {
	globalPVCs := []string{
		"data-test-global-topo-0",
		"data-test-global-topo-1",
		"data-test-global-topo-2",
	}
	unrelated := []string{"data-test-zone-a-0", "data-test-global-topo-orphaned-0"}

	t.Run("Delete removes only the retired TopoServer's own PVCs", func(t *testing.T) {
		requeue, tsExists, pvcs := newRetireFixture(
			multigresv1alpha1.DeletePVCRetentionPolicy,
		).retire(t)
		if requeue || tsExists {
			t.Fatalf("requeue=%v tsExists=%v, want the TopoServer retired", requeue, tsExists)
		}
		for _, name := range globalPVCs {
			if pvcs[name] {
				t.Errorf("PVC %s survived a Delete policy", name)
			}
		}
		for _, name := range unrelated {
			if !pvcs[name] {
				t.Errorf("PVC %s does not belong to the retired TopoServer but was deleted", name)
			}
		}
	})

	for _, tc := range []struct {
		name   string
		policy multigresv1alpha1.PVCRetentionPolicyType
	}{
		{"Retain keeps the PVCs", multigresv1alpha1.RetainPVCRetentionPolicy},
		{"no policy keeps the PVCs", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tsExists, pvcs := newRetireFixture(tc.policy).retire(t)
			if tsExists {
				t.Fatal("TopoServer not retired")
			}
			for _, name := range append(globalPVCs, unrelated...) {
				if !pvcs[name] {
					t.Errorf("PVC %s deleted under policy %q", name, tc.policy)
				}
			}
		})
	}
}

// TestRetireManagedGlobalTopo_WaitsForTheMove pins each condition that keeps
// the managed TopoServer, and its data, while the move to the external
// topology is unfinished.
func TestRetireManagedGlobalTopo_WaitsForTheMove(t *testing.T) {
	address := "test-global-topo.default.svc:2379"
	for _, tc := range []struct {
		name   string
		adjust func(f *retireFixture) []client.Object
	}{
		{
			name: "cluster not Healthy",
			adjust: func(f *retireFixture) []client.Object {
				f.cluster.Status.Phase = multigresv1alpha1.PhaseProgressing
				return nil
			},
		},
		{
			name: "status from an earlier generation",
			adjust: func(f *retireFixture) []client.Object {
				f.cluster.Status.ObservedGeneration = 1
				return nil
			},
		},
		{
			name: "external topology not ready",
			adjust: func(f *retireFixture) []client.Object {
				meta.SetStatusCondition(&f.cluster.Status.Conditions, metav1.Condition{
					Type:   conditionTopologyReady,
					Status: metav1.ConditionFalse,
					Reason: "TopoConnectFailed",
				})
				return nil
			},
		},
		{
			name: "a pool pod still started with the managed address",
			adjust: func(f *retireFixture) []client.Object {
				return []client.Object{f.pod("pool-0", "shard-pool",
					"multipooler", "--topo-global-server-addresses="+address)}
			},
		},
		{
			name: "a gateway passing the managed address as its own argument",
			adjust: func(f *retireFixture) []client.Object {
				return []client.Object{f.pod("gw-0", metadata.ComponentMultigateway,
					"multigateway", "--topo-global-server-addresses", address)}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
			requeue, tsExists, pvcs := f.retire(t, tc.adjust(f)...)
			if !tsExists || !pvcs["data-test-global-topo-0"] {
				t.Fatalf("tsExists=%v pvc=%v, want both kept while the move is unfinished",
					tsExists, pvcs["data-test-global-topo-0"])
			}
			if !requeue {
				t.Error("want a requeue to recheck the move")
			}
			// An expected wait during a healthy move is Normal, with a message
			// stable enough for the recorder to aggregate (no pod names).
			stable := len(f.events) == 1 &&
				strings.HasPrefix(f.events[0], "Normal GlobalTopoRetireWaiting ") &&
				!strings.Contains(f.events[0], "pool-0") && !strings.Contains(f.events[0], "gw-0")
			if !stable {
				t.Errorf("events = %q, want one stable GlobalTopoRetireWaiting event", f.events)
			}
		})
	}

	t.Run("a terminated pod with the managed address does not hold it back", func(t *testing.T) {
		for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
			f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
			evicted := f.pod("gw-0", metadata.ComponentMultigateway,
				"multigateway", "--topo-global-server-addresses="+address)
			evicted.Status.Phase = phase
			if _, tsExists, _ := f.retire(t, evicted); tsExists {
				t.Errorf("phase %s: TopoServer kept because of a pod that runs nothing", phase)
			}
		}
	})

	t.Run("a pending pod with the managed address still holds it back", func(t *testing.T) {
		f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
		pending := f.pod("gw-0", metadata.ComponentMultigateway,
			"multigateway", "--topo-global-server-addresses="+address)
		pending.Status.Phase = corev1.PodPending
		if _, tsExists, _ := f.retire(t, pending); !tsExists {
			t.Error("TopoServer retired while a pending pod is about to start against it")
		}
	})

	t.Run("the retiring etcd pods do not hold it back", func(t *testing.T) {
		f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
		etcd := f.pod("test-global-topo-0", metadata.ComponentTopoServer,
			"--advertise-client-urls=https://"+address)
		moved := f.pod("pool-0", "shard-pool",
			"multipooler", "--topo-global-server-addresses=https://shared-topo:2379")
		if _, tsExists, _ := f.retire(t, etcd, moved); tsExists {
			t.Error("TopoServer kept although only its own pods name the managed address")
		}
	})

	t.Run("a pod of another cluster does not hold it back", func(t *testing.T) {
		f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
		other := f.pod("other-pool-0", "shard-pool", "--topo-global-server-addresses="+address)
		other.Labels[metadata.LabelAppInstance] = "other"
		if _, tsExists, _ := f.retire(t, other); tsExists {
			t.Error("TopoServer kept because of another cluster's pod")
		}
	})
}

func TestRetireManagedGlobalTopo_ManagedClusterKeepsIt(t *testing.T) {
	f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
	f.cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
		Etcd: &multigresv1alpha1.EtcdSpec{},
	}
	requeue, tsExists, pvcs := f.retire(t)
	if requeue || !tsExists || !pvcs["data-test-global-topo-0"] {
		t.Errorf("requeue=%v tsExists=%v pvc=%v, want a managed cluster's topology untouched",
			requeue, tsExists, pvcs["data-test-global-topo-0"])
	}
}

func TestManagedGlobalTopoAddress(t *testing.T) {
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mgc-x", Namespace: "ha-project-x"},
	}
	if got, want := managedGlobalTopoAddress(
		cluster,
	), "mgc-x-global-topo.ha-project-x.svc:2379"; got != want {
		t.Errorf("managedGlobalTopoAddress() = %q, want %q", got, want)
	}
}

// TestRetireManagedGlobalTopo_RequiresADifferentEtcd pins the check that the
// external server is not the managed etcd under another name. Every other
// condition is met in both cases, so only the identity comparison keeps it.
func TestRetireManagedGlobalTopo_RequiresADifferentEtcd(t *testing.T) {
	managed := managedEtcd()
	external := func(id topo.EtcdIdentity, managedErr error) func(
		multigresv1alpha1.GlobalTopoServerRef,
	) (topo.EtcdIdentity, error) {
		return func(ref multigresv1alpha1.GlobalTopoServerRef) (topo.EtcdIdentity, error) {
			if isManagedRef(ref) {
				return managed, managedErr
			}
			return id, nil
		}
	}
	dead := errors.New("context deadline exceeded")
	other := singleMember(0xbb, 0xb0, "ext-0", "https://ext-0.example:2380")

	for _, tc := range []struct {
		name     string
		identity func(multigresv1alpha1.GlobalTopoServerRef) (topo.EtcdIdentity, error)
		retire   bool
	}{
		{name: "another etcd, managed readable", identity: external(other, nil), retire: true},
		{
			// The case a probe of the managed members could never decide: the
			// external membership alone shows the managed etcd is not behind it.
			name: "another etcd, managed etcd dead", identity: external(other, dead), retire: true,
		},
		{name: "an alias of the managed etcd", identity: external(managed, nil)},
		{name: "an alias of a dead managed etcd", identity: external(managed, dead)},
		{
			// A stretched migration: the external cluster still has a managed
			// member it was added next to, known only by its peer URL.
			name: "a cluster that still lists an unstarted managed member",
			identity: external(topo.EtcdIdentity{
				ClusterID: 0xbb, StatusClusterID: 0xbb, StatusMemberID: 0xb0,
				Members: []topo.EtcdMember{
					{ID: 0xb0, Name: "ext-0", PeerURLs: []string{"https://ext-0.example:2380"}},
					{ID: 0xa1, PeerURLs: []string{
						"https://test-global-topo-1.test-global-topo-headless.default.svc:2380",
					}},
				},
			}, dead),
		},
		{
			// An etcd grpc-proxy in front of the managed etcd: it invents its
			// membership and forwards Status to a member it did not list.
			name: "a proxy that invents its membership",
			identity: external(topo.EtcdIdentity{
				ClusterID: 0xaa, StatusClusterID: 0xaa, StatusMemberID: 0xa0,
				Members: []topo.EtcdMember{{Name: "etcd-proxy-7f9c", ClientURLs: []string{"https://proxy:2379"}}},
			}, dead),
		},
		{
			name: "a membership for another cluster than Status answered for",
			identity: external(topo.EtcdIdentity{
				ClusterID: 0xbb, StatusClusterID: 0xaa, StatusMemberID: 0xb0,
				Members: other.Members,
			}, nil),
		},
		{
			// Membership clean, but the cluster IDs match: kept, as one more
			// reason to wait.
			name:     "a clean membership with the managed cluster ID",
			identity: external(singleMember(0xaa, 0xc0, "ext-0", "https://ext-0.example:2380"), nil),
		},
		{
			name: "an unreadable external",
			identity: func(ref multigresv1alpha1.GlobalTopoServerRef) (topo.EtcdIdentity, error) {
				if isManagedRef(ref) {
					return managed, nil
				}
				return topo.EtcdIdentity{}, dead
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
			f.identity = tc.identity
			requeue, tsExists, pvcs := f.retire(t)
			if tc.retire {
				if tsExists || pvcs["data-test-global-topo-0"] {
					t.Fatalf(
						"tsExists=%v pvc=%v, want it retired",
						tsExists,
						pvcs["data-test-global-topo-0"],
					)
				}
				return
			}
			if !tsExists || !pvcs["data-test-global-topo-0"] || !requeue {
				t.Fatalf("requeue=%v tsExists=%v pvc=%v, want everything kept and a recheck",
					requeue, tsExists, pvcs["data-test-global-topo-0"])
			}
			if len(f.events) != 1 ||
				!strings.HasPrefix(f.events[0], "Warning GlobalTopoRetireBlocked ") {
				t.Errorf("events = %q, want one GlobalTopoRetireBlocked warning", f.events)
			}
		})
	}

	t.Run("a member whose URLs were rewritten to the managed pod's IP", func(t *testing.T) {
		f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
		f.identity = external(singleMember(0xbb, 0xb0, "renamed", "https://10.1.2.3:2380"), dead)
		etcdPod := f.pod("test-global-topo-0", metadata.ComponentTopoServer)
		etcdPod.Status.PodIPs = []corev1.PodIP{{IP: "10.1.2.3"}}
		if _, tsExists, _ := f.retire(t, etcdPod); !tsExists {
			t.Fatal("retired although the external lists a member at the managed pod's IP")
		}
	})
}

func TestManagedTopoRef(t *testing.T) {
	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "c-global-topo", Namespace: "ns"},
	}
	plain := managedTopoRef(ts)
	if plain.Address != "http://c-global-topo.ns.svc:2379" || plain.ClientCertSecret != "" {
		t.Errorf("without TLS: %+v", plain)
	}
	ts.Spec.TLS = &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)}
	secure := managedTopoRef(ts)
	secret := multigresv1alpha1.TopoServerCertSecretName(ts.Name)
	if secure.Address != "https://c-global-topo.ns.svc:2379" ||
		secure.ClientCertSecret != secret || secure.CASecret != secret {
		t.Errorf("with TLS: %+v", secure)
	}
}

// TestRetireManagedGlobalTopo_RetainSetWithTheMove pins that a Retain set on
// the global topology in the same change as the move to external is honoured,
// over a cluster-wide Delete and over the Delete the old TopoServer was
// created with.
func TestRetireManagedGlobalTopo_RetainSetWithTheMove(t *testing.T) {
	f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
	f.ts.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	f.cluster.Spec.GlobalTopoServer.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	_, tsExists, pvcs := f.retire(t)
	if tsExists {
		t.Fatal("TopoServer not retired")
	}
	for _, name := range []string{
		"data-test-global-topo-0", "data-test-global-topo-1", "data-test-global-topo-2",
	} {
		if !pvcs[name] {
			t.Errorf("PVC %s deleted although the global topology's policy is Retain", name)
		}
	}
}

// TestRetireManagedGlobalTopo_RetainWins pins that the volumes are deleted only
// when both the current spec and the TopoServer's own policy allow it: the
// current spec carries a Retain set with the move, the TopoServer's copy
// carries an etcd-level Retain the move removed from the spec.
func TestRetireManagedGlobalTopo_RetainWins(t *testing.T) {
	retain, del := multigresv1alpha1.RetainPVCRetentionPolicy, multigresv1alpha1.DeletePVCRetentionPolicy
	for _, tc := range []struct {
		current, own multigresv1alpha1.PVCRetentionPolicyType
		wantDeleted  bool
	}{
		{current: del, own: del, wantDeleted: true},
		{current: del, own: "", wantDeleted: true},
		{current: del, own: retain},
		{current: retain, own: del},
		{current: "", own: del},
		{current: retain, own: retain},
	} {
		t.Run(fmt.Sprintf("current %q own %q", tc.current, tc.own), func(t *testing.T) {
			f := newRetireFixture(tc.current)
			f.ts.Spec.PVCDeletionPolicy = nil
			if tc.own != "" {
				f.ts.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
					WhenDeleted: tc.own,
				}
			}
			_, tsExists, pvcs := f.retire(t)
			if tsExists {
				t.Fatal("TopoServer not retired")
			}
			if got := !pvcs["data-test-global-topo-0"]; got != tc.wantDeleted {
				t.Fatalf("PVCs deleted = %v, want %v", got, tc.wantDeleted)
			}
			kept := slices.ContainsFunc(f.events, func(e string) bool {
				return strings.HasPrefix(e, "Warning GlobalTopoPVCsKept ")
			})
			if want := tc.current == del && tc.own == retain; kept != want {
				t.Errorf("GlobalTopoPVCsKept event = %v, want %v (events %q)", kept, want, f.events)
			}
		})
	}
}

// TestRetireManagedGlobalTopo_RechecksBeforeDeleting pins the uncached re-read
// before the irreversible deletes: an edit that lands after the pass evaluated
// the cluster stops retirement for this pass.
func TestRetireManagedGlobalTopo_RechecksBeforeDeleting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored func(c *multigresv1alpha1.MultigresCluster)
	}{
		{"a newer generation", func(c *multigresv1alpha1.MultigresCluster) { c.Generation++ }},
		{"reverted to the managed topology", func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
			f.stored = tc.stored
			requeue, tsExists, pvcs := f.retire(t)
			if !tsExists || !pvcs["data-test-global-topo-0"] || !requeue {
				t.Fatalf("requeue=%v tsExists=%v pvc=%v, want nothing deleted and a recheck",
					requeue, tsExists, pvcs["data-test-global-topo-0"])
			}
		})
	}

	t.Run("a Retain that only the fresh read sees keeps the volumes", func(t *testing.T) {
		f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
		f.stored = func(c *multigresv1alpha1.MultigresCluster) {
			c.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			}
		}
		_, tsExists, pvcs := f.retire(t)
		if tsExists || !pvcs["data-test-global-topo-0"] {
			t.Fatalf("tsExists=%v pvc=%v, want the TopoServer retired and its volumes kept",
				tsExists, pvcs["data-test-global-topo-0"])
		}
	})
}

// TestReconcileGlobalTopoServer_LeavesARetiringTopoServerAlone pins what the
// documented override rests on: once the spec names an external server,
// nothing re-applies the managed TopoServer, so a pvcDeletionPolicy a user set
// on it by hand stands until retirement reads it.
func TestReconcileGlobalTopoServer_LeavesARetiringTopoServerAlone(t *testing.T) {
	f := newRetireFixture(multigresv1alpha1.DeletePVCRetentionPolicy)
	f.ts.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.objs...).Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileGlobalTopoServer(
		t.Context(),
		f.cluster,
		resolver.NewResolver(c, "default"),
	); err != nil {
		t.Fatalf("reconcileGlobalTopoServer() error = %v", err)
	}
	got := &multigresv1alpha1.TopoServer{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(f.ts), got); err != nil {
		t.Fatalf("get TopoServer: %v", err)
	}
	if got.Spec.PVCDeletionPolicy == nil ||
		got.Spec.PVCDeletionPolicy.WhenDeleted != multigresv1alpha1.RetainPVCRetentionPolicy {
		t.Errorf(
			"TopoServer policy = %+v, want the hand-set Retain left in place",
			got.Spec.PVCDeletionPolicy,
		)
	}
}
