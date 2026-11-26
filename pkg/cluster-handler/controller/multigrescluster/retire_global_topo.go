package multigrescluster

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// globalTopoRetireRequeue is how often a cluster that has moved to an
// external global topology rechecks whether the move has finished. The
// controller does not watch pods, and the last pod to move may change
// nothing it does watch.
const globalTopoRetireRequeue = 15 * time.Second

// managedGlobalTopoAddress is the client address of the global TopoServer the
// operator manages for cluster, the address every component is started with
// while the cluster uses it.
func managedGlobalTopoAddress(cluster *multigresv1alpha1.MultigresCluster) string {
	return fmt.Sprintf("%s-global-topo.%s.svc:2379", cluster.Name, cluster.Namespace)
}

// retireManagedGlobalTopo deletes the managed global TopoServer of a cluster
// that has moved to an external global topology, once the move has finished,
// and its etcd volumes when its PVC deletion policy says Delete.
//
// Retiring it as soon as the spec names an external server, before anything
// has used that server, removed the only working topology for every pod not
// yet moved, and left no way back if the external server turned out to be
// wrong. So it stays running until moveIncomplete finds nothing left to wait
// for.
//
// The volumes go with it because keeping them is not a safe rollback either:
// moving back to the managed topology recreates the StatefulSet on them, and
// etcd resumes with the topology from before the move, including pooler
// registrations under the names of the pods now running, which the shard
// controller would read as live. The StatefulSet itself always retains them,
// so recreating just the StatefulSet never drops data; this is the one place
// they are deleted short of deleting the cluster.
func (r *MultigresClusterReconciler) retireManagedGlobalTopo(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) (ctrl.Result, error) {
	spec, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to resolve global topo: %w", err)
	}
	if spec.External == nil {
		return ctrl.Result{}, nil
	}

	ts, err := r.managedGlobalTopoServer(ctx, cluster)
	if err != nil || ts == nil {
		return ctrl.Result{}, err
	}

	logger := log.FromContext(ctx)
	w, err := r.moveIncomplete(ctx, cluster, res, ts)
	if err != nil {
		return ctrl.Result{}, err
	}
	if w != nil {
		logger.Info(
			"Keeping the managed global TopoServer until the move to the external one finishes",
			"topoServer", ts.Name, "waitingFor", w.reason, "detail", w.detail,
		)
		// The cluster stays Healthy while this waits, so without an event a
		// TopoServer that never goes away has no explanation on the resource. The
		// message carries only the stable reason, so the recorder aggregates the
		// repeats of a 15s requeue; the changing detail is in the log line.
		eventType, eventReason := corev1.EventTypeNormal, "GlobalTopoRetireWaiting"
		if w.blocked {
			eventType, eventReason = corev1.EventTypeWarning, "GlobalTopoRetireBlocked"
		}
		r.Recorder.Eventf(cluster, eventType, eventReason,
			"Keeping TopoServer %s: waiting for %s", ts.Name, w.reason)
		return ctrl.Result{RequeueAfter: globalTopoRetireRequeue}, nil
	}

	// Everything below is irreversible, and this pass decided on a cluster read
	// at the start of the reconcile, possibly from the cache. A revert to the
	// managed topology, or a Retain made through a CoreTemplate (which does not
	// bump the cluster's generation), may have landed since, so the decision is
	// taken again from uncached reads.
	deletePVCs, proceed, err := r.retirementStillDue(ctx, cluster, ts)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !proceed {
		return ctrl.Result{RequeueAfter: globalTopoRetireRequeue}, nil
	}

	// Volumes before the TopoServer, so a failed volume delete is retried with
	// the TopoServer still there to find them by. PVC protection holds each
	// claim until the etcd pod using it has stopped.
	if deletePVCs {
		deleted, err := r.deleteTopoServerPVCs(ctx, ts)
		if err != nil {
			return ctrl.Result{}, err
		}
		if deleted > 0 {
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted %d PVC(s) of retired TopoServer %s (pvcDeletionPolicy.whenDeleted=Delete)",
				deleted, ts.Name)
		}
	}

	if err := r.Delete(ctx, ts); err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf(
			"failed to delete retired topo server %s: %w",
			ts.Name,
			err,
		)
	}
	r.Recorder.Eventf(cluster, "Normal", "Deleted",
		"Retired TopoServer %s: the cluster has moved to an external global topology", ts.Name)
	return ctrl.Result{}, nil
}

// retirementStillDue re-reads the cluster and its PVC policy through the
// APIReader just before the irreversible deletes. proceed is false when the
// cluster's generation moved since this pass evaluated it or it no longer
// names an external topology; deletePVCs reports whether the volumes go too.
func (r *MultigresClusterReconciler) retirementStillDue(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	ts *multigresv1alpha1.TopoServer,
) (deletePVCs, proceed bool, err error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	fresh := &multigresv1alpha1.MultigresCluster{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return false, false, fmt.Errorf("failed to re-read cluster before retiring: %w", err)
	}
	if fresh.Generation != cluster.Generation {
		return false, false, nil
	}
	res := resolver.NewResolver(uncachedReads{Client: r.Client, reader: reader}, fresh.Namespace)
	spec, err := res.ResolveGlobalTopo(ctx, fresh)
	if err != nil {
		return false, false, fmt.Errorf("failed to resolve global topo before retiring: %w", err)
	}
	if spec.External == nil {
		return false, false, nil
	}
	// The TopoServer too: its own policy is half of the decision, and a Retain
	// set on it by hand is the documented way to keep the volumes.
	freshTS := &multigresv1alpha1.TopoServer{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(ts), freshTS); err != nil {
		if errors.IsNotFound(err) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("failed to re-read topo server before retiring: %w", err)
	}
	return r.retireDeletesPVCs(spec, fresh, freshTS), true, nil
}

// retireDeletesPVCs decides whether the retiring TopoServer's volumes go.
//
// Retain wins: they are deleted only when the current spec's policy, merged
// as BuildGlobalTopoServer merges it, says Delete, and the TopoServer's own
// policy does not say Retain. The current spec matters because nothing updates
// the TopoServer's copy once the spec names an external server, so a Retain
// set together with the move exists only there. The TopoServer's copy matters
// because it is the only place an etcd-level policy survives once the move
// removed the etcd block, and it is what was in force while the etcd ran.
//
// When Retain wins over a current Delete the volumes are kept with an event,
// since a later move back to the managed topology would start etcd on them.
func (r *MultigresClusterReconciler) retireDeletesPVCs(
	spec *multigresv1alpha1.GlobalTopoServerSpec,
	cluster *multigresv1alpha1.MultigresCluster,
	ts *multigresv1alpha1.TopoServer,
) bool {
	current := multigresv1alpha1.MergePVCDeletionPolicy(
		spec.PVCDeletionPolicy,
		cluster.Spec.PVCDeletionPolicy,
	)
	if current == nil || current.WhenDeleted != multigresv1alpha1.DeletePVCRetentionPolicy {
		return false
	}
	if ts.Spec.PVCDeletionPolicy != nil &&
		ts.Spec.PVCDeletionPolicy.WhenDeleted == multigresv1alpha1.RetainPVCRetentionPolicy {
		r.Recorder.Eventf(
			cluster,
			corev1.EventTypeWarning,
			"GlobalTopoPVCsKept",
			"Keeping the PVCs of retired TopoServer %s: its own pvcDeletionPolicy.whenDeleted is Retain. "+
				"A later move back to the managed topology would start etcd on them with the old "+
				"topology; delete them by hand if that is not wanted",
			ts.Name,
		)
		return false
	}
	return true
}

// uncachedReads is a client whose reads go to the API server rather than the
// informer cache, for the resolver's lookups that decide an irreversible delete.
type uncachedReads struct {
	client.Client
	reader client.Reader
}

func (u uncachedReads) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	return u.reader.Get(ctx, key, obj, opts...)
}

func (u uncachedReads) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	return u.reader.List(ctx, list, opts...)
}

// managedGlobalTopoServer returns the global TopoServer this cluster
// controls, or nil.
//
// The selector requires the global-topo component in addition to the cluster
// label, and the result must be controller-owned by this cluster. A cell's
// own local TopoServer carries the same cluster label but is controller-owned
// by the Cell, so the cluster label alone would match it too.
func (r *MultigresClusterReconciler) managedGlobalTopoServer(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.TopoServer, error) {
	list := &multigresv1alpha1.TopoServerList{}
	if err := r.List(ctx, list,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			metadata.LabelMultigresCluster: cluster.Name,
			metadata.LabelAppComponent:     metadata.ComponentGlobalTopo,
		},
	); err != nil {
		return nil, fmt.Errorf("failed to list topo servers: %w", err)
	}
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], cluster) {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// retireWait is what retirement is waiting for. reason is stable across
// requeues, so events aggregate; detail changes (pod names, errors) and goes
// to the log. blocked marks a wait that needs a person rather than time.
type retireWait struct {
	reason  string
	detail  string
	blocked bool
}

// moveIncomplete names what the move to an external global topology is still
// waiting for, or returns nil when the managed topology can be retired.
//
// It is called only after this pass registered every cell and database in the
// external topology, which is the check that the external server works. On
// top of that it requires the cluster to have just been computed Healthy at
// its current generation, which a shard cannot be while any pool pod is still
// on the old topology, since such a pod never passes its readiness gate. And
// it requires direct evidence rather than inference from health: no pod of the
// cluster may still be started against the managed address.
//
// None of that proves the external server is a different etcd: an external
// endpoint that reaches the managed etcd under another name (its fully
// qualified name, an ExternalName Service, its ClusterIP, a LoadBalancer in
// front of its pods) passes every check above, and retiring would then delete
// the etcd the cluster runs on. So, last because it is the only part that
// dials out, it asks the external server who its members are (see
// externalIsAnotherEtcd), which needs nothing from the managed etcd: a dead
// one is retired as soon as the external server shows it is not one of them.
func (r *MultigresClusterReconciler) moveIncomplete(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
	ts *multigresv1alpha1.TopoServer,
) (*retireWait, error) {
	if meta.IsStatusConditionFalse(cluster.Status.Conditions, conditionTopologyReady) {
		return &retireWait{reason: "the external topology to report ready"}, nil
	}
	if cluster.Status.ObservedGeneration != cluster.Generation ||
		cluster.Status.Phase != multigresv1alpha1.PhaseHealthy {
		return &retireWait{
			reason: "the cluster to be Healthy",
			detail: fmt.Sprintf("phase %q, observed generation %d of %d",
				cluster.Status.Phase, cluster.Status.ObservedGeneration, cluster.Generation),
		}, nil
	}

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{metadata.LabelAppInstance: cluster.Name},
	); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	address := managedGlobalTopoAddress(cluster)
	for i := range pods.Items {
		pod := &pods.Items[i]
		// The etcd pods being retired are not clients of the address. A
		// terminated pod runs nothing, yet an evicted one keeps its old args until
		// pod GC, which may be never in practice.
		if isManagedEtcdPod(pod, ts) ||
			pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if podReferences(pod, address) {
			return &retireWait{
				reason: "every pod to stop using " + address,
				detail: "pod " + pod.Name,
			}, nil
		}
	}

	external, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return nil, err
	}
	return r.externalIsAnotherEtcd(ctx, cluster, ts, external)
}

// externalIsAnotherEtcd returns nil when the external server is shown to be an
// etcd other than ts, else what that is still waiting for. Every wait here is
// blocked: it needs a person (or a finished member removal), not time.
//
// The evidence is the external server's own membership. ts's members are
// named after their pods and advertise peer URLs under its headless Service
// (toposerver/container_env.go), so the external server answering for any of
// them, under any alias, lists them, and so does an etcd whose quorum still
// depends on them (a stretched migration with members not yet removed).
//
// A membership is believed only when it is consistent with a real etcd: a
// front end such as an etcd grpc-proxy invents its own (its hostname, no peer
// URLs) while forwarding Status to the backend, so the member that answered
// Status on the same connection has to be one of those listed.
//
// The cluster IDs are compared as well, when the managed etcd is readable,
// as one more reason to wait and never as a reason to retire: a dead managed
// etcd is unreadable, and members removed from a cluster shut down, so this
// never holds back a finished move.
func (r *MultigresClusterReconciler) externalIsAnotherEtcd(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	ts *multigresv1alpha1.TopoServer,
	external multigresv1alpha1.GlobalTopoServerRef,
) (*retireWait, error) {
	ext, err := r.etcdIdentity(ctx, cluster.Namespace, external)
	if err != nil {
		return &retireWait{
			reason: "the external etcd's membership to be readable",
			detail: err.Error(), blocked: true,
		}, nil
	}
	if bad := ext.Implausible(); bad != "" {
		return &retireWait{
			reason: "the external topology to report a genuine etcd membership",
			detail: bad, blocked: true,
		}, nil
	}
	hosts, err := r.managedEtcdHosts(ctx, ts)
	if err != nil {
		return nil, err
	}
	if m := managedMember(ext.Members, ts, hosts); m != "" {
		return &retireWait{
			reason: fmt.Sprintf(
				"the external topology to list no member of %s (it lists %s)",
				ts.Name,
				m,
			),
			blocked: true,
		}, nil
	}
	if managed, err := r.etcdIdentity(ctx, cluster.Namespace, managedTopoRef(ts)); err == nil {
		for _, id := range []uint64{managed.ClusterID, managed.StatusClusterID} {
			if id != 0 && (id == ext.ClusterID || id == ext.StatusClusterID) {
				return &retireWait{
					reason: fmt.Sprintf(
						"the external topology to be an etcd other than %s",
						ts.Name,
					),
					detail:  fmt.Sprintf("both report cluster ID %x", id),
					blocked: true,
				}, nil
			}
		}
	}
	return nil, nil
}

// managedMember names the first member that is one of ts's, or "": by its name
// (members are named after their pods, ts.Name-<ordinal>), or by any peer or
// client URL whose host is one of ts's Services or pods. Peer URLs matter for a
// member added but never started, which has no name and no client URLs yet.
func managedMember(
	members []topo.EtcdMember,
	ts *multigresv1alpha1.TopoServer,
	hosts map[string]bool,
) string {
	for _, m := range members {
		if ordinal, ok := strings.CutPrefix(m.Name, ts.Name+"-"); ok {
			if _, err := strconv.ParseUint(ordinal, 10, 32); err == nil {
				return m.Name
			}
		}
		for _, u := range slices.Concat(m.PeerURLs, m.ClientURLs) {
			if isManagedHost(urlHost(u), ts, hosts) {
				return u
			}
		}
	}
	return ""
}

// managedEtcdHosts returns the IP addresses ts's etcd is reachable at, its
// pods' and its client Service's, read uncached: a member whose URLs were
// rewritten to IPs is still recognised.
func (r *MultigresClusterReconciler) managedEtcdHosts(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (map[string]bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	hosts := map[string]bool{}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(ts.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	for i := range pods.Items {
		if isManagedEtcdPod(&pods.Items[i], ts) {
			for _, ip := range pods.Items[i].Status.PodIPs {
				hosts[normalizeHost(ip.IP)] = true
			}
		}
	}
	svc := &corev1.Service{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: ts.Namespace, Name: ts.Name}, svc)
	switch {
	case err == nil:
		for _, ip := range svc.Spec.ClusterIPs {
			if ip != corev1.ClusterIPNone {
				hosts[normalizeHost(ip)] = true
			}
		}
	case !errors.IsNotFound(err):
		return nil, fmt.Errorf("failed to get service %s: %w", ts.Name, err)
	}
	return hosts, nil
}

// isManagedHost reports whether host is ts's client or headless Service, in
// any of its short, namespaced or fully qualified forms, a pod record under
// the headless Service, or one of ips.
func isManagedHost(host string, ts *multigresv1alpha1.TopoServer, ips map[string]bool) bool {
	if host == "" {
		return false
	}
	if ips[host] {
		return true
	}
	for _, svc := range []string{ts.Name, ts.Name + "-headless"} {
		for _, name := range []string{
			svc, svc + "." + ts.Namespace, svc + "." + ts.Namespace + ".svc",
			svc + "." + ts.Namespace + ".svc.cluster.local",
		} {
			// Only the headless Service has records under it, one per pod.
			if host == name || (svc != ts.Name && strings.HasSuffix(host, "."+name)) {
				return true
			}
		}
	}
	return false
}

// urlHost is the normalized host of an etcd URL, or "" if it does not parse.
func urlHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return normalizeHost(u.Hostname())
}

func normalizeHost(h string) string {
	return strings.TrimSuffix(strings.ToLower(h), ".")
}

// isManagedEtcdPod reports whether pod is one of ts's etcd members, by the
// StatefulSet's pod naming (ts.Name-<ordinal>). The pods' labels carry the
// generic toposerver component, which every TopoServer's pods share.
func isManagedEtcdPod(pod *corev1.Pod, ts *multigresv1alpha1.TopoServer) bool {
	ordinal, ok := strings.CutPrefix(pod.Name, ts.Name+"-")
	if !ok || pod.Namespace != ts.Namespace {
		return false
	}
	_, err := strconv.ParseUint(ordinal, 10, 32)
	return err == nil
}

// managedTopoRef is a topology reference to ts's own Service, with ts's serving
// credential as both CA bundle and client keypair when TLS is on: the
// certificate carries client-auth usage for etcd peer TLS, which is also how
// the TopoServer controller's maintenance client reaches it.
func managedTopoRef(ts *multigresv1alpha1.TopoServer) multigresv1alpha1.GlobalTopoServerRef {
	scheme := "http"
	ref := multigresv1alpha1.GlobalTopoServerRef{Implementation: "etcd"}
	if ts.Spec.TLS.IsEnabled() {
		scheme = "https"
		secret := multigresv1alpha1.TopoServerCertSecretName(ts.Name)
		ref.CASecret = secret
		ref.ClientCertSecret = secret
	}
	ref.Address = fmt.Sprintf("%s://%s.%s.svc:2379", scheme, ts.Name, ts.Namespace)
	return ref
}

func (r *MultigresClusterReconciler) etcdIdentity(
	ctx context.Context,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topo.EtcdIdentity, error) {
	if r.ReadEtcdIdentity != nil {
		return r.ReadEtcdIdentity(ctx, namespace, ref)
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	return topo.ReadEtcdIdentity(ctx, reader, namespace, ref, r.DialContext)
}

// podReferences reports whether any container or init container of pod has
// address in an argument or environment value. Matching inside each string
// rather than on a flag name covers every form a component passes it in:
// multipooler and multiorch as one "--flag=value" argument, multigateway as
// the flag and the value in two.
func podReferences(pod *corev1.Pod, address string) bool {
	for _, c := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		for _, s := range slices.Concat(c.Command, c.Args) {
			if strings.Contains(s, address) {
				return true
			}
		}
		for _, e := range c.Env {
			if strings.Contains(e.Value, address) {
				return true
			}
		}
	}
	return false
}

// deleteTopoServerPVCs deletes the etcd data PVCs of ts and returns how many
// it asked to delete.
//
// They are identified by name, "data-<topo server>-<ordinal>" as the
// StatefulSet's claim template creates them, rather than by label: a cell's
// local TopoServer's claims carry the same cluster and component labels.
func (r *MultigresClusterReconciler) deleteTopoServerPVCs(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (int, error) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(ts.Namespace)); err != nil {
		return 0, fmt.Errorf("failed to list PVCs: %w", err)
	}
	prefix := "data-" + ts.Name + "-"
	deleted := 0
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		ordinal, ok := strings.CutPrefix(pvc.Name, prefix)
		if !ok || !pvc.DeletionTimestamp.IsZero() {
			continue
		}
		if _, err := strconv.ParseUint(ordinal, 10, 32); err != nil {
			continue
		}
		if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return deleted, fmt.Errorf("failed to delete topo PVC %s: %w", pvc.Name, err)
		}
		deleted++
	}
	return deleted, nil
}
