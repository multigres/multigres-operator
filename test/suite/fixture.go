package suite

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
)

// The name of a Secret object, not a credential. gosec matches on the string
// value rather than on how it is used, so the suppression has to be explicit.
//
//nolint:gosec // G101: this is a Secret's name; the password itself is set below
const adminSecretName = "multigres-admin-password"

// MinimalCluster creates the equivalent of config/samples/minimal.yaml in ns,
// along with the password Secret it references, and returns it.
//
// Built in Go rather than read from the sample, because the e2e loader
// (framework.MustLoadCluster) is behind //go:build e2e and this package
// deliberately has no build tag.
//
// The Secret is intentionally unlabelled, matching what a user would create.
// Under the production cache config that makes it invisible to the cached
// client, so this fixture also exercises why the reconcilers hold an APIReader.
func (c *C) MinimalCluster(name string) *MultigresCluster {
	c.Helper()
	return c.newCluster(name)
}

// newCluster creates the admin Secret every cluster in this package
// references, then the cluster itself, applying any spec adjustments in
// between.
//
// The four fixtures here were identical for twenty lines and diverged only
// at Spec.Databases, which is the argument for this existing: the cost of
// the copy grows with the test count rather than being a debt that stays
// fixed, and the next person writing a scenario test copies whichever
// fixture they happened to read.
//
// The adjustment is a callback rather than a returned unsaved object so
// that creating the cluster cannot be forgotten. A fixture that built an
// object and never persisted it would leave the test waiting on a
// convergence that had no reason to start.
//
// The Secret is intentionally unlabelled, matching what a user would create.
// Under the production cache config that makes it invisible to the cached
// client, so this fixture also exercises why the reconcilers hold an
// APIReader.
func (c *C) newCluster(name string, with ...func(*MultigresClusterSpec)) *MultigresCluster {
	c.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: c.NS},
		StringData: map[string]string{"password": "postgres"},
	}
	c.NoError(c.Create(secret), "create password secret")

	cluster := &MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.NS},
		Spec: MultigresClusterSpec{
			PostgresPasswordSecretRef: PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			Cells: []CellConfig{
				{Name: defaultSimCell, ZoneID: "us-central1-a"},
			},
		},
	}
	for _, adjust := range with {
		adjust(&cluster.Spec)
	}
	c.NoError(c.Create(cluster), "create MultigresCluster")
	return cluster
}

// WaitForClusterHealthy blocks until the cluster reports PhaseHealthy.
//
// One method rather than the seven copies pass 1 left behind: two named
// helpers (waitForClusterHealthy in the thrash file and waitForHealthy in the
// transitions file, byte-identical to each other) and five inlined
// Eventually blocks. They were hard to see as duplicates while each was
// wrapped in its own t-and-namespace threading.
//
// It is the convergence check nearly every scenario test starts from, which
// the old comment on one of the copies said out loud without anyone acting
// on it.
//
// 30 seconds because that is what all seven used. It is a convergence wait
// for a whole cluster under five controllers, not a single object read, so it
// is deliberately far longer than any assertion budget.
func (c *C) WaitForClusterHealthy(cluster *MultigresCluster) {
	c.Helper()
	c.Eventually(30*time.Second, "cluster to report Healthy", func() error {
		got := &MultigresCluster{}
		if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
			return err
		}
		if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", got.Status.Phase)
		}
		return nil
	})
}

// etcdMembers is the replica count of the fixture's managed global TopoServer,
// which sets no replicas and so takes the controller's default.
const etcdMembers = int(toposervercontroller.DefaultReplicas)

// globalTopoName is the managed global TopoServer of a cluster, which
// BuildGlobalTopoServer names after the cluster.
func globalTopoName(cluster *MultigresCluster) string {
	return cluster.Name + "-global-topo"
}

// enableEtcdDefragmentation turns on spec.etcd.maintenance defragmentation for
// the cluster's managed global TopoServer. It is separate from MinimalCluster
// so every other scenario keeps maintenance off.
//
// Enabling it as a spec edit on a running cluster is also what wakes the
// TopoServer: its controller reschedules itself an hour out while the etcd
// pods are younger than the stable period, and only a spec change (or a wiped
// status) reaches it sooner.
func (c *C) enableEtcdDefragmentation(cluster *MultigresCluster) {
	c.Helper()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Maintenance: &multigresv1alpha1.EtcdMaintenanceConfig{
					DefragmentationEnabled: ptr.To(true),
				},
			},
		}
	})
}

// ageEtcdPods backdates the Ready condition of every etcd pod of the named
// TopoServer by an hour, so they satisfy the controller's one-minute stable
// period without the test sleeping through it. The data plane sim already
// builds the StatefulSet status, the controller-revision-hash labels and the
// owner references maintenance requires; what it lacks is a clock, so it can
// only stamp Ready as of now.
//
// Testkit gap: a pod-age knob on the data plane sim (settle pods as Ready at
// a given time) would delete this helper.
func (c *C) ageEtcdPods(topoServer string) {
	c.Helper()
	for i := range etcdMembers {
		key := client.ObjectKey{Namespace: c.NS, Name: fmt.Sprintf("%s-%d", topoServer, i)}
		c.Eventually(30*time.Second, "etcd pod "+key.Name+" to be Ready", func() error {
			pod := &corev1.Pod{}
			if err := c.Get(key, pod); err != nil {
				return err
			}
			for j := range pod.Status.Conditions {
				cond := &pod.Status.Conditions[j]
				if cond.Type != corev1.PodReady || cond.Status != corev1.ConditionTrue {
					continue
				}
				cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Hour))
				return c.Client().Status().Update(c.Context(), pod)
			}
			return fmt.Errorf("pod %s is not Ready", key.Name)
		})
	}
}

// updateCluster applies mutate to a fresh read of the cluster and writes it
// back, failing the test rather than returning an error: a write that does not
// land is this file's own setup breaking, never an observation about the
// operator.
//
// It retries on conflict because the cluster controller writes the
// MultigresCluster's own metadata (the cleanup finalizer, the applied-images
// annotation) with optimistic locking, so a reconcile landing between the Get
// and the Update is ordinary rather than a failure. ctrltest has no
// get-mutate-update helper, so this and the loop in
// scenario_shard_lifecycle_test.go both hand-roll one; delete both once it
// does.
func (c *C) updateCluster(
	cluster *MultigresCluster,
	mutate func(*MultigresCluster),
) {
	c.Helper()
	c.NoError(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		got := &MultigresCluster{}
		if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
			return err
		}
		mutate(got)
		return c.Update(got)
	}), "update cluster")
}
