package suite

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
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
