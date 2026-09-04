//go:build e2e

package topotls_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestTopoTLSCluster brings up a cluster with topoTLS enabled and verifies it
// serves queries. Reaching that state proves the full mutual TLS path: etcd
// requires a client certificate on its client and peer listeners, the four pod
// components present the issued certificate, and the operator presents it on its
// own connections when it registers the cell and database. A plaintext client
// anywhere in that path would leave the cluster unable to become ready.
func TestTopoTLSCluster(t *testing.T) {
	t.Parallel()

	// cert-manager and the shared CA the operator signs the etcd serving
	// certificate and the client credential with.
	cluster.EnsureTopoTLSIssuer(t)

	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/topo-tls.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	// The child resource tree still forms with topology TLS on.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.TopoServerList{},
		func(l *multigresv1alpha1.TopoServerList) int { return len(l.Items) },
		1, "TopoServer",
	)

	// etcd, the components, and the pool pod all come up. etcd requires a client
	// certificate on its listeners, so every component reaching a ready state
	// means it presented the mounted certificate.
	framework.WaitForStatefulSet(t, c, ns, "etcd")
	framework.WaitForDeployment(t, c, ns, "multiadmin")
	framework.WaitForDeployment(t, c, ns, "multigateway")
	framework.WaitForDeployment(t, c, ns, "multiorch")
	framework.WaitForPod(t, c, ns, "postgres")
	cluster.WaitForAllPodsReady(t, ns)

	// The operator also connects to etcd, over the same mutual TLS, to register
	// the cell and database. TopologyReady goes true only once that connection
	// succeeds, so it is the direct signal that the operator presented its own
	// certificate.
	waitForTopologyReady(t, c, ns, "topo-tls")

	// Pod readiness and the operator's own connection do not prove the data
	// plane works: multigateway, multiorch and multipooler each open their own
	// topology connection with the mounted certificate to resolve a pooler and
	// route queries, and a broken client path there leaves the pods ready but
	// unable to serve. A served query is the end-to-end check that they too
	// present the certificate.
	gwSvc := framework.FindGatewayService(t, cluster, ns)
	framework.WaitForQueryServing(t, cluster, ns, gwSvc)
}

func waitForTopologyReady(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(ctx, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		cluster := &multigresv1alpha1.MultigresCluster{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, cluster); err != nil {
			return false, nil
		}
		return meta.IsStatusConditionTrue(cluster.Status.Conditions, "TopologyReady"), nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for TopologyReady on cluster %s/%s: %v", ns, name, err)
	}
}
