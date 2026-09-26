package multigrescluster

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/multigres/testkit/assert"
)

func TestBuildGlobalTopoServer_Errors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	// Test case: GlobalTopoServerSpec is not nil, but both Etcd and External are nil.
	// This hits the "else { return error }" branch in BuildGlobalTopoServer.
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: nil,
		},
	}

	_, err := BuildGlobalTopoServer(cluster, cluster.Spec.GlobalTopoServer, scheme)
	assert.NewCollecting(t).NoError(err, "Expected nil error for nil GlobalTopoServerSpec, got")
}
