package topology

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestExternalTopologyKeepsLongRoot(t *testing.T) {
	c := assert.NewCollecting(t)
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-abcdefghijklmnop",
			Namespace: "namespace-abcdefghijklmnopqrstu",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{},
			},
		},
	}
	roots, err := ForCluster(cluster)
	c.Require().NoError(err)
	c.EqDeep(
		"/multigres/namespace-abcdefghijklmnopqrstu/cluster-abcdefghijklmnop",
		roots.ClusterRoot(),
	)
}

func TestRootsWithTopologyTLS(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	const namespace = "namespace-abcdefghijklmnopqrstu"
	const clusterName = "cluster-abcdefghijklmnop"
	const unboundedRoot = "/multigres/" + namespace + "/" + clusterName
	const hashedRoot = "/multigres-fallback/b-Tmo_r9oWzDWEuz_6f6LFOAmYO7ve1i4ksIy7qa9ac"

	for _, tc := range []struct {
		name        string
		namespace   string
		clusterName string
		topoTLS     bool
		want        string
	}{
		{"67 byte TLS fallback", namespace, clusterName, true, hashedRoot},
		{"plaintext keeps existing root", namespace, clusterName, false, unboundedRoot},
		{"64 byte TLS fallback is unchanged", namespace, clusterName[:21], true, unboundedRoot[:64]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			roots, err := NewRoots(nil, tc.namespace, tc.clusterName, tc.topoTLS)
			c.Require().NoError(err)
			c.EqDeep(tc.want, roots.ClusterRoot())
			c.EqDeep(tc.want+"/global", roots.Global())
			cellRoot, err := roots.Cell("zone-a")
			c.Require().NoError(err)
			c.EqDeep(tc.want+"/zone-a", cellRoot)
			c.True(strings.HasPrefix(cellRoot, roots.KeyPrefix()))
			if tc.topoTLS {
				c.LessOrEqual(certs.MaxCommonNameBytes, len(roots.ClusterRoot()))
			}
		})
	}

	for _, ref := range []string{"~", "namespace-abcdefghijklmnopqrstu", "../multigres-fallback", strings.Repeat("p", 64)} {
		annotations := map[string]string{metadata.AnnotationProjectRef: ref}
		plain, err := NewRoots(annotations, namespace, clusterName, false)
		c.Require().NoError(err)
		tls, err := NewRoots(annotations, namespace, clusterName, true)
		c.Require().NoError(err)
		c.EqDeep(plain, tls, "explicit refs must not change with TLS")
		c.NotEqDeep(hashedRoot, tls.ClusterRoot())
		c.False(strings.HasPrefix(hashedRoot+"/global", tls.KeyPrefix()))
	}

	seen := map[string]bool{hashedRoot: true}
	for _, pair := range [][2]string{
		{namespace + "x", clusterName},
		{namespace, clusterName + "x"},
		{namespace + "/a", clusterName},
		{namespace, "a/" + clusterName},
		{strings.Repeat("n", 63), strings.Repeat("c", 253)},
	} {
		roots, err := NewRoots(nil, pair[0], pair[1], true)
		c.Require().NoError(err)
		c.LessOrEqual(certs.MaxCommonNameBytes, len(roots.ClusterRoot()))
		c.False(seen[roots.ClusterRoot()], "fallback identities must be distinct")
		seen[roots.ClusterRoot()] = true
	}
}

func TestRoots(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		annotations map[string]string
		namespace   string
		clusterName string
		cellName    string
		wantGlobal  string
		wantCell    string
		wantErr     bool
	}{
		"project reference is the stable identity": {
			annotations: map[string]string{metadata.AnnotationProjectRef: "proj_123"},
			namespace:   "ignored",
			clusterName: "ignored",
			cellName:    "eu-west-1",
			wantGlobal:  "/multigres/proj_123/global",
			wantCell:    "/multigres/proj_123/eu-west-1",
		},
		"namespace and name are the fallback identity": {
			namespace:   "customer-a",
			clusterName: "production",
			cellName:    "eu-west-1",
			wantGlobal:  "/multigres/customer-a/production/global",
			wantCell:    "/multigres/customer-a/production/eu-west-1",
		},
		"path separators and dot segments are encoded": {
			annotations: map[string]string{metadata.AnnotationProjectRef: "team/project"},
			cellName:    "..",
			wantGlobal:  "/multigres/team%2Fproject/global",
			wantCell:    "/multigres/team%2Fproject/%2E%2E",
		},
		"empty fallback namespace is rejected": {
			clusterName: "cluster",
			cellName:    "cell",
			wantErr:     true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)
			roots, err := NewRoots(tc.annotations, tc.namespace, tc.clusterName, false)
			if tc.wantErr {
				c.Require().Error(err, "expected an error")
				return
			}
			c.Require().NoError(err, "NewRoots")
			cell, err := roots.Cell(tc.cellName)
			c.Require().NoError(err, "Cell")
			c.Eq(tc.wantGlobal, roots.Global(), "Global()")
			c.Eq(tc.wantCell, cell, "Cell()")
			c.NotEq(cell, roots.Global(), "global and cell roots must be disjoint")
		})
	}
}

func TestFallbackIdentityIsNamespaceScoped(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	first, err := NewRoots(nil, "tenant-a", "production", false)
	c.NoError(err)
	second, err := NewRoots(nil, "tenant-b", "production", false)
	c.NoError(err)
	c.NotEq(
		second.Global(),
		first.Global(),
		"equal cluster names in different namespaces collided at",
	)
}

func TestClusterRootPrefixesEveryRoot(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		namespace   string
		clusterName string
		want        string
	}{
		"project ref": {
			annotations: map[string]string{metadata.AnnotationProjectRef: "proj_123"},
			namespace:   "supabase",
			clusterName: "test-cluster",
			want:        "/multigres/proj_123",
		},
		"namespace and name fallback": {
			namespace:   "supabase",
			clusterName: "test-cluster",
			want:        "/multigres/supabase/test-cluster",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			roots, err := NewRoots(tc.annotations, tc.namespace, tc.clusterName, false)
			c.Require().NoError(err, "NewRoots() error =")
			c.Eq(tc.want, roots.ClusterRoot(), "ClusterRoot()")
			c.Eq(roots.ClusterRoot()+"/global", roots.Global(), "Global()")
			cell, err := roots.Cell("zone-a")
			c.Require().NoError(err, "Cell() error =")
			c.Eq(roots.ClusterRoot()+"/zone-a", cell, "Cell()")
		})
	}
}

// A range opened at the bare cluster root would reach a sibling whose identity
// merely starts with the same characters, so authorization grants on the
// prefix that includes the separator.
func TestKeyPrefixDoesNotReachSiblingClusters(t *testing.T) {
	c := assert.NewAborting(t)
	short, err := NewRoots(
		map[string]string{metadata.AnnotationProjectRef: "proj_123"}, "supabase", "a", false,
	)
	c.NoError(err, "NewRoots() error =")
	long, err := NewRoots(
		map[string]string{metadata.AnnotationProjectRef: "proj_1234"}, "supabase", "b", false,
	)
	c.NoError(err, "NewRoots() error =")

	if !strings.HasPrefix(long.ClusterRoot(), short.ClusterRoot()) {
		t.Fatal("fixtures no longer exercise the sibling prefix hazard")
	}
	if strings.HasPrefix(long.Global(), short.KeyPrefix()) {
		t.Errorf(
			"KeyPrefix %q reaches sibling key %q",
			short.KeyPrefix(), long.Global(),
		)
	}
	if !strings.HasPrefix(short.Global(), short.KeyPrefix()) {
		t.Errorf("KeyPrefix %q does not cover its own keys", short.KeyPrefix())
	}
}
