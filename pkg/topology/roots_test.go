package topology

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestExternalTopologyRoots(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		external *multigresv1alpha1.ExternalTopoServerSpec
		want     string
	}{
		// The server brings its own credentials, so the root is no
		// certificate's common name and keeps its full length.
		"own secrets keep the long root": {
			//nolint:gosec // K8s resource names, not credentials
			external: &multigresv1alpha1.ExternalTopoServerSpec{
				CASecret:         "infra-etcd-ca",
				ClientCertSecret: "infra-etcd-client",
			},
			want: "/multigres/namespace-abcdefghijklmnopqrstu/cluster-abcdefghijklmnop",
		},
		// The operator issues the client certificate, so the root is its
		// common name and is bounded like a managed topology root.
		"no secrets bound the root": {
			external: &multigresv1alpha1.ExternalTopoServerSpec{},
			want:     "/multigres-fallback/b-Tmo_r9oWzDWEuz_6f6LFOAmYO7ve1i4ksIy7qa9ac",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cluster-abcdefghijklmnop",
					Namespace: "namespace-abcdefghijklmnopqrstu",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						External: tc.external,
					},
				},
			}
			roots, err := ForCluster(cluster)
			require.NoError(t, err)
			assert.Equal(t, tc.want, roots.ClusterRoot())
		})
	}
}

func TestRootsWithTopologyTLS(t *testing.T) {
	t.Parallel()

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
			roots, err := NewRoots(nil, tc.namespace, tc.clusterName, tc.topoTLS)
			require.NoError(t, err)
			assert.Equal(t, tc.want, roots.ClusterRoot())
			assert.Equal(t, tc.want+"/global", roots.Global())
			cellRoot, err := roots.Cell("zone-a")
			require.NoError(t, err)
			assert.Equal(t, tc.want+"/zone-a", cellRoot)
			assert.True(t, strings.HasPrefix(cellRoot, roots.KeyPrefix()))
			if tc.topoTLS {
				assert.LessOrEqual(t, len(roots.ClusterRoot()), certs.MaxCommonNameBytes)
			}
		})
	}

	for _, ref := range []string{"~", "namespace-abcdefghijklmnopqrstu", "../multigres-fallback", strings.Repeat("p", 64)} {
		annotations := map[string]string{metadata.AnnotationProjectRef: ref}
		plain, err := NewRoots(annotations, namespace, clusterName, false)
		require.NoError(t, err)
		tls, err := NewRoots(annotations, namespace, clusterName, true)
		require.NoError(t, err)
		assert.Equal(t, plain, tls, "explicit refs must not change with TLS")
		assert.NotEqual(t, hashedRoot, tls.ClusterRoot())
		assert.False(t, strings.HasPrefix(hashedRoot+"/global", tls.KeyPrefix()))
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
		require.NoError(t, err)
		assert.LessOrEqual(t, len(roots.ClusterRoot()), certs.MaxCommonNameBytes)
		assert.False(t, seen[roots.ClusterRoot()], "fallback identities must be distinct")
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
			roots, err := NewRoots(tc.annotations, tc.namespace, tc.clusterName, false)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRoots: %v", err)
			}
			cell, err := roots.Cell(tc.cellName)
			if err != nil {
				t.Fatalf("Cell: %v", err)
			}
			if got := roots.Global(); got != tc.wantGlobal {
				t.Errorf("Global() = %q, want %q", got, tc.wantGlobal)
			}
			if cell != tc.wantCell {
				t.Errorf("Cell() = %q, want %q", cell, tc.wantCell)
			}
			if roots.Global() == cell {
				t.Error("global and cell roots must be disjoint")
			}
		})
	}
}

func TestFallbackIdentityIsNamespaceScoped(t *testing.T) {
	t.Parallel()

	first, err := NewRoots(nil, "tenant-a", "production", false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRoots(nil, "tenant-b", "production", false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Global() == second.Global() {
		t.Fatalf("equal cluster names in different namespaces collided at %q", first.Global())
	}
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
			roots, err := NewRoots(tc.annotations, tc.namespace, tc.clusterName, false)
			if err != nil {
				t.Fatalf("NewRoots() error = %v", err)
			}
			if got := roots.ClusterRoot(); got != tc.want {
				t.Errorf("ClusterRoot() = %q, want %q", got, tc.want)
			}
			if want := roots.ClusterRoot() + "/global"; roots.Global() != want {
				t.Errorf("Global() = %q, want %q", roots.Global(), want)
			}
			cell, err := roots.Cell("zone-a")
			if err != nil {
				t.Fatalf("Cell() error = %v", err)
			}
			if want := roots.ClusterRoot() + "/zone-a"; cell != want {
				t.Errorf("Cell() = %q, want %q", cell, want)
			}
		})
	}
}

// A range opened at the bare cluster root would reach a sibling whose identity
// merely starts with the same characters, so authorization grants on the
// prefix that includes the separator.
func TestKeyPrefixDoesNotReachSiblingClusters(t *testing.T) {
	short, err := NewRoots(
		map[string]string{metadata.AnnotationProjectRef: "proj_123"}, "supabase", "a", false,
	)
	if err != nil {
		t.Fatalf("NewRoots() error = %v", err)
	}
	long, err := NewRoots(
		map[string]string{metadata.AnnotationProjectRef: "proj_1234"}, "supabase", "b", false,
	)
	if err != nil {
		t.Fatalf("NewRoots() error = %v", err)
	}

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
