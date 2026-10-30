package ownership

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

func TestOwnerOf(t *testing.T) {
	orig := Fields
	t.Cleanup(func() { Fields = orig })
	Fields = []Field{
		{Kind: "Shard", Path: ".status.phase", Manager: "phase-owner"},
		{Kind: "Shard", Path: ".status.conditions", Manager: "conditions-owner"},
		{
			Kind:    "Shard",
			Path:    `.status.conditions[type="StorageClassValid"]`,
			Manager: "storage-owner",
		},
		{Kind: "Cell", Path: ".status.phase", Manager: "cell-phase-owner"},
	}

	tests := map[string]struct {
		kind, path string
		want       string
	}{
		"exact match": {"Shard", ".status.phase", "phase-owner"},
		"longest prefix wins": {
			"Shard",
			`.status.conditions[type="StorageClassValid"].status`,
			"storage-owner",
		},
		"shorter row for other type": {
			"Shard",
			`.status.conditions[type="Other"].status`,
			"conditions-owner",
		},
		"sub-field by dot":         {"Shard", ".status.phase.detail", "phase-owner"},
		"boundary miss":            {"Shard", ".status.phaseX", ""},
		"wrong kind misses":        {"TopoServer", ".status.phase", ""},
		"kind selects its own row": {"Cell", ".status.phase", "cell-phase-owner"},
		"unknown path misses":      {"Shard", ".status.message", ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := OwnerOf(tc.kind, tc.path)
			require.Equal(t, tc.want != "", ok)
			require.Equal(t, tc.want, got.Manager)
		})
	}
}

func TestManagersSortedAndDistinct(t *testing.T) {
	m := Managers()
	require.Len(t, m, 9)
	require.True(t, slices.IsSorted(m))
	require.Len(t, slices.Compact(slices.Clone(m)), len(m))
}

func TestIsLegacy(t *testing.T) {
	for _, name := range []string{
		"multigres-operator",
		"multigres-resource-handler",
		"multigres-resource-handler-guard",
		"multigres-resource-handler-readiness",
		"multigres-operator-guard",
	} {
		require.True(t, IsLegacy(name), name)
	}
	for _, name := range Managers() {
		require.False(t, IsLegacy(name), name)
	}
	// certs.FieldOwner shares the legacy name. That is fine: DropLegacy is
	// only called on CRs and Pods, and certs writes Secrets and Certificates.
	require.Equal(t, "multigres-operator", certs.FieldOwner)
	require.True(t, IsLegacy(certs.FieldOwner))
}

func countingClient(t *testing.T) (client.Client, *int) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	calls := 0
	c := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				calls++
				return nil
			},
		},
	)
	return c, &calls
}

func shardWithManagers(managers ...string) *multigresv1alpha1.Shard {
	s := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "default", ResourceVersion: "5"},
	}
	for _, m := range managers {
		s.ManagedFields = append(
			s.ManagedFields,
			metav1.ManagedFieldsEntry{Manager: m, Operation: metav1.ManagedFieldsOperationApply},
		)
	}
	return s
}

func TestDropLegacy_NoLegacyNoRequest(t *testing.T) {
	s := shardWithManagers(ManagerShard, ManagerShardGuard)
	c, calls := countingClient(t)
	require.NoError(t, DropLegacy(context.Background(), c, s))
	require.Zero(t, *calls)
}

func TestDropLegacy_AllLegacyNoRequest(t *testing.T) {
	s := shardWithManagers(LegacyOperator, LegacyResourceHandlerGuard)
	c, calls := countingClient(t)
	require.NoError(t, DropLegacy(context.Background(), c, s))
	require.Zero(t, *calls)
}

func TestDropLegacy_ReturnsPatchErrorUnchanged(t *testing.T) {
	s := shardWithManagers(LegacyOperator, ManagerShard)
	want := apierrors.NewInvalid(schema.GroupKind{Group: "multigres.com", Kind: "Shard"}, "s", nil)
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	c := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return want
			},
		},
	)
	require.Same(t, want, DropLegacy(context.Background(), c, s))
}
