//go:build integration
// +build integration

package ownership_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// countingClient counts the patches that reach the API server.
type countingClient struct {
	client.Client
	patches int
}

func (c *countingClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	c.patches++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *countingClient) Status() client.SubResourceWriter {
	return &countingStatus{SubResourceWriter: c.Client.Status(), c: c}
}

type countingStatus struct {
	client.SubResourceWriter
	c *countingClient
}

func (s *countingStatus) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	s.c.patches++
	return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

const applier = "noop-apply-test"

// TestSkipNoopApplies applies through SkipNoopApplies against a real API
// server and requires every apply that changes nothing to stay off the wire,
// and every apply that changes something to be sent: a changed value, a field
// the manager stops setting, a field it starts setting, and a value another
// writer changed, which takes the field off the manager's entry.
func TestSkipNoopApplies(t *testing.T) {
	ctx := t.Context()
	base := newEnvClient(t)
	counted := &countingClient{Client: base}
	c := ownership.SkipNoopApplies(counted)

	configMap := func(labels, data map[string]string) *corev1.ConfigMap {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "skip", Namespace: "default", Labels: labels},
			Data:       data,
		}
		cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
		return cm
	}
	apply := func(obj client.Object, wantSent bool, why string) {
		t.Helper()
		before := counted.patches
		require.NoError(t, c.Patch(ctx, obj, client.Apply, client.FieldOwner(applier),
			client.ForceOwnership))
		require.Equal(t, wantSent, counted.patches > before, why)
		require.NotEmpty(t, obj.GetResourceVersion(), "%s: obj not filled", why)
	}

	labels := map[string]string{"a": "1", "b": "2"}
	apply(configMap(labels, map[string]string{"k": "v"}), true, "create")
	apply(configMap(labels, map[string]string{"k": "v"}), false, "same content")
	apply(configMap(labels, map[string]string{"k": "w"}), true, "changed value")
	apply(configMap(labels, map[string]string{"k": "w"}), false, "same again")
	apply(configMap(map[string]string{"a": "1"}, map[string]string{"k": "w"}), true,
		"a label no longer set")
	apply(configMap(map[string]string{"a": "1"}, map[string]string{"k": "w", "n": "x"}), true,
		"a key newly set")

	// Another writer changes a value this manager set. The update takes the
	// field off the manager's entry, so the unchanged desired content is
	// applied again and puts the value back.
	edited := &corev1.ConfigMap{}
	require.NoError(t, base.Get(ctx, client.ObjectKey{Namespace: "default", Name: "skip"}, edited))
	edited.Data["k"] = "edited"
	require.NoError(t, base.Update(ctx, edited, client.FieldOwner("kubectl-edit")))
	desired := configMap(map[string]string{"a": "1"}, map[string]string{"k": "w", "n": "x"})
	apply(desired, true, "a value another writer changed")
	require.Equal(t, "w", desired.Data["k"])

	// Keyed list elements, with integer keys.
	service := func(port int32) *corev1.Service {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "skip", Namespace: "default"},
			Spec: corev1.ServiceSpec{
				ClusterIP: corev1.ClusterIPNone,
				Ports: []corev1.ServicePort{{
					Name: "pg", Port: port, Protocol: corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(port),
				}},
				Selector: map[string]string{"app": "skip"},
			},
		}
		svc.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
		return svc
	}
	apply(service(5432), true, "create service")
	apply(service(5432), false, "same service")
	apply(service(5433), true, "changed port")

	// The status subresource, through ApplyStatus.
	shard := newShard("skip-status")
	require.NoError(t, base.Create(ctx, shard))
	setStatus := func(phase multigresv1alpha1.Phase, wantSent bool, why string) {
		t.Helper()
		s := newShard("skip-status")
		s.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("Shard"))
		s.Status.Phase = phase
		before := counted.patches
		require.NoError(t, ownership.ApplyStatus(ctx, c, s, applier))
		require.Equal(t, wantSent, counted.patches > before, why)
	}
	setStatus(multigresv1alpha1.PhaseProgressing, true, "first status")
	setStatus(multigresv1alpha1.PhaseProgressing, false, "same status")
	setStatus(multigresv1alpha1.PhaseHealthy, true, "changed status")

	// An apply carrying a resourceVersion is a precondition only the API
	// server can check, so it is sent even when the cache shows its content
	// already stored, and a stale one fails.
	conditional := configMap(map[string]string{"a": "1"}, map[string]string{"k": "w", "n": "x"})
	conditional.ResourceVersion = "1"
	err := c.Patch(
		ctx,
		conditional,
		client.Apply,
		client.FieldOwner(applier),
		client.ForceOwnership,
	)
	require.True(t, apierrors.IsConflict(err), "a stale precondition conflicts, got %v", err)

	// A raw apply patch carries its own content, which need not be the
	// object's, so it is sent as given.
	// The object passed carries what is stored, so comparing it rather than
	// the patch would skip the patch.
	raw := configMap(map[string]string{"a": "1"}, map[string]string{"k": "w", "n": "x"})
	before := counted.patches
	require.NoError(t, c.Patch(ctx, raw, client.RawPatch(
		types.ApplyPatchType,
		[]byte(
			`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"skip","namespace":"default",`+
				`"labels":{"a":"1"}},"data":{"k":"raw","n":"x"}}`,
		),
	),
		client.FieldOwner(applier), client.ForceOwnership))
	require.Equal(t, before+1, counted.patches, "a raw apply is sent")
	require.Equal(t, "raw", raw.Data["k"])
}

// noEntries reads objects without their managedFields, so no apply through
// it can be proven a no-op and every one is sent.
type noEntries struct {
	client.Client
}

func (n noEntries) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	if err := n.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	obj.SetManagedFields(nil)
	return nil
}

// TestApplyReportsChange requires Apply to report a change only when the
// object changed: an apply that is sent and stored unchanged is not one, or a
// caller recording an event per change would record one for it.
func TestApplyReportsChange(t *testing.T) {
	ctx := t.Context()
	base := newEnvClient(t)

	configMap := func(v string) *corev1.ConfigMap {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "apply-change", Namespace: "default"},
			Data:       map[string]string{"k": v},
		}
		cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
		return cm
	}
	apply := func(c client.Client, cm *corev1.ConfigMap, wantChanged bool, why string) {
		t.Helper()
		changed, err := ownership.Apply(ctx, c, cm)
		require.NoError(t, err)
		require.Equal(t, wantChanged, changed, why)
	}

	counted := &countingClient{Client: base}
	apply(counted, configMap("a"), true, "creating the object is a change")
	apply(counted, configMap("b"), true, "a new value is a change")
	before := counted.patches
	apply(counted, configMap("b"), false, "a repeat is not a change")
	require.Equal(t, before, counted.patches, "a repeat is not sent")

	// This client's own apply that its cache has not yet seen: a repeat is
	// sent, since the cache cannot show it stored, and the API server stores
	// nothing new.
	lagging := &testutil.HeldReads{Client: base}
	lagging.Hold(t, configMap("b"))
	wrapped := ownership.SkipNoopApplies(&countingClient{Client: lagging})
	apply(wrapped, configMap("c"), true, "a new value under a lagging cache is a change")
	apply(wrapped, configMap("c"), false, "a repeat under a lagging cache is not a change")

	blind := &countingClient{Client: noEntries{Client: base}}
	apply(blind, configMap("c"), false, "a repeat the server stores unchanged is not a change")
	require.Equal(t, 1, blind.patches, "the repeat is sent when it cannot be proven a no-op")
}

// storedThenFailed sends the next patch, then returns an error as if its
// response had been lost, once armed.
type storedThenFailed struct {
	client.Client
	armed bool
}

func (s *storedThenFailed) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	err := s.Client.Patch(ctx, obj, patch, opts...)
	if err == nil && s.armed {
		s.armed = false
		return errors.New("connection reset by peer")
	}
	return err
}

// notFoundReads reads every object as missing while missing is set, as a
// cache that has not yet seen a newly created one does.
type notFoundReads struct {
	client.Client
	missing bool
}

func (r *notFoundReads) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	if r.missing {
		return apierrors.NewNotFound(corev1.Resource("configmaps"), key.Name)
	}
	return r.Client.Get(ctx, key, obj, opts...)
}

// TestSkipNoopAppliesWriteMemory covers what the wrapper remembers of its own
// writes against a cache held at one snapshot: a write whose response was
// lost, a raw apply, a precondition on the object's UID, and an Applied
// report for a repeat the API server stores unchanged.
func TestSkipNoopAppliesWriteMemory(t *testing.T) {
	ctx := t.Context()
	base := newEnvClient(t)
	configMap := func(name, v string) *corev1.ConfigMap {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Data:       map[string]string{"k": v},
		}
		cm.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
		return cm
	}
	stored := func(name string) string {
		t.Helper()
		got := &corev1.ConfigMap{}
		require.NoError(t, base.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, got))
		return got.Data["k"]
	}
	apply := func(c client.Client, cm *corev1.ConfigMap) error {
		return c.Patch(ctx, cm, client.Apply, client.FieldOwner(applier), client.ForceOwnership)
	}

	t.Run("a write whose response was lost", func(t *testing.T) {
		held := &testutil.HeldReads{Client: base}
		failing := &storedThenFailed{Client: held}
		c := ownership.SkipNoopApplies(failing)
		require.NoError(t, apply(c, configMap("lost", "A")))
		held.Hold(t, configMap("lost", ""))
		failing.armed = true
		require.Error(t, apply(c, configMap("lost", "B")))
		require.Equal(t, "B", stored("lost"), "the failed apply was stored")
		require.NoError(t, apply(c, configMap("lost", "A")))
		require.Equal(t, "A", stored("lost"), "the next apply is sent whatever the cache holds")
	})

	t.Run("a raw apply", func(t *testing.T) {
		held := &testutil.HeldReads{Client: base}
		c := ownership.SkipNoopApplies(held)
		require.NoError(t, apply(c, configMap("raw-memory", "A")))
		held.Hold(t, configMap("raw-memory", ""))
		raw := configMap("raw-memory", "")
		require.NoError(t, c.Patch(ctx, raw, client.RawPatch(types.ApplyPatchType,
			[]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"raw-memory",`+
				`"namespace":"default"},"data":{"k":"B"}}`)),
			client.FieldOwner(applier), client.ForceOwnership))
		require.Equal(t, "B", stored("raw-memory"))
		require.NoError(t, apply(c, configMap("raw-memory", "A")))
		require.Equal(t, "A", stored("raw-memory"), "the raw write is remembered")
	})

	t.Run("a UID precondition", func(t *testing.T) {
		counted := &countingClient{Client: base}
		c := ownership.SkipNoopApplies(counted)
		require.NoError(t, apply(c, configMap("uid", "A")))
		current := &corev1.ConfigMap{}
		require.NoError(
			t,
			base.Get(ctx, client.ObjectKey{Namespace: "default", Name: "uid"}, current),
		)
		cm := configMap("uid", "A")
		cm.UID = "not-the-stored-uid"
		before := counted.patches
		require.Error(t, apply(c, cm), "only the API server can check a UID")
		require.Equal(t, before+1, counted.patches)
	})

	t.Run("a repeat before the cache shows the create", func(t *testing.T) {
		c := ownership.SkipNoopApplies(&notFoundReads{Client: base, missing: true})
		changed, err := ownership.Apply(ctx, c, configMap("created", "A"))
		require.NoError(t, err)
		require.True(t, changed, "the create is a change")
		changed, err = ownership.Apply(ctx, c, configMap("created", "A"))
		require.NoError(t, err)
		require.False(t, changed, "the repeat returns the create's resourceVersion")
	})

	t.Run("an empty apply after a write the cache lacks", func(t *testing.T) {
		held := &testutil.HeldReads{Client: base}
		c := ownership.SkipNoopApplies(held)
		require.NoError(t, apply(c, configMap("clear", "A")))
		held.Hold(t, configMap("clear", ""))
		extra := configMap("clear", "")
		extra.Data = map[string]string{"extra": "x"}
		require.NoError(t, c.Patch(ctx, extra, client.Apply, client.FieldOwner("clearer")))
		empty := configMap("clear", "")
		empty.Data = nil
		require.NoError(t, c.Patch(ctx, empty, client.Apply, client.FieldOwner("clearer")))
		got := &corev1.ConfigMap{}
		require.NoError(
			t,
			base.Get(ctx, client.ObjectKey{Namespace: "default", Name: "clear"}, got),
		)
		require.NotContains(
			t,
			got.Data,
			"extra",
			"the empty apply removes what the cache never showed",
		)
	})

	t.Run("a read that misses keeps later writes", func(t *testing.T) {
		held := &testutil.HeldReads{Client: base}
		reads := &notFoundReads{Client: held}
		c := ownership.SkipNoopApplies(reads)
		require.NoError(t, apply(c, configMap("missed", "A")))
		held.Hold(t, configMap("missed", ""))
		require.NoError(t, apply(c, configMap("missed", "B")))
		reads.missing = true
		require.NoError(t, apply(c, configMap("missed", "B")))
		reads.missing = false
		require.NoError(t, apply(c, configMap("missed", "A")))
		require.Equal(t, "A", stored("missed"), "the cached create does not stand in for B")
	})
}
