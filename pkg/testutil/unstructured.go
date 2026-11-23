package testutil

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SetUnstructuredPath sets value at path in obj, an unstructured object's
// content. Each step of path is a map key (string) or a list index (int);
// every step but the last must already exist.
func SetUnstructuredPath(obj any, path []any, value any) error {
	if len(path) == 0 {
		return fmt.Errorf("empty path: nothing to set")
	}
	cur := obj
	for i, step := range path {
		last := i == len(path)-1
		switch key := step.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return fmt.Errorf("step %d (%q): not an object", i, key)
			}
			if last {
				m[key] = value
				return nil
			}
			next, ok := m[key]
			if !ok {
				return fmt.Errorf("step %d (%q): missing", i, key)
			}
			cur = next
		case int:
			l, ok := cur.([]any)
			if !ok || key < 0 || key >= len(l) {
				return fmt.Errorf("step %d (%d): not a list or out of range", i, key)
			}
			if last {
				l[key] = value
				return nil
			}
			cur = l[key]
		default:
			return fmt.Errorf("step %d: %T is neither a key nor an index", i, step)
		}
	}
	panic("unreachable: the last step returns")
}

// UpdateUnstructured reads the object of kind gvk at key, lets set change its
// content, and writes it back with an update, retrying on conflicts. Going
// through unstructured content keeps what set writes exactly as written: a
// typed client would drop a non-nil empty map that is tagged omitempty, so it
// cannot write a shape such as `resources: {limits: {}}`.
func UpdateUnstructured(
	ctx context.Context,
	c client.Client,
	gvk schema.GroupVersionKind,
	key client.ObjectKey,
	set func(content map[string]any) error,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if err := c.Get(ctx, key, u); err != nil {
			return err
		}
		if err := set(u.Object); err != nil {
			return err
		}
		return c.Update(ctx, u)
	})
}
