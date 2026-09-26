//go:build integration

package webhook_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

func TestCEL_EtcdMaintenance(t *testing.T) {
	c := getPrivilegedClient(t)
	for _, tc := range []struct {
		name      string
		config    map[string]any
		wantError string
	}{
		{"default", map[string]any{}, ""},
		{"periodic", map[string]any{"autoCompactionRetention": "30m"}, ""},
		{"revision", map[string]any{"autoCompactionMode": "revision", "autoCompactionRetention": "5000"}, ""},
		{"quota", map[string]any{"quotaBackendBytes": int64(512 << 20), "defragmentationEnabled": true}, ""},
		{"zero-retention", map[string]any{"autoCompactionRetention": "0h"}, "positive"},
		{"bad-duration", map[string]any{"autoCompactionRetention": "forever"}, "compaction retention"},
		{"bad-revision", map[string]any{"autoCompactionMode": "revision", "autoCompactionRetention": "1h"}, "compaction retention"},
		{"negative-revision", map[string]any{"autoCompactionMode": "revision", "autoCompactionRetention": "-1"}, "compaction retention"},
		{"bad-mode", map[string]any{"autoCompactionMode": "disabled"}, "Unsupported value"},
		{"zero-quota", map[string]any{"quotaBackendBytes": int64(0)}, "greater than or equal"},
		{"large-quota", map[string]any{"quotaBackendBytes": int64(9 << 30)}, "less than or equal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ck := assert.NewCollecting(t)
			obj := &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "multigres.com/v1alpha1",
					"kind":       "TopoServer",
					"metadata": map[string]any{
						"name":      "maintenance-" + tc.name,
						"namespace": testNamespace,
					},
					"spec": map[string]any{
						"etcd": map[string]any{"replicas": int64(3), "maintenance": tc.config},
					},
				},
			}
			err := c.Create(t.Context(), obj)
			if tc.wantError != "" {
				ck.Require().
					False(err == nil || !strings.Contains(err.Error(), tc.wantError), "expected %q, got %v", tc.wantError, err)
				return
			}
			ck.Require().NoError(err)
			t.Cleanup(func() { _ = c.Delete(t.Context(), obj) })
			stored := &unstructured.Unstructured{}
			stored.SetGroupVersionKind(obj.GroupVersionKind())
			ck.Require().NoError(c.Get(t.Context(), client.ObjectKeyFromObject(obj), stored))
			for key, want := range tc.config {
				got, found, err := unstructured.NestedFieldNoCopy(
					stored.Object,
					"spec",
					"etcd",
					"maintenance",
					key,
				)
				ck.False(
					err != nil || !found || got != want,
					"%s: got %v, want %v (err %v)",
					key,
					got,
					want,
					err,
				)
			}
		})
	}
}
