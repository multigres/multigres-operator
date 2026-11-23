package testutil

import (
	"reflect"
	"testing"
)

func TestSetUnstructuredPath(t *testing.T) {
	t.Parallel()

	doc := map[string]any{"spec": map[string]any{"list": []any{map[string]any{}}}}
	if err := SetUnstructuredPath(
		doc,
		[]any{"spec", "list", 0, "resources"},
		map[string]any{},
	); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"spec": map[string]any{"list": []any{
		map[string]any{"resources": map[string]any{}},
	}}}
	if !reflect.DeepEqual(doc, want) {
		t.Errorf("doc = %v, want %v", doc, want)
	}
	for _, path := range [][]any{nil, {"spec", "missing", "x"}, {"spec", "list", 1}, {"spec", 1.5}} {
		if err := SetUnstructuredPath(doc, path, "v"); err == nil {
			t.Errorf("path %v: no error", path)
		}
	}
}
