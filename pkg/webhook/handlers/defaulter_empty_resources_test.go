package handlers

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// TestDefaulterKeepsEmptyResourcesBlocks admits a cluster that uses no
// templates, so the defaulter writes the resolved components back into its
// spec, with every resources block set to a shape that asks for nothing. It
// goes through controller-runtime's admission handler, which turns the
// defaulted object into the JSON patch the API server applies, and the test
// applies that patch: every block must be stored exactly as written. An
// emptied requests, limits or claims member means "no requests or limits";
// with the member or the block removed, the next resolution would install the
// default requests and limits instead.
func TestDefaulterKeepsEmptyResourcesBlocks(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	res := resolver.NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "no-templates")
	handler := admission.WithCustomDefaulter(scheme, &multigresv1alpha1.MultigresCluster{},
		NewMultigresClusterDefaulter(res))

	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		for name, shape := range map[string]map[string]any{
			"empty requests": {"requests": map[string]any{}},
			"empty limits":   {"limits": map[string]any{}},
			"empty claims":   {"claims": []any{}},
		} {
			cluster := map[string]any{
				"apiVersion": multigresv1alpha1.GroupVersion.String(),
				"kind":       "MultigresCluster",
				"metadata":   map[string]any{"name": "c", "namespace": "no-templates"},
				"spec": map[string]any{
					"mode": mode,
					"globalTopoServer": map[string]any{
						"etcd": map[string]any{"resources": shape},
					},
					"cells": []any{map[string]any{
						"name": "zone-a",
						"spec": map[string]any{
							"multigateway": map[string]any{"resources": shape},
						},
					}},
					"databases": []any{map[string]any{
						"name": "postgres", "default": true,
						"tablegroups": []any{map[string]any{
							"name": "default", "default": true,
							"shards": []any{map[string]any{
								"name": "0-inf",
								"spec": map[string]any{
									"multiorch": map[string]any{"resources": shape},
									"pools": map[string]any{"default": map[string]any{
										"type":                "readWrite",
										"cells":               []any{"zone-a"},
										"postgres":            map[string]any{"resources": shape},
										"multipooler":         map[string]any{"resources": shape},
										"pgbackrestResources": shape,
									}},
								},
							}},
						}},
					}},
				},
			}
			raw, err := json.Marshal(cluster)
			if err != nil {
				t.Fatal(err)
			}
			resp := handler.Handle(t.Context(), admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: raw},
				},
			})
			if !resp.Allowed {
				t.Fatalf("mode %q, %s: admission denied: %v", mode, name, resp.Result)
			}
			// The cluster uses no template, so defaulting writes the resolved
			// components back; with no patches at all nothing was checked.
			if len(resp.Patches) == 0 {
				t.Fatalf("mode %q, %s: defaulting changed nothing", mode, name)
			}
			patch, err := json.Marshal(resp.Patches)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := jsonpatch.DecodePatch(patch)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := decoded.Apply(raw)
			if err != nil {
				t.Fatalf("mode %q, %s: applying the admission patch: %v", mode, name, err)
			}
			var stored map[string]any
			if err := json.Unmarshal(admitted, &stored); err != nil {
				t.Fatal(err)
			}
			shard := []any{"spec", "databases", 0, "tablegroups", 0, "shards", 0, "spec"}
			for _, path := range [][]any{
				{"spec", "globalTopoServer", "etcd", "resources"},
				{"spec", "cells", 0, "spec", "multigateway", "resources"},
				append(slices.Clone(shard), "multiorch", "resources"),
				append(slices.Clone(shard), "pools", "default", "postgres", "resources"),
				append(slices.Clone(shard), "pools", "default", "multipooler", "resources"),
				append(slices.Clone(shard), "pools", "default", "pgbackrestResources"),
			} {
				if got := jsonAt(stored, path); !reflect.DeepEqual(got, any(shape)) {
					t.Errorf("mode %q, %s: admitted %v = %v, want %v", mode, name, path, got, shape)
				}
			}
		}
	}
}

// jsonAt returns the value at path in a decoded JSON document, or nil.
func jsonAt(doc any, path []any) any {
	for _, step := range path {
		switch key := step.(type) {
		case string:
			m, ok := doc.(map[string]any)
			if !ok {
				return nil
			}
			doc = m[key]
		case int:
			l, ok := doc.([]any)
			if !ok || key >= len(l) {
				return nil
			}
			doc = l[key]
		}
	}
	return doc
}
