package resolver

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolver_ResolveCell(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_, cellTpl, _, ns := setupFixtures(t)

	tests := map[string]struct {
		config        *multigresv1alpha1.CellConfig
		objects       []client.Object
		wantGw        *multigresv1alpha1.StatelessSpec
		wantPlacement *multigresv1alpha1.PodPlacementSpec
		wantTopo      *multigresv1alpha1.LocalTopoServerSpec
		wantErr       bool
	}{
		"Template Found": {
			config: &multigresv1alpha1.CellConfig{
				CellTemplate: "default",
			},
			objects: []client.Object{cellTpl},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(3)),
				// Expect default resources to be applied
				Resources: DefaultResourcesGateway(),
			},
			wantTopo: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "local-etcd-default",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  "/multigres/",
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Template Not Found (Error)": {
			config: &multigresv1alpha1.CellConfig{
				CellTemplate: "missing",
			},
			wantErr: true,
		},
		"Inline Overrides": {
			config: &multigresv1alpha1.CellConfig{
				Spec: &multigresv1alpha1.CellInlineSpec{
					Multigateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(3)),
					},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(3)),
				// Expect default resources to be applied here too
				Resources: DefaultResourcesGateway(),
			},
			wantTopo: nil, // Inline spec didn't provide one
		},
		"Inline External Topo": {
			config: &multigresv1alpha1.CellConfig{ // Needs name for path generation
				Name: "cell1",
				Spec: &multigresv1alpha1.CellInlineSpec{
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						External: &multigresv1alpha1.ExternalTopoServerSpec{
							CASecret: "my-secret",
						},
					},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(1)),
				Resources: DefaultResourcesGateway(),
			},
			wantTopo: &multigresv1alpha1.LocalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					CASecret:       "my-secret",
					RootPath:       "/multigres/cell1", // Expect defaulted path
					Implementation: "etcd",
				},
			},
		},
		"Client Error": {
			config: &multigresv1alpha1.CellConfig{CellTemplate: "any"},
			// Will use mock client logic inside test runner
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var c client.Client
			if name == "Client Error" {
				base := fake.NewClientBuilder().WithScheme(scheme).Build()
				c = testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
					OnGet: func(_ client.ObjectKey) error { return errors.New("fail") },
				})
			} else {
				c = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(tc.objects...).
					Build()
			}
			r := NewResolver(c, ns)

			gw, placement, topo, err := r.ResolveCell(t.Context(), tc.config)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if diff := cmp.Diff(
				tc.wantGw,
				gw,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Gateway Diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantPlacement, placement, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Placement Diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(
				tc.wantTopo,
				topo,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Topo Diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolver_ResolveCellTemplate(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	_, cellTpl, _, ns := setupFixtures(t)
	customCell := cellTpl.DeepCopy()
	customCell.Name = "custom-cell"

	tests := map[string]struct {
		existingObjects []client.Object
		defaults        multigresv1alpha1.TemplateDefaults
		reqName         multigresv1alpha1.TemplateRef
		wantErr         bool
		errContains     string
		wantFound       bool
		wantResName     string
	}{
		"Explicit Found": {
			existingObjects: []client.Object{customCell},
			reqName:         "custom-cell",
			wantFound:       true,
			wantResName:     "custom-cell",
		},
		"Explicit Not Found (Error)": {
			existingObjects: []client.Object{},
			reqName:         "missing-cell",
			wantErr:         true,
			errContains:     "referenced CellTemplate 'missing-cell' not found",
		},
		"Implicit Fallback Found": {
			existingObjects: []client.Object{cellTpl},
			defaults:        multigresv1alpha1.TemplateDefaults{},
			reqName:         "",
			wantFound:       true,
			wantResName:     "default",
		},
		"Implicit Fallback Not Found (Safe Empty Return)": {
			existingObjects: []client.Object{},
			defaults:        multigresv1alpha1.TemplateDefaults{},
			reqName:         "",
			wantFound:       false,
			wantErr:         false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.existingObjects...).
				Build()
			r := NewResolver(c, ns)

			res, err := r.ResolveCellTemplate(t.Context(), tc.reqName)
			if tc.wantErr {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf(
						"Error message mismatch: got %q, want substring %q",
						err.Error(),
						tc.errContains,
					)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !tc.wantFound {
				if res == nil {
					t.Fatal(
						"Expected non-nil result structure even for not-found implicit fallback",
					)
				}
				if res.GetName() != "" {
					t.Errorf("Expected empty result, got object with name %q", res.GetName())
				}
				return
			}

			if got, want := res.GetName(), tc.wantResName; got != want {
				t.Errorf("Result name mismatch: got %q, want %q", got, want)
			}
		})
	}
}

func TestMergeCellConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		tpl           *multigresv1alpha1.CellTemplate
		overrides     *multigresv1alpha1.CellOverrides
		inline        *multigresv1alpha1.CellInlineSpec
		wantGw        *multigresv1alpha1.StatelessSpec
		wantPlacement *multigresv1alpha1.PodPlacementSpec
		wantTopo      *multigresv1alpha1.LocalTopoServerSpec
	}{
		"Full Merge With Resources and Affinity Overrides": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					Multigateway: &multigresv1alpha1.StatelessSpec{
						Replicas:       ptr.To(int32(3)),
						PodAnnotations: map[string]string{"foo": "bar"},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("100m")},
						},
					},
					LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "base"},
					},
				},
			},
			overrides: &multigresv1alpha1.CellOverrides{
				Multigateway: &multigresv1alpha1.StatelessSpec{
					Replicas:       ptr.To(int32(2)),
					PodAnnotations: map[string]string{"baz": "qux"},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("200m")},
					},
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{},
					},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas:       ptr.To(int32(2)),
				PodAnnotations: map[string]string{"foo": "bar", "baz": "qux"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("200m")},
				},
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{},
				},
			},
			wantTopo: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "base"},
			},
		},
		"Template Only (Nil Overrides)": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					Multigateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
				},
			},
			overrides: nil,
			wantGw:    &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
		},
		"Preserve Base (Empty Override)": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					Multigateway: &multigresv1alpha1.StatelessSpec{
						Replicas:       ptr.To(int32(3)),
						PodAnnotations: map[string]string{"foo": "bar"},
					},
				},
			},
			overrides: &multigresv1alpha1.CellOverrides{
				Multigateway: &multigresv1alpha1.StatelessSpec{},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas:       ptr.To(int32(3)),
				PodAnnotations: map[string]string{"foo": "bar"},
			},
		},
		"Map Init (Nil Base)": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					Multigateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
				},
			},
			overrides: &multigresv1alpha1.CellOverrides{
				Multigateway: &multigresv1alpha1.StatelessSpec{
					PodAnnotations: map[string]string{"a": "b"},
					PodLabels:      map[string]string{"c": "d"},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{
				Replicas:       ptr.To(int32(3)),
				PodAnnotations: map[string]string{"a": "b"},
				PodLabels:      map[string]string{"c": "d"},
			},
		},
		"Inline Priority": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					Multigateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
				},
			},
			inline: &multigresv1alpha1.CellInlineSpec{
				Multigateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(99))},
				LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "inline-etcd"},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(99))},
			wantTopo: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "inline-etcd"},
			},
		},
		"Nil Template (Override Only)": {
			tpl: nil,
			overrides: &multigresv1alpha1.CellOverrides{
				Multigateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(2))},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(2))},
		},
		"Nil Everything": {
			tpl:      nil,
			wantGw:   &multigresv1alpha1.StatelessSpec{},
			wantTopo: nil,
		},
		"Placement Merge": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					MultigatewayPlacement: &multigresv1alpha1.PodPlacementSpec{
						Tolerations: []corev1.Toleration{
							{
								Key:      "workload",
								Operator: corev1.TolerationOpEqual,
								Value:    "customer-pg",
								Effect:   corev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			inline: &multigresv1alpha1.CellInlineSpec{
				MultigatewayPlacement: &multigresv1alpha1.PodPlacementSpec{
					Tolerations: []corev1.Toleration{
						{
							Key:      "dedicated",
							Operator: corev1.TolerationOpEqual,
							Value:    "multigres",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
			wantGw: &multigresv1alpha1.StatelessSpec{},
			wantPlacement: &multigresv1alpha1.PodPlacementSpec{
				Tolerations: []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "multigres",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
		},
		"Placement Clear": {
			tpl: &multigresv1alpha1.CellTemplate{
				Spec: multigresv1alpha1.CellTemplateSpec{
					MultigatewayPlacement: &multigresv1alpha1.PodPlacementSpec{
						Tolerations: []corev1.Toleration{
							{
								Key:      "workload",
								Operator: corev1.TolerationOpEqual,
								Value:    "customer-pg",
								Effect:   corev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			inline: &multigresv1alpha1.CellInlineSpec{
				MultigatewayPlacement: &multigresv1alpha1.PodPlacementSpec{},
			},
			wantGw:        &multigresv1alpha1.StatelessSpec{},
			wantPlacement: &multigresv1alpha1.PodPlacementSpec{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gw, placement, topo := mergeCellConfig(tc.tpl, tc.overrides, tc.inline)

			if diff := cmp.Diff(
				tc.wantGw,
				gw,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Gateway mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantPlacement, placement, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Placement mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantTopo, topo); diff != "" {
				t.Errorf("Topo mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolver_ClientErrors_Cell(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	errSimulated := errors.New("simulated database connection error")
	mc := testutil.NewFakeClientWithFailures(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		&testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error { return errSimulated },
		},
	)
	r := NewResolver(mc, "default")

	_, err := r.ResolveCellTemplate(t.Context(), "any")
	if err == nil ||
		err.Error() != "failed to get CellTemplate: simulated database connection error" {
		t.Errorf("Error mismatch: got %v, want simulated error", err)
	}
}
