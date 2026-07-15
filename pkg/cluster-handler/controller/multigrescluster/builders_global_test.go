package multigrescluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildGlobalTopoServer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	t.Run("Etcd Enabled", func(t *testing.T) {
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Image:    "etcd:latest",
				Replicas: ptr.To(int32(3)),
			},
		}

		got, err := BuildGlobalTopoServer(cluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildGlobalTopoServer() error = %v", err)
		}

		if got == nil {
			t.Fatal("Expected TopoServer, got nil")
		}
		if got.Name != "my-cluster-global-topo" {
			t.Errorf("Name = %v, want %v", got.Name, "my-cluster-global-topo")
		}
		if got.Spec.Etcd.Image != "etcd:latest" {
			t.Errorf("Image = %v, want %v", got.Spec.Etcd.Image, "etcd:latest")
		}
		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		} else if got.OwnerReferences[0].Name != "my-cluster" {
			t.Errorf("OwnerReference Name = %v, want %v", got.OwnerReferences[0].Name, "my-cluster")
		}
	})

	t.Run("Etcd Disabled (External)", func(t *testing.T) {
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: nil, // Simulating external mode where Etcd spec is nil
		}

		got, err := BuildGlobalTopoServer(cluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildGlobalTopoServer() error = %v", err)
		}
		if got != nil {
			t.Errorf("Expected nil when Etcd spec is nil, got %v", got)
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Image: "img"},
		}
		_, err := BuildGlobalTopoServer(cluster, spec, emptyScheme)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})
}

func TestBuildMultiadminDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Images: multigresv1alpha1.ClusterImages{
				Multiadmin: "multiadmin:latest",
			},
		},
	}

	spec := &multigresv1alpha1.StatelessSpec{
		Replicas:       ptr.To(int32(2)),
		PodLabels:      map[string]string{"custom": "label"},
		PodAnnotations: map[string]string{"anno": "tation"},
	}

	t.Run("Success", func(t *testing.T) {
		got, err := BuildMultiadminDeployment(cluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminDeployment() error = %v", err)
		}

		if got.Name != "my-cluster-multiadmin" {
			t.Errorf("Name = %v, want %v", got.Name, "my-cluster-multiadmin")
		}
		if *got.Spec.Replicas != 2 {
			t.Errorf("Replicas = %v, want 2", *got.Spec.Replicas)
		}
		if got.Spec.Template.Labels["custom"] != "label" {
			t.Errorf("PodLabels missing custom label")
		}
		if got.Spec.Template.Annotations["anno"] != "tation" {
			t.Errorf("PodAnnotations missing annotation")
		}

		// Verify container image from cluster spec
		if len(got.Spec.Template.Spec.Containers) > 0 {
			if got.Spec.Template.Spec.Containers[0].Image != "multiadmin:latest" {
				t.Errorf(
					"Container Image = %v, want multiadmin:latest",
					got.Spec.Template.Spec.Containers[0].Image,
				)
			}
		}

		// Verify Selector does NOT contain mutable labels
		selector := got.Spec.Selector.MatchLabels
		if _, ok := selector["app.kubernetes.io/name"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/name")
		}
		if _, ok := selector["app.kubernetes.io/managed-by"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/managed-by")
		}
		if _, ok := selector["app.kubernetes.io/component"]; !ok {
			t.Error("Selector MUST contain app.kubernetes.io/component")
		}

		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		}
	})

	t.Run("Success with Observability", func(t *testing.T) {
		obsCluster := cluster.DeepCopy()
		obsCluster.Spec.Observability = &multigresv1alpha1.ObservabilityConfig{
			TracesSampler: "multigres_custom",
			SamplingConfigRef: &multigresv1alpha1.SamplingConfigRef{
				Name: "sample-config",
				Key:  "sampling-config.yaml",
			},
		}
		got, err := BuildMultiadminDeployment(obsCluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminDeployment() error = %v", err)
		}
		if len(got.Spec.Template.Spec.Volumes) == 0 {
			t.Errorf("Expected OTEL volume to be added")
		}
		if len(got.Spec.Template.Spec.Containers[0].VolumeMounts) == 0 {
			t.Errorf("Expected OTEL volume mount to be added")
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminDeployment(cluster, spec, emptyScheme)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})
}

func TestBuildMultiadminWebDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Images: multigresv1alpha1.ClusterImages{
				MultiadminWeb: "multiadmin-web:latest",
			},
		},
	}

	spec := &multigresv1alpha1.StatelessSpec{
		Replicas:       ptr.To(int32(2)),
		PodLabels:      map[string]string{"custom": "label"},
		PodAnnotations: map[string]string{"anno": "tation"},
	}

	t.Run("Success", func(t *testing.T) {
		got, err := BuildMultiadminWebDeployment(cluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminWebDeployment() error = %v", err)
		}

		if got.Name != "my-cluster-multiadmin-web" {
			t.Errorf("Name = %v, want %v", got.Name, "my-cluster-multiadmin-web")
		}
		if *got.Spec.Replicas != 2 {
			t.Errorf("Replicas = %v, want 2", *got.Spec.Replicas)
		}
		if got.Spec.Template.Labels["custom"] != "label" {
			t.Errorf("PodLabels missing custom label")
		}
		if got.Spec.Template.Annotations["anno"] != "tation" {
			t.Errorf("PodAnnotations missing annotation")
		}

		// Verify container image from cluster spec
		if len(got.Spec.Template.Spec.Containers) > 0 {
			if got.Spec.Template.Spec.Containers[0].Image != "multiadmin-web:latest" {
				t.Errorf(
					"Container Image = %v, want multiadmin-web:latest",
					got.Spec.Template.Spec.Containers[0].Image,
				)
			}
		}

		// Verify env vars
		envVars := got.Spec.Template.Spec.Containers[0].Env
		wantEnv := map[string]string{
			"MULTIADMIN_API_URL": fmt.Sprintf("http://%s-multiadmin:18000", cluster.Name),
			"POSTGRES_HOST":      fmt.Sprintf("%s-multigateway", cluster.Name),
			"POSTGRES_PORT":      "5432",
			"POSTGRES_DATABASE":  "postgres",
			"POSTGRES_USER":      "postgres",
		}
		for wantName, wantValue := range wantEnv {
			found := false
			for _, ev := range envVars {
				if ev.Name == wantName {
					found = true
					if ev.Value != wantValue {
						t.Errorf("Env %s = %q, want %q", wantName, ev.Value, wantValue)
					}
					break
				}
			}
			if !found {
				t.Errorf("Missing env var %s", wantName)
			}
		}

		// Verify Selector does NOT contain mutable labels
		selector := got.Spec.Selector.MatchLabels
		if _, ok := selector["app.kubernetes.io/name"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/name")
		}
		if _, ok := selector["app.kubernetes.io/managed-by"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/managed-by")
		}
		if _, ok := selector["app.kubernetes.io/component"]; !ok {
			t.Error("Selector MUST contain app.kubernetes.io/component")
		}

		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		}
	})

	t.Run("CustomPostgresSuperuser", func(t *testing.T) {
		c := *cluster
		c.Spec.PostgresSuperuser = "admin"
		got, err := BuildMultiadminWebDeployment(&c, spec, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminWebDeployment() error = %v", err)
		}
		found := false
		for _, ev := range got.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == "POSTGRES_USER" {
				found = true
				if ev.Value != "admin" {
					t.Errorf("POSTGRES_USER = %q, want %q", ev.Value, "admin")
				}
				break
			}
		}
		if !found {
			t.Fatal("Missing env var POSTGRES_USER")
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminWebDeployment(cluster, spec, emptyScheme)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})
}

func TestBuildMultiadminWebService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/instance":   "my-cluster",
		"app.kubernetes.io/component":  "multiadmin-web",
		"app.kubernetes.io/part-of":    "multigres",
		"app.kubernetes.io/managed-by": "multigres-operator",
		"multigres.com/cluster":        "my-cluster",
	}

	wantPort := corev1.ServicePort{
		Name:       "http",
		Port:       18100,
		TargetPort: intstr.FromInt(18100),
		Protocol:   corev1.ProtocolTCP,
	}

	tests := []struct {
		name            string
		extAW           *multigresv1alpha1.ExternalAdminWebConfig
		wantType        corev1.ServiceType
		wantAnnotations map[string]string
		wantExternalIPs []string
	}{
		{
			name:     "nil config → ClusterIP, no annotations",
			extAW:    nil,
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name:     "Enabled: false → ClusterIP, no annotations",
			extAW:    &multigresv1alpha1.ExternalAdminWebConfig{Enabled: false},
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name:     "Enabled: true, no annotations → ClusterIP",
			extAW:    &multigresv1alpha1.ExternalAdminWebConfig{Enabled: true},
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name: "Enabled: true, with annotations → annotations applied",
			extAW: &multigresv1alpha1.ExternalAdminWebConfig{
				Enabled: true,
				Annotations: map[string]string{
					"team.example.com/owner": "platform-engineering",
				},
			},
			wantType: corev1.ServiceTypeClusterIP,
			wantAnnotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
		},
		{
			name: "Enabled: true, with external IPs",
			extAW: &multigresv1alpha1.ExternalAdminWebConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"10.0.0.1"},
			},
			wantType:        corev1.ServiceTypeClusterIP,
			wantExternalIPs: []string{"10.0.0.1"},
		},
		{
			name: "Enabled: true, with IPs and annotations",
			extAW: &multigresv1alpha1.ExternalAdminWebConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::10"},
				Annotations: map[string]string{"custom/key": "val"},
			},
			wantType:        corev1.ServiceTypeClusterIP,
			wantExternalIPs: []string{"2001:db8::10"},
			wantAnnotations: map[string]string{"custom/key": "val"},
		},
		{
			name:     "Disabled after previously enabled → no annotations",
			extAW:    &multigresv1alpha1.ExternalAdminWebConfig{Enabled: false},
			wantType: corev1.ServiceTypeClusterIP,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildMultiadminWebService(cluster, tc.extAW, scheme)
			require.NoError(t, err)

			assert.Equal(t, "my-cluster-multiadmin-web", got.Name)
			assert.Equal(t, "default", got.Namespace)
			assert.Equal(t, tc.wantType, got.Spec.Type)
			assert.Equal(t, tc.wantExternalIPs, got.Spec.ExternalIPs)

			require.Len(t, got.Spec.Ports, 1)
			assert.Equal(t, wantPort, got.Spec.Ports[0])

			assert.Equal(t, wantLabels, got.Labels)

			if tc.wantAnnotations != nil {
				for k, v := range tc.wantAnnotations {
					assert.Equal(t, v, got.Annotations[k], "annotation %s", k)
				}
			} else {
				assert.Empty(t, got.Annotations)
			}

			require.Len(t, got.OwnerReferences, 1)
			assert.Equal(t, "my-cluster", got.OwnerReferences[0].Name)
		})
	}

	t.Run("Annotation removal on disable", func(t *testing.T) {
		enabledCfg := &multigresv1alpha1.ExternalAdminWebConfig{
			Enabled: true,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
		}
		enabled, err := BuildMultiadminWebService(cluster, enabledCfg, scheme)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, enabled.Spec.Type)
		assert.Equal(
			t,
			"platform-engineering",
			enabled.Annotations["team.example.com/owner"],
		)

		disabledCfg := &multigresv1alpha1.ExternalAdminWebConfig{Enabled: false}
		disabled, err := BuildMultiadminWebService(cluster, disabledCfg, scheme)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, disabled.Spec.Type)
		assert.Empty(t, disabled.Annotations)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminWebService(cluster, nil, emptyScheme)
		assert.Error(t, err)
	})
}

func TestBuildMultiadminService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	t.Run("Success", func(t *testing.T) {
		got, err := BuildMultiadminService(cluster, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminService() error = %v", err)
		}

		if got.Name != "my-cluster-multiadmin" {
			t.Errorf("Name = %v, want %v", got.Name, "my-cluster-multiadmin")
		}

		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminService(cluster, emptyScheme)
		if err == nil {
			t.Error("Expected error due to missing scheme types, got nil")
		}
	})
}

func TestBuildMultigatewayGlobalService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	// Expected labels on every produced Service (standard + cluster label).
	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/instance":   "my-cluster",
		"app.kubernetes.io/component":  "multigateway",
		"app.kubernetes.io/part-of":    "multigres",
		"app.kubernetes.io/managed-by": "multigres-operator",
		"multigres.com/cluster":        "my-cluster",
	}

	wantPort := corev1.ServicePort{
		Name:       "postgres",
		Port:       5432,
		TargetPort: intstr.FromString("postgres"),
		Protocol:   corev1.ProtocolTCP,
	}

	tests := []struct {
		name            string
		extGw           *multigresv1alpha1.ExternalGatewayConfig
		wantType        corev1.ServiceType
		wantAnnotations map[string]string // nil means no gateway annotations expected
		wantExternalIPs []string
	}{
		{
			name:     "nil config → ClusterIP, no gateway annotations",
			extGw:    nil,
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name:     "Enabled: false → ClusterIP, no gateway annotations",
			extGw:    &multigresv1alpha1.ExternalGatewayConfig{Enabled: false},
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name:     "Enabled: true, no annotations → ClusterIP",
			extGw:    &multigresv1alpha1.ExternalGatewayConfig{Enabled: true},
			wantType: corev1.ServiceTypeClusterIP,
		},
		{
			name: "Enabled: true, with annotations → ClusterIP, annotations applied",
			extGw: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled: true,
				Annotations: map[string]string{
					"team.example.com/owner":        "platform-engineering",
					"monitoring.example.com/scrape": "true",
				},
			},
			wantType: corev1.ServiceTypeClusterIP,
			wantAnnotations: map[string]string{
				"team.example.com/owner":        "platform-engineering",
				"monitoring.example.com/scrape": "true",
			},
		},
		{
			name: "Enabled: true, annotations with label-prefix keys → labels unchanged",
			extGw: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled: true,
				Annotations: map[string]string{
					"app.kubernetes.io/custom-annotation": "should-not-overwrite-labels",
					"multigres.com/some-annotation":       "also-should-not-overwrite",
				},
			},
			wantType: corev1.ServiceTypeClusterIP,
			wantAnnotations: map[string]string{
				"app.kubernetes.io/custom-annotation": "should-not-overwrite-labels",
				"multigres.com/some-annotation":       "also-should-not-overwrite",
			},
		},
		{
			name: "Enabled: true, with external IPs",
			extGw: &multigresv1alpha1.ExternalGatewayConfig{
				Enabled:     true,
				ExternalIPs: []multigresv1alpha1.IPAddress{"2001:db8::10"},
			},
			wantType:        corev1.ServiceTypeClusterIP,
			wantExternalIPs: []string{"2001:db8::10"},
		},
		{
			name:     "Disabled after previously enabled → ClusterIP, no gateway annotations",
			extGw:    &multigresv1alpha1.ExternalGatewayConfig{Enabled: false},
			wantType: corev1.ServiceTypeClusterIP,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildMultigatewayGlobalService(cluster, tc.extGw, scheme)
			require.NoError(t, err)

			// Name and namespace
			assert.Equal(t, "my-cluster-multigateway", got.Name)
			assert.Equal(t, "default", got.Namespace)

			// Service type
			assert.Equal(t, tc.wantType, got.Spec.Type)
			assert.Equal(t, tc.wantExternalIPs, got.Spec.ExternalIPs)

			// Port 5432 invariant
			require.Len(t, got.Spec.Ports, 1)
			assert.Equal(t, wantPort, got.Spec.Ports[0])

			// Labels preserved
			assert.Equal(t, wantLabels, got.Labels)

			// Annotations
			if tc.wantAnnotations != nil {
				for k, v := range tc.wantAnnotations {
					assert.Equal(t, v, got.Annotations[k], "annotation %s", k)
				}
			} else {
				// No gateway annotations expected; annotations should be nil or empty
				assert.Empty(t, got.Annotations)
			}

			// Selector: component + instance, no cell label
			assert.Equal(t, "multigateway", got.Spec.Selector["app.kubernetes.io/component"])
			assert.Equal(t, "my-cluster", got.Spec.Selector["app.kubernetes.io/instance"])
			assert.NotContains(t, got.Spec.Selector, "multigres.com/cell")

			// Owner reference
			require.Len(t, got.OwnerReferences, 1)
			assert.Equal(t, "my-cluster", got.OwnerReferences[0].Name)
		})
	}

	t.Run("Annotation removal on disable", func(t *testing.T) {
		// Build with annotations enabled
		enabledCfg := &multigresv1alpha1.ExternalGatewayConfig{
			Enabled: true,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
		}
		enabled, err := BuildMultigatewayGlobalService(cluster, enabledCfg, scheme)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, enabled.Spec.Type)
		assert.Equal(
			t,
			"platform-engineering",
			enabled.Annotations["team.example.com/owner"],
		)

		// Build with disabled config; previously-set gateway annotations absent
		disabledCfg := &multigresv1alpha1.ExternalGatewayConfig{Enabled: false}
		disabled, err := BuildMultigatewayGlobalService(cluster, disabledCfg, scheme)
		require.NoError(t, err)
		assert.Equal(t, corev1.ServiceTypeClusterIP, disabled.Spec.Type)
		assert.Empty(t, disabled.Annotations)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultigatewayGlobalService(cluster, nil, emptyScheme)
		assert.Error(t, err)
	})
}

func TestBuildMultigatewayGlobalReplicaService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
	}

	got, err := BuildMultigatewayGlobalReplicaService(cluster, scheme)
	require.NoError(t, err)

	assert.Equal(t, "my-cluster-multigateway-replica", got.Name)
	assert.Equal(t, "default", got.Namespace)
	assert.Equal(t, corev1.ServiceTypeClusterIP, got.Spec.Type)

	require.Len(t, got.Spec.Ports, 1)
	assert.Equal(t, corev1.ServicePort{
		Name:       "pg-replica",
		Port:       5433,
		TargetPort: intstr.FromString("pg-replica"),
		Protocol:   corev1.ProtocolTCP,
	}, got.Spec.Ports[0])

	assert.Equal(t, map[string]string{
		"app.kubernetes.io/component": "multigateway",
		"app.kubernetes.io/instance":  "my-cluster",
	}, got.Spec.Selector)

	require.Len(t, got.OwnerReferences, 1)
	assert.Equal(t, "my-cluster", got.OwnerReferences[0].Name)

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultigatewayGlobalReplicaService(cluster, emptyScheme)
		assert.Error(t, err)
	})
}
