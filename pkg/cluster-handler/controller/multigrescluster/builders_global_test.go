package multigrescluster

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

	t.Run("Etcd Enabled with placement", func(t *testing.T) {
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:latest"},
			Placement: &multigresv1alpha1.TopoServerPlacementSpec{
				Tolerations: []corev1.Toleration{
					{
						Key:      "workload",
						Operator: corev1.TolerationOpEqual,
						Value:    "customer-pg",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
		}

		got, err := BuildGlobalTopoServer(cluster, spec, scheme)
		if err != nil {
			t.Fatalf("BuildGlobalTopoServer() error = %v", err)
		}
		if diff := cmp.Diff(spec.Placement, got.Spec.Placement); diff != "" {
			t.Errorf("Placement diff (-want +got):\n%s", diff)
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
	globalTopo := multigresv1alpha1.GlobalTopoServerRef{
		Address:  "shared-etcd.default.svc:2379",
		RootPath: "/multigres/default/my-cluster/global",
	}

	t.Run("Success", func(t *testing.T) {
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
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
		assert.Contains(t, got.Spec.Template.Spec.Containers[0].Args,
			"--topo-global-server-addresses=shared-etcd.default.svc:2379")
		assert.Contains(t, got.Spec.Template.Spec.Containers[0].Args,
			"--topo-global-root=/multigres/default/my-cluster/global")

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
		got, err := BuildMultiadminDeployment(obsCluster, spec, nil, globalTopo, scheme)
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

	t.Run("Success with internal mTLS and empty CertCommonName", func(t *testing.T) {
		tlsCluster := cluster.DeepCopy()
		tlsCluster.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
		if tlsCluster.Spec.CertCommonName != "" {
			t.Fatalf("test requires empty CertCommonName, got %q", tlsCluster.Spec.CertCommonName)
		}

		got, err := BuildMultiadminDeployment(tlsCluster, spec, nil, globalTopo, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminDeployment() error = %v", err)
		}

		var foundVol bool
		for _, v := range got.Spec.Template.Spec.Volumes {
			if v.Name == multiAdminTLSVolumeName {
				foundVol = true
				if v.Secret == nil {
					t.Fatal("TLS volume should use Secret source")
				}
				wantSecretName := "multiadmin.my-cluster.default.multigres.internal" //nolint:gosec // test constant
				if v.Secret.SecretName != wantSecretName {
					t.Errorf(
						"TLS secretName = %q, want %q",
						v.Secret.SecretName,
						wantSecretName,
					)
				}
				if v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o444 {
					t.Errorf(
						"TLS secret defaultMode = %v, want 0444",
						v.Secret.DefaultMode,
					)
				}
			}
		}
		if !foundVol {
			t.Fatalf("expected TLS volume %q", multiAdminTLSVolumeName)
		}

		container := got.Spec.Template.Spec.Containers[0]
		var foundMount bool
		for _, m := range container.VolumeMounts {
			if m.Name == multiAdminTLSVolumeName {
				foundMount = true
				if m.MountPath != multiAdminTLSMountPath {
					t.Errorf(
						"TLS mount path = %q, want %q",
						m.MountPath,
						multiAdminTLSMountPath,
					)
				}
			}
		}
		if !foundMount {
			t.Fatalf("expected TLS volume mount %q", multiAdminTLSVolumeName)
		}

		wantArgs := []string{
			"--grpc-cert", multiAdminTLSCertFile,
			"--grpc-key", multiAdminTLSKeyFile,
			"--grpc-ca", multiAdminTLSCAFile,
			"--grpc-server-ca", multiAdminTLSCAFile,
			"--multipooler-grpc-cert", multiAdminTLSCertFile,
			"--multipooler-grpc-key", multiAdminTLSKeyFile,
			"--multipooler-grpc-ca", multiAdminTLSCAFile,
			"--multipooler-grpc-server-name",
			"multipooler.my-cluster.default.multigres.internal",
			"--multipooler-grpc-require-tls",
		}
		tailArgs := container.Args[len(container.Args)-len(wantArgs):]
		if diff := cmp.Diff(wantArgs, tailArgs); diff != "" {
			t.Errorf("mTLS args mismatch (-want +got):\n%s", diff)
		}
	})

	for name, mutateCluster := range map[string]func(*multigresv1alpha1.MultigresCluster){
		"nil InternalTLS": func(*multigresv1alpha1.MultigresCluster) {},
		"disabled InternalTLS with public CertCommonName": func(
			cluster *multigresv1alpha1.MultigresCluster,
		) {
			cluster.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(false)}
			cluster.Spec.CertCommonName = "db.public.example.com"
		},
	} {
		t.Run("No internal mTLS when "+name, func(t *testing.T) {
			disabledCluster := cluster.DeepCopy()
			mutateCluster(disabledCluster)
			got, err := BuildMultiadminDeployment(disabledCluster, spec, nil, globalTopo, scheme)
			if err != nil {
				t.Fatalf("BuildMultiadminDeployment() error = %v", err)
			}

			for _, volume := range got.Spec.Template.Spec.Volumes {
				if volume.Name == multiAdminTLSVolumeName {
					t.Errorf("unexpected TLS volume %q", multiAdminTLSVolumeName)
				}
			}
			container := got.Spec.Template.Spec.Containers[0]
			for _, mount := range container.VolumeMounts {
				if mount.Name == multiAdminTLSVolumeName {
					t.Errorf("unexpected TLS volume mount %q", multiAdminTLSVolumeName)
				}
			}
			internalTLSArgs := map[string]struct{}{
				"--grpc-cert":                    {},
				"--grpc-key":                     {},
				"--grpc-ca":                      {},
				"--grpc-server-ca":               {},
				"--multipooler-grpc-cert":        {},
				"--multipooler-grpc-key":         {},
				"--multipooler-grpc-ca":          {},
				"--multipooler-grpc-server-name": {},
				"--multipooler-grpc-require-tls": {},
			}
			for _, arg := range container.Args {
				if _, found := internalTLSArgs[arg]; found {
					t.Errorf("unexpected internal TLS argument %q", arg)
				}
			}
		})
	}

	t.Run("Success with tolerations", func(t *testing.T) {
		placement := &multigresv1alpha1.PodPlacementSpec{
			Tolerations: []corev1.Toleration{
				{
					Key:      "workload",
					Operator: corev1.TolerationOpEqual,
					Value:    "customer-pg",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		}
		got, err := BuildMultiadminDeployment(cluster, spec, placement, globalTopo, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminDeployment() error = %v", err)
		}
		if diff := cmp.Diff(placement.Tolerations, got.Spec.Template.Spec.Tolerations); diff != "" {
			t.Errorf("Tolerations diff (-want +got):\n%s", diff)
		}
	})

	t.Run("Success with nil placement", func(t *testing.T) {
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
		if err != nil {
			t.Fatalf("BuildMultiadminDeployment() error = %v", err)
		}
		if len(got.Spec.Template.Spec.Tolerations) != 0 {
			t.Errorf("Tolerations = %v, want none", got.Spec.Template.Spec.Tolerations)
		}
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, emptyScheme)
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

func TestBuildAdminNetworkPolicies(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)

	baseCluster := func(np *multigresv1alpha1.NetworkPolicyConfig) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-cluster",
				Namespace: "default",
				UID:       "cluster-uid",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				NetworkPolicy: np,
			},
		}
	}

	t.Run("NilConfig", func(t *testing.T) {
		got, err := BuildAdminNetworkPolicies(baseCluster(nil), scheme)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("Disabled", func(t *testing.T) {
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: false}),
			scheme,
		)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("EnabledWithAllowedNamespaces", func(t *testing.T) {
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{
				Enabled:                  true,
				AllowedIngressNamespaces: []string{"envoy-gateway-system", "multigres-operator"},
			}),
			scheme,
		)
		require.NoError(t, err)
		require.Len(t, got, 2)

		wantNames := []string{
			"my-cluster-multiadmin-restrict-ingress",
			"my-cluster-multiadmin-web-restrict-ingress",
		}
		wantComponents := []string{"multiadmin", "multiadmin-web"}

		for i, policy := range got {
			assert.Equal(t, wantNames[i], policy.Name)
			assert.Equal(t, "default", policy.Namespace)
			require.Len(t, policy.OwnerReferences, 1)

			assert.Equal(t,
				map[string]string{
					"app.kubernetes.io/component": wantComponents[i],
					"app.kubernetes.io/instance":  "my-cluster",
					"multigres.com/cluster":       "my-cluster",
				},
				policy.Spec.PodSelector.MatchLabels,
			)
			assert.Equal(t,
				[]networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
				policy.Spec.PolicyTypes,
			)

			require.Len(t, policy.Spec.Ingress, 1)
			peers := policy.Spec.Ingress[0].From
			require.Len(t, peers, 3)
			// An empty pod selector without a namespace selector matches local pods.
			assert.NotNil(t, peers[0].PodSelector)
			assert.Nil(t, peers[0].NamespaceSelector)
			for j, ns := range []string{"envoy-gateway-system", "multigres-operator"} {
				peer := peers[j+1]
				require.NotNil(t, peer.NamespaceSelector)
				assert.Equal(t,
					map[string]string{"kubernetes.io/metadata.name": ns},
					peer.NamespaceSelector.MatchLabels,
				)
				assert.Nil(t, peer.PodSelector)
			}
		}
	})

	t.Run("EnabledWithoutAllowedNamespaces", func(t *testing.T) {
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: true}),
			scheme,
		)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Len(t, got[0].Spec.Ingress, 1)
		assert.Len(t, got[0].Spec.Ingress[0].From, 1)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: true}),
			emptyScheme,
		)
		assert.Error(t, err)
	})
}
