package multigrescluster

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
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
		c := assert.NewCollecting(t)
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Image:    "etcd:latest",
				Replicas: ptr.To(int32(3)),
			},
		}

		got, err := BuildGlobalTopoServer(cluster, spec, scheme)
		c.Require().NoError(err, "BuildGlobalTopoServer() error =")

		c.Require().NotNil(got, "Expected TopoServer, got nil")
		c.Eq("my-cluster-global-topo", got.Name, "Name")
		c.Eq("etcd:latest", got.Spec.Etcd.Image, "Image")
		// Verify OwnerReference
		if len(got.OwnerReferences) != 1 {
			t.Errorf("OwnerReferences count = %v, want 1", len(got.OwnerReferences))
		} else if got.OwnerReferences[0].Name != "my-cluster" {
			t.Errorf("OwnerReference Name = %v, want %v", got.OwnerReferences[0].Name, "my-cluster")
		}
	})

	t.Run("Etcd Enabled with placement", func(t *testing.T) {
		c := assert.NewCollecting(t)
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
		c.Require().NoError(err, "BuildGlobalTopoServer() error =")
		c.EqDiff(spec.Placement, got.Spec.Placement, "Placement diff")
	})

	t.Run("Etcd Disabled (External)", func(t *testing.T) {
		c := assert.NewCollecting(t)
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: nil, // Simulating external mode where Etcd spec is nil
		}

		got, err := BuildGlobalTopoServer(cluster, spec, scheme)
		c.Require().NoError(err, "BuildGlobalTopoServer() error =")
		c.Nil(got, "Expected nil when Etcd spec is nil, got")
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		spec := &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Image: "img"},
		}
		_, err := BuildGlobalTopoServer(cluster, spec, emptyScheme)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
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
		c := assert.NewCollecting(t)
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
		c.Require().NoError(err, "BuildMultiadminDeployment() error =")

		c.Eq("my-cluster-multiadmin", got.Name, "Name")
		c.Eq(2, *got.Spec.Replicas, "Replicas")
		c.Eq("label", got.Spec.Template.Labels["custom"], "PodLabels missing custom label")
		c.Eq("tation", got.Spec.Template.Annotations["anno"], "PodAnnotations missing annotation")
		c.Contains(
			got.Spec.Template.Spec.Containers[0].Args,
			"--topo-global-server-addresses=shared-etcd.default.svc:2379",
		)
		c.Contains(
			got.Spec.Template.Spec.Containers[0].Args,
			"--topo-global-root=/multigres/default/my-cluster/global",
		)

		// Verify container image from cluster spec
		if len(got.Spec.Template.Spec.Containers) > 0 {
			c.Eq("multiadmin:latest", got.Spec.Template.Spec.Containers[0].Image, "Container Image")
		}

		// Verify Selector does NOT contain mutable labels
		selector := got.Spec.Selector.MatchLabels
		if _, ok := selector["app.kubernetes.io/name"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/name")
		}
		if _, ok := selector["app.kubernetes.io/managed-by"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/managed-by")
		}
		_, ok := selector["app.kubernetes.io/component"]
		c.True(ok, "Selector MUST contain app.kubernetes.io/component")

		// Verify OwnerReference
		c.Len(
			got.OwnerReferences,
			1,
			"OwnerReferences count = %v, want 1",
			len(got.OwnerReferences),
		)
	})

	t.Run("Success with Observability", func(t *testing.T) {
		c := assert.NewCollecting(t)
		obsCluster := cluster.DeepCopy()
		obsCluster.Spec.Observability = &multigresv1alpha1.ObservabilityConfig{
			TracesSampler: "multigres_custom",
			SamplingConfigRef: &multigresv1alpha1.SamplingConfigRef{
				Name: "sample-config",
				Key:  "sampling-config.yaml",
			},
		}
		got, err := BuildMultiadminDeployment(obsCluster, spec, nil, globalTopo, scheme)
		c.Require().NoError(err, "BuildMultiadminDeployment() error =")
		c.NotEmpty(got.Spec.Template.Spec.Volumes, "Expected OTEL volume to be added")
		c.NotEmpty(
			got.Spec.Template.Spec.Containers[0].VolumeMounts,
			"Expected OTEL volume mount to be added",
		)
	})

	t.Run("Success with internal mTLS and empty CertCommonName", func(t *testing.T) {
		c := assert.NewCollecting(t)
		tlsCluster := cluster.DeepCopy()
		tlsCluster.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
		c.Require().
			Eq("", tlsCluster.Spec.CertCommonName, "test requires empty CertCommonName, got")

		got, err := BuildMultiadminDeployment(tlsCluster, spec, nil, globalTopo, scheme)
		c.Require().NoError(err, "BuildMultiadminDeployment() error =")

		var foundVol bool
		for _, v := range got.Spec.Template.Spec.Volumes {
			if v.Name == multiAdminTLSVolumeName {
				foundVol = true
				c.Require().NotNil(v.Secret, "TLS volume should use Secret source")
				wantSecretName := "multiadmin.my-cluster.default.multigres.internal" //nolint:gosec // test constant
				c.Eq(wantSecretName, v.Secret.SecretName, "TLS secretName")
				if v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o444 {
					t.Errorf(
						"TLS secret defaultMode = %v, want 0444",
						v.Secret.DefaultMode,
					)
				}
			}
		}
		c.Require().True(foundVol, "expected TLS volume %q", multiAdminTLSVolumeName)

		container := got.Spec.Template.Spec.Containers[0]
		var foundMount bool
		for _, m := range container.VolumeMounts {
			if m.Name == multiAdminTLSVolumeName {
				foundMount = true
				c.Eq(multiAdminTLSMountPath, m.MountPath, "TLS mount path")
			}
		}
		c.Require().True(foundMount, "expected TLS volume mount %q", multiAdminTLSVolumeName)

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
		c.EqDiff(wantArgs, tailArgs, "mTLS args mismatch")
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
			c := assert.NewCollecting(t)
			disabledCluster := cluster.DeepCopy()
			mutateCluster(disabledCluster)
			got, err := BuildMultiadminDeployment(disabledCluster, spec, nil, globalTopo, scheme)
			c.Require().NoError(err, "BuildMultiadminDeployment() error =")

			for _, volume := range got.Spec.Template.Spec.Volumes {
				c.NotEq(multiAdminTLSVolumeName, volume.Name, "unexpected TLS volume")
			}
			container := got.Spec.Template.Spec.Containers[0]
			for _, mount := range container.VolumeMounts {
				c.NotEq(multiAdminTLSVolumeName, mount.Name, "unexpected TLS volume mount")
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
				_, found := internalTLSArgs[arg]
				c.False(found, "unexpected internal TLS argument %q", arg)
			}
		})
	}

	t.Run("Success with tolerations", func(t *testing.T) {
		c := assert.NewCollecting(t)
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
		c.Require().NoError(err, "BuildMultiadminDeployment() error =")
		c.EqDiff(placement.Tolerations, got.Spec.Template.Spec.Tolerations, "Tolerations diff")
	})

	t.Run("Success with nil placement", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
		c.Require().NoError(err, "BuildMultiadminDeployment() error =")
		c.Empty(got.Spec.Template.Spec.Tolerations, "Tolerations")
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, emptyScheme)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
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
		c := assert.NewCollecting(t)
		got, err := BuildMultiadminWebDeployment(cluster, spec, scheme)
		c.Require().NoError(err, "BuildMultiadminWebDeployment() error =")

		c.Eq("my-cluster-multiadmin-web", got.Name, "Name")
		c.Eq(2, *got.Spec.Replicas, "Replicas")
		c.Eq("label", got.Spec.Template.Labels["custom"], "PodLabels missing custom label")
		c.Eq("tation", got.Spec.Template.Annotations["anno"], "PodAnnotations missing annotation")

		// Verify container image from cluster spec
		if len(got.Spec.Template.Spec.Containers) > 0 {
			c.Eq(
				"multiadmin-web:latest",
				got.Spec.Template.Spec.Containers[0].Image,
				"Container Image",
			)
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
					c.Eq(wantValue, ev.Value, "Env %s = %q, want", wantName, ev.Value)
					break
				}
			}
			c.True(found, "Missing env var %s", wantName)
		}

		// Verify Selector does NOT contain mutable labels
		selector := got.Spec.Selector.MatchLabels
		if _, ok := selector["app.kubernetes.io/name"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/name")
		}
		if _, ok := selector["app.kubernetes.io/managed-by"]; ok {
			t.Error("Selector should not contain app.kubernetes.io/managed-by")
		}
		_, ok := selector["app.kubernetes.io/component"]
		c.True(ok, "Selector MUST contain app.kubernetes.io/component")

		// Verify OwnerReference
		c.Len(
			got.OwnerReferences,
			1,
			"OwnerReferences count = %v, want 1",
			len(got.OwnerReferences),
		)
	})

	t.Run("CustomPostgresSuperuser", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := *cluster
		c.Spec.PostgresSuperuser = "admin"
		got, err := BuildMultiadminWebDeployment(&c, spec, scheme)
		ck.Require().NoError(err, "BuildMultiadminWebDeployment() error =")
		found := false
		for _, ev := range got.Spec.Template.Spec.Containers[0].Env {
			if ev.Name == "POSTGRES_USER" {
				found = true
				ck.Eq("admin", ev.Value, "POSTGRES_USER")
				break
			}
		}
		ck.Require().True(found, "Missing env var POSTGRES_USER")
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminWebDeployment(cluster, spec, emptyScheme)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
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
			c := assert.NewCollecting(t)
			got, err := BuildMultiadminWebService(cluster, tc.extAW, scheme)
			c.Require().NoError(err)

			c.EqDeep("my-cluster-multiadmin-web", got.Name)
			c.EqDeep("default", got.Namespace)
			c.EqDeep(tc.wantType, got.Spec.Type)
			c.EqDeep(tc.wantExternalIPs, got.Spec.ExternalIPs)

			c.Require().Len(got.Spec.Ports, 1)
			c.EqDeep(wantPort, got.Spec.Ports[0])

			c.EqDeep(wantLabels, got.Labels)

			if tc.wantAnnotations != nil {
				for k, v := range tc.wantAnnotations {
					c.EqDeep(v, got.Annotations[k], "annotation %s", k)
				}
			} else {
				c.Empty(got.Annotations)
			}

			c.Require().Len(got.OwnerReferences, 1)
			c.EqDeep("my-cluster", got.OwnerReferences[0].Name)
		})
	}

	t.Run("Annotation removal on disable", func(t *testing.T) {
		c := assert.NewCollecting(t)
		enabledCfg := &multigresv1alpha1.ExternalAdminWebConfig{
			Enabled: true,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
		}
		enabled, err := BuildMultiadminWebService(cluster, enabledCfg, scheme)
		c.Require().NoError(err)
		c.EqDeep(corev1.ServiceTypeClusterIP, enabled.Spec.Type)
		c.EqDeep("platform-engineering", enabled.Annotations["team.example.com/owner"])

		disabledCfg := &multigresv1alpha1.ExternalAdminWebConfig{Enabled: false}
		disabled, err := BuildMultiadminWebService(cluster, disabledCfg, scheme)
		c.Require().NoError(err)
		c.EqDeep(corev1.ServiceTypeClusterIP, disabled.Spec.Type)
		c.Empty(disabled.Annotations)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminWebService(cluster, nil, emptyScheme)
		assert.NewCollecting(t).Error(err)
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
		c := assert.NewCollecting(t)
		got, err := BuildMultiadminService(cluster, scheme)
		c.Require().NoError(err, "BuildMultiadminService() error =")

		c.Eq("my-cluster-multiadmin", got.Name, "Name")

		// Verify OwnerReference
		c.Len(
			got.OwnerReferences,
			1,
			"OwnerReferences count = %v, want 1",
			len(got.OwnerReferences),
		)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultiadminService(cluster, emptyScheme)
		assert.NewCollecting(t).Error(err, "Expected error due to missing scheme types, got nil")
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
			c := assert.NewCollecting(t)
			got, err := BuildMultigatewayGlobalService(cluster, tc.extGw, scheme)
			c.Require().NoError(err)

			// Name and namespace
			c.EqDeep("my-cluster-multigateway", got.Name)
			c.EqDeep("default", got.Namespace)

			// Service type
			c.EqDeep(tc.wantType, got.Spec.Type)
			c.EqDeep(tc.wantExternalIPs, got.Spec.ExternalIPs)

			// Port 5432 invariant
			c.Require().Len(got.Spec.Ports, 1)
			c.EqDeep(wantPort, got.Spec.Ports[0])

			// Labels preserved
			c.EqDeep(wantLabels, got.Labels)

			// Annotations
			if tc.wantAnnotations != nil {
				for k, v := range tc.wantAnnotations {
					c.EqDeep(v, got.Annotations[k], "annotation %s", k)
				}
			} else {
				// No gateway annotations expected; annotations should be nil or empty
				c.Empty(got.Annotations)
			}

			// Selector: component + instance, no cell label
			c.EqDeep("multigateway", got.Spec.Selector["app.kubernetes.io/component"])
			c.EqDeep("my-cluster", got.Spec.Selector["app.kubernetes.io/instance"])
			c.NotHasKey(got.Spec.Selector, "multigres.com/cell")

			// Owner reference
			c.Require().Len(got.OwnerReferences, 1)
			c.EqDeep("my-cluster", got.OwnerReferences[0].Name)
		})
	}

	t.Run("Annotation removal on disable", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// Build with annotations enabled
		enabledCfg := &multigresv1alpha1.ExternalGatewayConfig{
			Enabled: true,
			Annotations: map[string]string{
				"team.example.com/owner": "platform-engineering",
			},
		}
		enabled, err := BuildMultigatewayGlobalService(cluster, enabledCfg, scheme)
		c.Require().NoError(err)
		c.EqDeep(corev1.ServiceTypeClusterIP, enabled.Spec.Type)
		c.EqDeep("platform-engineering", enabled.Annotations["team.example.com/owner"])

		// Build with disabled config; previously-set gateway annotations absent
		disabledCfg := &multigresv1alpha1.ExternalGatewayConfig{Enabled: false}
		disabled, err := BuildMultigatewayGlobalService(cluster, disabledCfg, scheme)
		c.Require().NoError(err)
		c.EqDeep(corev1.ServiceTypeClusterIP, disabled.Spec.Type)
		c.Empty(disabled.Annotations)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultigatewayGlobalService(cluster, nil, emptyScheme)
		assert.NewCollecting(t).Error(err)
	})
}

func TestBuildMultigatewayGlobalReplicaService(t *testing.T) {
	c := assert.NewCollecting(t)
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
	c.Require().NoError(err)

	c.EqDeep("my-cluster-multigateway-replica", got.Name)
	c.EqDeep("default", got.Namespace)
	c.EqDeep(corev1.ServiceTypeClusterIP, got.Spec.Type)

	c.Require().Len(got.Spec.Ports, 1)
	c.EqDeep(corev1.ServicePort{
		Name:       "pg-replica",
		Port:       5433,
		TargetPort: intstr.FromString("pg-replica"),
		Protocol:   corev1.ProtocolTCP,
	}, got.Spec.Ports[0])

	c.EqDeep(map[string]string{
		"app.kubernetes.io/component": "multigateway",
		"app.kubernetes.io/instance":  "my-cluster",
	}, got.Spec.Selector)

	c.Require().Len(got.OwnerReferences, 1)
	c.EqDeep("my-cluster", got.OwnerReferences[0].Name)

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildMultigatewayGlobalReplicaService(cluster, emptyScheme)
		assert.NewCollecting(t).Error(err)
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
		c := assert.NewCollecting(t)
		got, err := BuildAdminNetworkPolicies(baseCluster(nil), scheme)
		c.Require().NoError(err)
		c.Nil(got)
	})

	t.Run("Disabled", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: false}),
			scheme,
		)
		c.Require().NoError(err)
		c.Nil(got)
	})

	t.Run("EnabledWithAllowedNamespaces", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{
				Enabled:                  true,
				AllowedIngressNamespaces: []string{"envoy-gateway-system", "multigres-operator"},
			}),
			scheme,
		)
		c.Require().NoError(err)
		c.Require().Len(got, 2)

		wantNames := []string{
			"my-cluster-multiadmin-restrict-ingress",
			"my-cluster-multiadmin-web-restrict-ingress",
		}
		wantComponents := []string{"multiadmin", "multiadmin-web"}

		for i, policy := range got {
			c.EqDeep(wantNames[i], policy.Name)
			c.EqDeep("default", policy.Namespace)
			c.Require().Len(policy.OwnerReferences, 1)

			c.EqDeep(map[string]string{
				"app.kubernetes.io/component": wantComponents[i],
				"app.kubernetes.io/instance":  "my-cluster",
				"multigres.com/cluster":       "my-cluster",
			}, policy.Spec.PodSelector.MatchLabels)
			c.EqDeep(
				[]networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
				policy.Spec.PolicyTypes,
			)

			c.Require().Len(policy.Spec.Ingress, 1)
			peers := policy.Spec.Ingress[0].From
			c.Require().Len(peers, 3)
			// An empty pod selector without a namespace selector matches local pods.
			c.NotNil(peers[0].PodSelector)
			c.Nil(peers[0].NamespaceSelector)
			for j, ns := range []string{"envoy-gateway-system", "multigres-operator"} {
				peer := peers[j+1]
				c.Require().NotNil(peer.NamespaceSelector)
				c.EqDeep(
					map[string]string{"kubernetes.io/metadata.name": ns},
					peer.NamespaceSelector.MatchLabels,
				)
				c.Nil(peer.PodSelector)
			}
		}
	})

	t.Run("EnabledWithoutAllowedNamespaces", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: true}),
			scheme,
		)
		c.Require().NoError(err)
		c.Require().Len(got, 2)
		c.Require().Len(got[0].Spec.Ingress, 1)
		c.Len(got[0].Spec.Ingress[0].From, 1)
	})

	t.Run("ControllerRefError", func(t *testing.T) {
		emptyScheme := runtime.NewScheme()
		_, err := BuildAdminNetworkPolicies(
			baseCluster(&multigresv1alpha1.NetworkPolicyConfig{Enabled: true}),
			emptyScheme,
		)
		assert.NewCollecting(t).Error(err)
	})
}

func TestBuildMultiadminDeployment_TopoClientTLS(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Images: multigresv1alpha1.ClusterImages{Multiadmin: "multiadmin:latest"},
		},
	}
	spec := &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}

	t.Run("presents the client certificate when the reference carries it", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		secret := multigresv1alpha1.TopoClientCertSecretName("my-cluster")
		globalTopo := multigresv1alpha1.GlobalTopoServerRef{
			Address:          "my-cluster-global-topo.default.svc:2379",
			RootPath:         "/multigres/global",
			CASecret:         secret,
			ClientCertSecret: secret,
		}
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
		ck.Require().NoError(err, "BuildMultiadminDeployment() error =")
		c := got.Spec.Template.Spec.Containers[0]
		ck.True(
			hasArgValue(c.Args, "--topo-etcd-tls-cert", multigresv1alpha1.TopoClientTLSCertFile),
			"missing --topo-etcd-tls-cert flag: %v",
			c.Args,
		)
		ck.True(
			containerMountsVolume(c, multigresv1alpha1.TopoClientTLSVolumeName),
			"multiadmin does not mount the topo client certificate",
		)
		volumes := got.Spec.Template.Spec.Volumes
		ck.True(
			podHasVolume(volumes, multigresv1alpha1.TopoClientTLSVolumeName),
			"topo client volume missing from pod spec",
		)
	})

	t.Run("renders unchanged when the reference carries no credential", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		globalTopo := multigresv1alpha1.GlobalTopoServerRef{
			Address:  "my-cluster-global-topo.default.svc:2379",
			RootPath: "/multigres/global",
		}
		got, err := BuildMultiadminDeployment(cluster, spec, nil, globalTopo, scheme)
		ck.Require().NoError(err, "BuildMultiadminDeployment() error =")
		c := got.Spec.Template.Spec.Containers[0]
		for _, a := range c.Args {
			ck.NotEq(
				"--topo-etcd-tls-cert",
				a,
				"topo TLS flag present with no credential on the reference",
			)
		}
		ck.False(
			podHasVolume(got.Spec.Template.Spec.Volumes, multigresv1alpha1.TopoClientTLSVolumeName),
			"topo client volume present with no credential on the reference",
		)
	})
}

func hasArgValue(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func containerMountsVolume(c corev1.Container, name string) bool {
	for _, m := range c.VolumeMounts {
		if m.Name == name {
			return true
		}
	}
	return false
}

func podHasVolume(vols []corev1.Volume, name string) bool {
	for _, v := range vols {
		if v.Name == name {
			return true
		}
	}
	return false
}
