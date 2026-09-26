package multigrescluster

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

// registerCertManagerTypes registers cert-manager Certificate as an
// unstructured type so the fake client can store, retrieve, and list
// it without mutating the scheme at runtime.
func registerCertManagerTypes(s *runtime.Scheme) {
	s.AddKnownTypeWithName(certGVK, &unstructured.Unstructured{})
	listGVK := certGVK
	listGVK.Kind += "List"
	s.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
}

func TestBuildCertificate(t *testing.T) {
	tests := map[string]struct {
		cluster        *multigresv1alpha1.MultigresCluster
		wantName       string
		wantDNSNames   []any
		wantSubject    string
		wantSecretName string
		wantIssuerName string
	}{
		"standard certCommonName with db prefix": {
			cluster: &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "supabase",
					UID:       "cluster-uid-1",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "db.abc123.supabase.red",
				},
			},
			wantName: "db.abc123.supabase.red",
			wantDNSNames: []any{
				"db.abc123.supabase.red",
				"abc123.supabase.red",
			},
			wantSubject:    "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=db.abc123.supabase.red",
			wantSecretName: multigresv1alpha1.CertSecretName,
		},
		"certCommonName without db prefix": {
			cluster: &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "supabase",
					UID:       "cluster-uid-2",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "custom.example.com",
				},
			},
			wantName:       "custom.example.com",
			wantDNSNames:   []any{"custom.example.com"},
			wantSubject:    "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=custom.example.com",
			wantSecretName: multigresv1alpha1.CertSecretName,
		},
		"custom issuerName overrides default": {
			cluster: &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "supabase",
					UID:       "cluster-uid-3",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "custom.example.com",
					IssuerName:     "my-custom-issuer",
				},
			},
			wantName:       "custom.example.com",
			wantDNSNames:   []any{"custom.example.com"},
			wantSubject:    "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=custom.example.com",
			wantSecretName: multigresv1alpha1.CertSecretName,
			wantIssuerName: "my-custom-issuer",
		},
	}

	scheme := setupScheme()

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got, err := buildCertificate(tc.cluster, scheme)
			c.Require().NoError(err, "buildCertificate() error")

			wantGVK := schema.GroupVersionKind{
				Group:   "cert-manager.io",
				Version: "v1",
				Kind:    "Certificate",
			}
			c.EqDiff(wantGVK, got.GroupVersionKind(), "GVK mismatch")
			c.Eq(tc.wantName, got.GetName(), "Name")

			// Verify owner reference points to the cluster
			ownerRefs := got.GetOwnerReferences()
			c.Require().Len(ownerRefs, 1, "expected 1 ownerReference, got %d", len(ownerRefs))
			c.Eq(tc.cluster.Name, ownerRefs[0].Name, "ownerRef.Name")
			c.Eq("MultigresCluster", ownerRefs[0].Kind, "ownerRef.Kind")

			spec, ok := got.Object["spec"].(map[string]any)
			c.Require().True(ok, "spec is not a map")
			c.Eq(
				"",
				cmp.Diff(tc.wantDNSNames, spec["dnsNames"]),
				"dnsNames mismatch (-want +got):\n",
			)
			c.Eq(
				"",
				cmp.Diff(tc.wantSubject, spec["literalSubject"]),
				"literalSubject mismatch (-want +got):\n",
			)
			c.Eq(
				"",
				cmp.Diff(tc.wantSecretName, spec["secretName"]),
				"secretName mismatch (-want +got):\n",
			)
			wantIssuer := tc.wantIssuerName
			if wantIssuer == "" {
				wantIssuer = CertIssuerName
			}
			wantIssuerRef := map[string]any{
				"name":  wantIssuer,
				"kind":  "ClusterIssuer",
				"group": "cert-manager.io",
			}
			c.Eq(
				"",
				cmp.Diff(wantIssuerRef, spec["issuerRef"]),
				"issuerRef mismatch (-want +got):\n",
			)
			wantUsages := []any{
				"digital signature",
				"key encipherment",
				"server auth",
			}
			c.Eq("", cmp.Diff(wantUsages, spec["usages"]), "usages mismatch (-want +got):\n")
		})
	}
}

func TestBuildInternalCertificates(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "MultigresCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "supabase",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			InternalTLS:    &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)},
			CertCommonName: "db.abc123.supabase.red",
		},
	}

	got, err := buildInternalCertificates(cluster, scheme)
	c.Require().NoError(err, "buildInternalCertificates() error")

	want := map[string]string{
		"multiadmin.test-cluster.supabase.multigres.internal": multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentMultiAdminTLS,
			cluster.Name,
			cluster.Namespace,
		),
		"multigateway.test-cluster.supabase.multigres.internal": multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentMultiGatewayTLS,
			cluster.Name,
			cluster.Namespace,
		),
		"multiorch.test-cluster.supabase.multigres.internal": multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentMultiOrchTLS,
			cluster.Name,
			cluster.Namespace,
		),
		"multipooler.test-cluster.supabase.multigres.internal": multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentMultiPoolerTLS,
			cluster.Name,
			cluster.Namespace,
		),
		"multigres-operator.test-cluster.supabase.multigres.internal": multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentOperatorTLS,
			cluster.Name,
			cluster.Namespace,
		),
	}
	c.Require().Len(got, len(want), "got %d certs, want", len(got))
	for _, cert := range got {
		secretName, ok := want[cert.GetName()]
		if !ok {
			t.Fatalf("unexpected Certificate %q", cert.GetName())
		}
		spec, ok := cert.Object["spec"].(map[string]any)
		c.Require().True(ok, "spec is not a map")
		c.Eq(
			"",
			cmp.Diff(secretName, spec["secretName"]),
			"secretName mismatch for %s (-want +got):\n",
			cert.GetName(),
		)
		wantCommonName := cert.GetName()
		wantSubject := "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=" + wantCommonName
		c.Eq(
			"",
			cmp.Diff(wantSubject, spec["literalSubject"]),
			"literalSubject mismatch for %s (-want +got):\n",
			cert.GetName(),
		)
		wantUsages := []any{
			"digital signature",
			"key encipherment",
			"server auth",
			"client auth",
		}
		if cert.GetName() == "multigres-operator.test-cluster.supabase.multigres.internal" {
			wantUsages = []any{
				"digital signature",
				"key encipherment",
				"client auth",
			}
		}
		c.Eq(
			"",
			cmp.Diff(wantUsages, spec["usages"]),
			"usages mismatch for %s (-want +got):\n",
			cert.GetName(),
		)
		wantDNSNames := []any{cert.GetName()}
		if cert.GetName() == "multigres-operator.test-cluster.supabase.multigres.internal" {
			wantDNSNames = []any{}
		}
		if cert.GetName() == "multigateway.test-cluster.supabase.multigres.internal" {
			// multigateway's cert also needs to verify against the logical
			// MultiPooler identity used for gateway-to-gateway cancel forwarding.
			wantDNSNames = append(
				wantDNSNames,
				"multipooler.test-cluster.supabase.multigres.internal",
			)
		}
		c.Eq(
			"",
			cmp.Diff(wantDNSNames, spec["dnsNames"]),
			"dnsNames mismatch for %s (-want +got):\n",
			cert.GetName(),
		)
	}

	changedExternalName := cluster.DeepCopy()
	changedExternalName.Spec.CertCommonName = "db.changed.supabase.red"
	gotAfterExternalNameChange, err := buildInternalCertificates(changedExternalName, scheme)
	c.Require().NoError(err, "buildInternalCertificates() after external name change")
	c.Require().
		Len(gotAfterExternalNameChange, len(got), "got %d certs after external name change, want", len(gotAfterExternalNameChange))

	changedByName := make(map[string]*unstructured.Unstructured, len(gotAfterExternalNameChange))
	for _, cert := range gotAfterExternalNameChange {
		changedByName[cert.GetName()] = cert
	}

	for _, originalCert := range got {
		changedCert, ok := changedByName[originalCert.GetName()]
		if !ok {
			t.Fatalf("Certificate %q missing after external name change", originalCert.GetName())
		}
		c.EqDiff(
			originalCert,
			changedCert,
			"internal Certificate %q changed with external CertCommonName",
			originalCert.GetName(),
		)
	}
}

func TestTruncateCommonName(t *testing.T) {
	const shortCN = "multiadmin.mgc-x.ha-project-x.multigres.internal"
	const longCN = "multiadmin.mgc-iaogrkvrpaubkinljowl.ha-project-iaogrkvrpaubkinljowl.multigres.internal"
	const longCNVariant = "multiadmin.mgc-iaogrkvrpaubkinljowm.ha-project-iaogrkvrpaubkinljowl.multigres.internal"

	t.Run("short CN is returned unchanged", func(t *testing.T) {
		c := assert.NewCollecting(t)
		c.Require().LessOrEqual(maxCommonNameBytes, len(shortCN), "test fixture shortCN is")
		got := truncateCommonName(shortCN)
		c.EqDiff(shortCN, got, "truncateCommonName() mismatch")
	})

	t.Run("long CN is truncated to the X.509 limit", func(t *testing.T) {
		c := assert.NewCollecting(t)
		c.Require().Greater(maxCommonNameBytes, len(longCN), "test fixture longCN is")
		got := truncateCommonName(longCN)
		c.LessOrEqual(
			maxCommonNameBytes,
			len(got),
			"truncateCommonName() = %q (%d bytes), want <= %d bytes",
			got,
			len(got),
			maxCommonNameBytes,
		)
	})

	t.Run("truncation is deterministic", func(t *testing.T) {
		first := truncateCommonName(longCN)
		second := truncateCommonName(longCN)
		assert.NewCollecting(t).
			Eq("", cmp.Diff(first, second), "truncateCommonName() not deterministic (-first +second):\n")
	})

	t.Run("different long inputs produce different outputs", func(t *testing.T) {
		got := truncateCommonName(longCN)
		gotVariant := truncateCommonName(longCNVariant)
		assert.NewCollecting(t).NotEq(gotVariant, got, "truncateCommonName() collided")
	})
}

func TestReconcileCertificate(t *testing.T) {
	scheme := setupScheme()
	wantInternalCertificates := func(
		cluster *multigresv1alpha1.MultigresCluster,
	) map[string]string {
		want := make(map[string]string, 5)
		for _, component := range []string{
			multigresv1alpha1.ComponentMultiAdminTLS,
			multigresv1alpha1.ComponentMultiGatewayTLS,
			multigresv1alpha1.ComponentMultiOrchTLS,
			multigresv1alpha1.ComponentMultiPoolerTLS,
			multigresv1alpha1.ComponentOperatorTLS,
		} {
			name := multigresv1alpha1.ComponentCertCommonName(
				component,
				cluster.Name,
				cluster.Namespace,
			)
			want[name] = multigresv1alpha1.ComponentCertSecretName(
				component,
				cluster.Name,
				cluster.Namespace,
			)
		}
		return want
	}
	assertCertificates := func(
		t *testing.T,
		fc client.Client,
		cluster *multigresv1alpha1.MultigresCluster,
		want map[string]string,
	) {
		t.Helper()
		c := assert.NewAborting(t)
		certificates := &unstructured.UnstructuredList{}
		certificates.SetGroupVersionKind(certGVK)
		c.NoError(fc.List(
			t.Context(),
			certificates,
			client.InNamespace(cluster.Namespace),
		), "list Certificates")
		c.Len(certificates.Items, len(want), "got %d Certificates, want", len(certificates.Items))

		seen := make(map[string]struct{}, len(certificates.Items))
		for _, certificate := range certificates.Items {
			wantSecret, ok := want[certificate.GetName()]
			if !ok {
				t.Errorf("unexpected Certificate %q", certificate.GetName())
				continue
			}
			if _, duplicate := seen[certificate.GetName()]; duplicate {
				t.Errorf("duplicate Certificate %q", certificate.GetName())
			}
			seen[certificate.GetName()] = struct{}{}

			gotSecret, found, err := unstructured.NestedString(
				certificate.Object,
				"spec",
				"secretName",
			)
			if err != nil {
				t.Errorf("Certificate %q secretName: %v", certificate.GetName(), err)
			} else if !found {
				t.Errorf("Certificate %q has no secretName", certificate.GetName())
			} else if gotSecret != wantSecret {
				t.Errorf(
					"Certificate %q secretName = %q, want %q",
					certificate.GetName(),
					gotSecret,
					wantSecret,
				)
			}
			if len(certificate.GetOwnerReferences()) != 1 ||
				certificate.GetOwnerReferences()[0].UID != cluster.UID {
				t.Errorf(
					"Certificate %q is not exclusively owned by cluster %q",
					certificate.GetName(),
					cluster.Name,
				)
			}
		}
	}

	t.Run("creates only internal Certificates when internal TLS is enabled", func(t *testing.T) {
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c1", Namespace: "default", UID: "uid-1",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				InternalTLS: &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)},
			},
		}
		assert.NewAborting(t).
			NoError(r.reconcileCertificate(t.Context(), cluster), "unexpected error")
		assertCertificates(t, fc, cluster, wantInternalCertificates(cluster))
	})

	t.Run("CertCommonName alone creates only the public Certificate", func(t *testing.T) {
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c2", Namespace: "default", UID: "uid-2",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				CertCommonName: "db.abc123.supabase.red",
			},
		}
		assert.NewAborting(t).
			NoError(r.reconcileCertificate(t.Context(), cluster), "unexpected error")

		assertCertificates(t, fc, cluster, map[string]string{
			cluster.Spec.CertCommonName: multigresv1alpha1.CertSecretName,
		})
	})
	t.Run(
		"disabled internal TLS creates no Certificates without a public name",
		func(t *testing.T) {
			fc := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &MultigresClusterReconciler{
				Client:   fc,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c2-disabled", Namespace: "default", UID: "uid-2-disabled",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					InternalTLS: &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(false)},
				},
			}
			assert.NewAborting(t).
				NoError(r.reconcileCertificate(t.Context(), cluster), "unexpected error")
			assertCertificates(t, fc, cluster, map[string]string{})
		},
	)
	t.Run("creates internal and public Certificates when both are enabled", func(t *testing.T) {
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c2-both", Namespace: "default", UID: "uid-2-both",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				InternalTLS:    &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)},
				CertCommonName: "db.both.supabase.red",
			},
		}
		assert.NewAborting(t).
			NoError(r.reconcileCertificate(t.Context(), cluster), "unexpected error")

		want := wantInternalCertificates(cluster)
		want[cluster.Spec.CertCommonName] = multigresv1alpha1.CertSecretName
		assertCertificates(t, fc, cluster, want)
	})

	t.Run(
		"disabling internal TLS deletes internal Certificates but keeps public",
		func(t *testing.T) {
			c := assert.NewAborting(t)
			fc := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &MultigresClusterReconciler{
				Client:   fc,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			cluster := &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "c2-toggle", Namespace: "default", UID: "uid-2-toggle",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					InternalTLS:    &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)},
					CertCommonName: "db.toggle.supabase.red",
				},
			}
			c.NoError(
				r.reconcileCertificate(t.Context(), cluster),
				"create internal and public Certificates",
			)
			want := wantInternalCertificates(cluster)
			want[cluster.Spec.CertCommonName] = multigresv1alpha1.CertSecretName
			assertCertificates(t, fc, cluster, want)

			cluster.Spec.InternalTLS.Enabled = ptr.To(false)
			c.NoError(r.reconcileCertificate(t.Context(), cluster), "disable internal TLS")
			assertCertificates(t, fc, cluster, map[string]string{
				cluster.Spec.CertCommonName: multigresv1alpha1.CertSecretName,
			})
		},
	)

	t.Run("idempotent on repeated calls", func(t *testing.T) {
		c := assert.NewAborting(t)
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c3", Namespace: "default", UID: "uid-3",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				CertCommonName: "db.xyz.supabase.red",
			},
		}
		c.NoError(r.reconcileCertificate(t.Context(), cluster), "first call")
		c.NoError(r.reconcileCertificate(t.Context(), cluster), "second call")
	})

	t.Run("no Patch when nothing changed", func(t *testing.T) {
		c := assert.NewCollecting(t)
		var patchCount int
		fc := fake.NewClientBuilder().
			WithScheme(scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(
					ctx context.Context,
					cli client.WithWatch,
					obj client.Object,
					patch client.Patch,
					opts ...client.PatchOption,
				) error {
					patchCount++
					return cli.Patch(ctx, obj, patch, opts...)
				},
			}).
			Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c8", Namespace: "default", UID: "uid-8",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				InternalTLS:    &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)},
				CertCommonName: "db.noop.supabase.red",
			},
		}

		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "first reconcile")
		c.Require().Eq(6, patchCount, "patchCount after first reconcile")

		// Reconciling again with the same spec should not re-patch any
		// Certificate since the live specs already match desired.
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "second reconcile")
		c.Eq(6, patchCount, "patchCount after second reconcile")
	})

	t.Run("CN change updates Certificate", func(t *testing.T) {
		c := assert.NewCollecting(t)
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c4", Namespace: "default", UID: "uid-4",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				CertCommonName: "db.old.supabase.red",
			},
		}

		// Create with old CN
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "create old")

		// Change CN
		cluster.Spec.CertCommonName = "db.new.supabase.red"
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "create new")

		// New cert should exist
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		c.Require().NoError(fc.Get(t.Context(), types.NamespacedName{
			Name: "db.new.supabase.red", Namespace: "default",
		}, got), "new Certificate should exist")

		// Old cert should be deleted by reconcileCertificate
		old := &unstructured.Unstructured{}
		old.SetGroupVersionKind(certGVK)
		c.Error(fc.Get(t.Context(), types.NamespacedName{
			Name: "db.old.supabase.red", Namespace: "default",
		}, old), "old Certificate should be deleted on CN change")
	})

	t.Run("CN unset cleans up Certificate", func(t *testing.T) {
		c := assert.NewCollecting(t)
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c5", Namespace: "default", UID: "uid-5",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				CertCommonName: "db.cleanup.supabase.red",
			},
		}

		// Create cert
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "create")

		// Verify it exists
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		c.Require().NoError(fc.Get(t.Context(), types.NamespacedName{
			Name: "db.cleanup.supabase.red", Namespace: "default",
		}, got), "Certificate should exist before cleanup")

		// Unset CN and reconcile
		cluster.Spec.CertCommonName = ""
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "cleanup")

		// Cert should be deleted
		err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.cleanup.supabase.red", Namespace: "default",
		}, got)
		c.Error(err, "Certificate should be deleted after unsetting CN")
	})

	t.Run(
		"cleanup ignores certs not owned by this cluster",
		func(t *testing.T) {
			c := assert.NewAborting(t)
			fc := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &MultigresClusterReconciler{
				Client:   fc,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			// Pre-create a Certificate owned by a different cluster
			other := &unstructured.Unstructured{}
			other.SetGroupVersionKind(certGVK)
			other.SetName("db.other.supabase.red")
			other.SetNamespace("default")
			other.SetOwnerReferences([]metav1.OwnerReference{
				{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
					Name:       "other-cluster",
					UID:        "other-uid",
				},
			})
			other.Object["spec"] = map[string]any{
				"secretName": multigresv1alpha1.CertSecretName,
			}
			c.NoError(fc.Create(t.Context(), other), "failed to create other cert")

			// Our cluster has no CN — should not delete the other cert
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c6", Namespace: "default", UID: "uid-6",
				},
			}
			c.NoError(r.reconcileCertificate(t.Context(), cluster), "cleanup")

			// Other cert should still exist
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(certGVK)
			c.NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: "db.other.supabase.red", Namespace: "default",
			}, got), "Certificate owned by another cluster should survive")
		},
	)

	t.Run(
		"legacy internal identity retries cleanup after Secret deletion fails",
		func(t *testing.T) {
			c := assert.NewCollecting(t)
			oldCertificateName := "multipooler.db.secretold.supabase.red"
			oldSecretName := oldCertificateName
			deleteFailure := errors.New("injected Secret deletion failure")
			failSecretDelete := false
			fc := fake.NewClientBuilder().
				WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					Delete: func(
						ctx context.Context,
						cli client.WithWatch,
						obj client.Object,
						opts ...client.DeleteOption,
					) error {
						if obj.GetName() == oldSecretName && failSecretDelete {
							failSecretDelete = false
							return deleteFailure
						}
						return cli.Delete(ctx, obj, opts...)
					},
				}).
				Build()
			r := &MultigresClusterReconciler{
				Client:   fc,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			cluster := &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "c9", Namespace: "default", UID: "uid-9",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "db.secretold.supabase.red",
				},
			}

			legacyCertificate, err := buildCertificateFromSpec(cluster, scheme, certSpec{
				name:       oldCertificateName,
				secretName: oldSecretName,
				commonName: oldCertificateName,
				dnsNames:   []any{oldCertificateName},
				usages:     []any{"server auth", "client auth"},
			})
			c.Require().NoError(err, "build legacy Certificate")
			c.Require().
				NoError(fc.Create(t.Context(), legacyCertificate), "create legacy Certificate")
			oldSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      oldSecretName,
					Namespace: "default",
				},
			}
			c.Require().NoError(fc.Create(t.Context(), oldSecret), "failed to create old secret")

			failSecretDelete = true
			err = r.reconcileCertificate(t.Context(), cluster)
			c.Require().ErrorIs(err, deleteFailure, "first cleanup error")

			staleCertificate := &unstructured.Unstructured{}
			staleCertificate.SetGroupVersionKind(certGVK)
			c.NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: oldCertificateName, Namespace: "default",
			}, staleCertificate), "stale Certificate should remain after Secret deletion failure")
			c.NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: oldSecretName, Namespace: "default",
			}, &corev1.Secret{}), "stale Secret should remain after deletion failure")

			c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "cleanup retry")
			c.Error(fc.Get(t.Context(), types.NamespacedName{
				Name: oldCertificateName, Namespace: "default",
			}, staleCertificate), "stale Certificate should be deleted on cleanup retry")
			c.Error(fc.Get(t.Context(), types.NamespacedName{
				Name: oldSecretName, Namespace: "default",
			}, &corev1.Secret{}), "stale internal Secret should be deleted on cleanup retry")
		},
	)

	t.Run("CN change keeps the shared external Secret", func(t *testing.T) {
		c := assert.NewCollecting(t)
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "multigres.com/v1alpha1",
				Kind:       "MultigresCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "c10", Namespace: "default", UID: "uid-10",
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				CertCommonName: "db.sharedold.supabase.red",
			},
		}
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "create old")

		// The external gateway cert always uses the fixed secret name, so it
		// must survive the CN rotation for the new Certificate to reuse it.
		sharedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      multigresv1alpha1.CertSecretName,
				Namespace: "default",
			},
		}
		c.Require().NoError(fc.Create(t.Context(), sharedSecret), "failed to create shared secret")

		cluster.Spec.CertCommonName = "db.sharednew.supabase.red"
		c.Require().NoError(r.reconcileCertificate(t.Context(), cluster), "create new")

		c.NoError(fc.Get(t.Context(), types.NamespacedName{
			Name: multigresv1alpha1.CertSecretName, Namespace: "default",
		}, &corev1.Secret{}), "shared external Secret should survive CN rotation")
	})

	t.Run(
		"reports a collision for a Certificate with matching spec but no ownerRef",
		func(t *testing.T) {
			c := assert.NewCollecting(t)
			fc := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &MultigresClusterReconciler{
				Client:   fc,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			cluster := &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "c11", Namespace: "default", UID: "uid-11",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "db.unmanaged.supabase.red",
				},
			}

			desired, err := buildCertificate(cluster, scheme)
			c.Require().NoError(err, "buildCertificate")
			// Pre-create a Certificate with a matching spec but no
			// ownerRef, simulating one left unmanaged by a prior bug.
			unowned := &unstructured.Unstructured{}
			unowned.SetGroupVersionKind(certGVK)
			unowned.SetName(desired.GetName())
			unowned.SetNamespace("default")
			unowned.Object["spec"] = desired.Object["spec"]
			c.Require().NoError(fc.Create(t.Context(), unowned), "failed to create unowned cert")

			c.Require().
				Error(r.reconcileCertificate(t.Context(), cluster), "reconcile: got nil error, want collision error")

			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(certGVK)
			c.Require().NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: desired.GetName(), Namespace: "default",
			}, got), "Certificate should exist")
			c.Empty(got.GetOwnerReferences(), "foreign Certificate should not be adopted")
		},
	)

	t.Run(
		"two clusters in same namespace get independent certs",
		func(t *testing.T) {
			c := assert.NewCollecting(t)
			// Regression: the original cell-level architecture had
			// multiple cells fighting over the same Certificate with
			// ownerRef flipping. Moving to the cluster controller
			// eliminates that. This test verifies two clusters in the
			// same namespace each own their own Certificate with no
			// interference.
			fc := fake.NewClientBuilder().WithScheme(scheme).Build()

			clusterA := &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-a", Namespace: "default", UID: "uid-a",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "db.projA.supabase.red",
				},
			}
			clusterB := &multigresv1alpha1.MultigresCluster{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "multigres.com/v1alpha1",
					Kind:       "MultigresCluster",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-b", Namespace: "default", UID: "uid-b",
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					CertCommonName: "db.projB.supabase.red",
				},
			}

			rA := &MultigresClusterReconciler{
				Client: fc, Scheme: scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			rB := &MultigresClusterReconciler{
				Client: fc, Scheme: scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			c.Require().NoError(rA.reconcileCertificate(
				t.Context(), clusterA,
			), "cluster-a reconcile")
			c.Require().NoError(rB.reconcileCertificate(
				t.Context(), clusterB,
			), "cluster-b reconcile")

			// Both certs exist
			for _, name := range []string{
				"db.projA.supabase.red",
				"db.projB.supabase.red",
			} {
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(certGVK)
				c.NoError(fc.Get(t.Context(), types.NamespacedName{
					Name: name, Namespace: "default",
				}, got), "Certificate %q should exist", name)
			}

			// Each cert is owned by the correct cluster
			certA := &unstructured.Unstructured{}
			certA.SetGroupVersionKind(certGVK)
			c.Require().NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projA.supabase.red", Namespace: "default",
			}, certA), "failed to get certA")
			c.Eq("uid-a", certA.GetOwnerReferences()[0].UID, "certA owner UID")

			certB := &unstructured.Unstructured{}
			certB.SetGroupVersionKind(certGVK)
			c.Require().NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projB.supabase.red", Namespace: "default",
			}, certB), "failed to get certB")
			c.Eq("uid-b", certB.GetOwnerReferences()[0].UID, "certB owner UID")

			// Unsetting CN on cluster-a only deletes its cert
			clusterA.Spec.CertCommonName = ""
			c.Require().NoError(rA.reconcileCertificate(
				t.Context(), clusterA,
			), "cluster-a cleanup")

			gone := &unstructured.Unstructured{}
			gone.SetGroupVersionKind(certGVK)
			c.Error(fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projA.supabase.red", Namespace: "default",
			}, gone), "cluster-a cert should be deleted")

			// cluster-b cert is untouched
			still := &unstructured.Unstructured{}
			still.SetGroupVersionKind(certGVK)
			c.NoError(fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projB.supabase.red", Namespace: "default",
			}, still), "cluster-b cert should survive")
		},
	)
}
