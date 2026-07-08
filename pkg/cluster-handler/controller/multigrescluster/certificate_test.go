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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
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
	}

	scheme := setupScheme()

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := buildCertificate(tc.cluster, scheme)
			if err != nil {
				t.Fatalf("buildCertificate() error: %v", err)
			}

			wantGVK := schema.GroupVersionKind{
				Group:   "cert-manager.io",
				Version: "v1",
				Kind:    "Certificate",
			}
			if diff := cmp.Diff(wantGVK, got.GroupVersionKind()); diff != "" {
				t.Errorf("GVK mismatch (-want +got):\n%s", diff)
			}
			if got.GetName() != tc.wantName {
				t.Errorf("Name = %q, want %q", got.GetName(), tc.wantName)
			}

			// Verify owner reference points to the cluster
			ownerRefs := got.GetOwnerReferences()
			if len(ownerRefs) != 1 {
				t.Fatalf("expected 1 ownerReference, got %d", len(ownerRefs))
			}
			if ownerRefs[0].Name != tc.cluster.Name {
				t.Errorf(
					"ownerRef.Name = %q, want %q",
					ownerRefs[0].Name, tc.cluster.Name,
				)
			}
			if ownerRefs[0].Kind != "MultigresCluster" {
				t.Errorf(
					"ownerRef.Kind = %q, want MultigresCluster",
					ownerRefs[0].Kind,
				)
			}

			spec, ok := got.Object["spec"].(map[string]any)
			if !ok {
				t.Fatal("spec is not a map")
			}
			if diff := cmp.Diff(tc.wantDNSNames, spec["dnsNames"]); diff != "" {
				t.Errorf("dnsNames mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantSubject, spec["literalSubject"]); diff != "" {
				t.Errorf("literalSubject mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantSecretName, spec["secretName"]); diff != "" {
				t.Errorf("secretName mismatch (-want +got):\n%s", diff)
			}
			wantIssuerRef := map[string]any{
				"name":  CertIssuerName,
				"kind":  "ClusterIssuer",
				"group": "cert-manager.io",
			}
			if diff := cmp.Diff(wantIssuerRef, spec["issuerRef"]); diff != "" {
				t.Errorf("issuerRef mismatch (-want +got):\n%s", diff)
			}
			wantUsages := []any{
				"digital signature",
				"key encipherment",
				"server auth",
			}
			if diff := cmp.Diff(wantUsages, spec["usages"]); diff != "" {
				t.Errorf("usages mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildInternalCertificates(t *testing.T) {
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
			CertCommonName: "db.abc123.supabase.red",
		},
	}

	got, err := buildInternalCertificates(cluster, scheme)
	if err != nil {
		t.Fatalf("buildInternalCertificates() error: %v", err)
	}

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
	}
	if len(got) != len(want) {
		t.Fatalf("got %d certs, want %d", len(got), len(want))
	}
	for _, cert := range got {
		secretName, ok := want[cert.GetName()]
		if !ok {
			t.Fatalf("unexpected Certificate %q", cert.GetName())
		}
		spec, ok := cert.Object["spec"].(map[string]any)
		if !ok {
			t.Fatal("spec is not a map")
		}
		if diff := cmp.Diff(secretName, spec["secretName"]); diff != "" {
			t.Errorf("secretName mismatch for %s (-want +got):\n%s", cert.GetName(), diff)
		}
		wantSubject := "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=" + cert.GetName()
		if diff := cmp.Diff(wantSubject, spec["literalSubject"]); diff != "" {
			t.Errorf("literalSubject mismatch for %s (-want +got):\n%s", cert.GetName(), diff)
		}
		wantUsages := []any{
			"digital signature",
			"key encipherment",
			"server auth",
			"client auth",
		}
		if diff := cmp.Diff(wantUsages, spec["usages"]); diff != "" {
			t.Errorf("usages mismatch for %s (-want +got):\n%s", cert.GetName(), diff)
		}
		wantDNSNames := []any{cert.GetName()}
		if cert.GetName() == "multigateway.test-cluster.supabase.multigres.internal" {
			// multigateway's cert also needs to verify against the logical
			// MultiPooler identity used for gateway-to-gateway cancel forwarding.
			wantDNSNames = append(
				wantDNSNames,
				"multipooler.test-cluster.supabase.multigres.internal",
				cluster.Spec.CertCommonName,
				"abc123.supabase.red",
			)
		}
		if diff := cmp.Diff(wantDNSNames, spec["dnsNames"]); diff != "" {
			t.Errorf("dnsNames mismatch for %s (-want +got):\n%s", cert.GetName(), diff)
		}
	}

	changedExternalName := cluster.DeepCopy()
	changedExternalName.Spec.CertCommonName = "db.changed.supabase.red"
	gotAfterExternalNameChange, err := buildInternalCertificates(changedExternalName, scheme)
	if err != nil {
		t.Fatalf("buildInternalCertificates() after external name change: %v", err)
	}
	if len(gotAfterExternalNameChange) != len(got) {
		t.Fatalf(
			"got %d certs after external name change, want %d",
			len(gotAfterExternalNameChange),
			len(got),
		)
	}

	changedByName := make(map[string]*unstructured.Unstructured, len(gotAfterExternalNameChange))
	for _, cert := range gotAfterExternalNameChange {
		changedByName[cert.GetName()] = cert
	}

	gatewayName := "multigateway.test-cluster.supabase.multigres.internal"
	gatewayChecked := false
	for _, originalCert := range got {
		changedCert, ok := changedByName[originalCert.GetName()]
		if !ok {
			t.Fatalf("Certificate %q missing after external name change", originalCert.GetName())
		}
		if originalCert.GetName() != gatewayName {
			if diff := cmp.Diff(originalCert, changedCert); diff != "" {
				t.Errorf(
					"internal Certificate %q changed with external CertCommonName (-want +got):\n%s",
					originalCert.GetName(),
					diff,
				)
			}
			continue
		}

		gatewayChecked = true
		changedSpec, ok := changedCert.Object["spec"].(map[string]any)
		if !ok {
			t.Fatal("changed multigateway spec is not a map")
		}
		wantChangedDNSNames := []any{
			gatewayName,
			"multipooler.test-cluster.supabase.multigres.internal",
			"db.changed.supabase.red",
			"changed.supabase.red",
		}
		if diff := cmp.Diff(wantChangedDNSNames, changedSpec["dnsNames"]); diff != "" {
			t.Errorf("changed multigateway dnsNames mismatch (-want +got):\n%s", diff)
		}

		originalWithoutDNSNames := originalCert.DeepCopy()
		changedWithoutDNSNames := changedCert.DeepCopy()
		delete(originalWithoutDNSNames.Object["spec"].(map[string]any), "dnsNames")
		delete(changedWithoutDNSNames.Object["spec"].(map[string]any), "dnsNames")
		if diff := cmp.Diff(originalWithoutDNSNames, changedWithoutDNSNames); diff != "" {
			t.Errorf(
				"multigateway Certificate changed beyond public DNS SANs (-want +got):\n%s",
				diff,
			)
		}
	}
	if !gatewayChecked {
		t.Fatal("multigateway Certificate was not checked after external name change")
	}
}

func TestReconcileCertificate(t *testing.T) {
	scheme := setupScheme()

	t.Run("no-op when CertCommonName is empty and no prior cert", func(t *testing.T) {
		fc := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &MultigresClusterReconciler{
			Client:   fc,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: "c1", Namespace: "default", UID: "uid-1",
			},
		}
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("creates Certificate when CertCommonName is set", func(t *testing.T) {
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
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		if err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.abc123.supabase.red", Namespace: "default",
		}, got); err != nil {
			t.Fatalf("Certificate should exist: %v", err)
		}
		if got.GetOwnerReferences()[0].Name != "c2" {
			t.Errorf(
				"ownerRef.Name = %q, want c2",
				got.GetOwnerReferences()[0].Name,
			)
		}
	})

	t.Run("idempotent on repeated calls", func(t *testing.T) {
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
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("first call: %v", err)
		}
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("second call: %v", err)
		}
	})

	t.Run("no Patch when nothing changed", func(t *testing.T) {
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
				CertCommonName: "db.noop.supabase.red",
			},
		}

		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("first reconcile: %v", err)
		}
		if patchCount != 5 {
			t.Fatalf("patchCount after first reconcile = %d, want 5", patchCount)
		}

		// Reconciling again with the same spec should not re-patch any
		// Certificate since the live specs already match desired.
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("second reconcile: %v", err)
		}
		if patchCount != 5 {
			t.Errorf(
				"patchCount after second reconcile = %d, want 5 (no new patches)",
				patchCount,
			)
		}
	})

	t.Run("CN change updates Certificate", func(t *testing.T) {
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
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("create old: %v", err)
		}

		// Change CN
		cluster.Spec.CertCommonName = "db.new.supabase.red"
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("create new: %v", err)
		}

		// New cert should exist
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		if err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.new.supabase.red", Namespace: "default",
		}, got); err != nil {
			t.Fatalf("new Certificate should exist: %v", err)
		}

		// Old cert should be deleted by reconcileCertificate
		old := &unstructured.Unstructured{}
		old.SetGroupVersionKind(certGVK)
		if err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.old.supabase.red", Namespace: "default",
		}, old); err == nil {
			t.Error("old Certificate should be deleted on CN change")
		}
	})

	t.Run("CN unset cleans up Certificate", func(t *testing.T) {
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
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Verify it exists
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		if err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.cleanup.supabase.red", Namespace: "default",
		}, got); err != nil {
			t.Fatalf("Certificate should exist before cleanup: %v", err)
		}

		// Unset CN and reconcile
		cluster.Spec.CertCommonName = ""
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("cleanup: %v", err)
		}

		// Cert should be deleted
		err := fc.Get(t.Context(), types.NamespacedName{
			Name: "db.cleanup.supabase.red", Namespace: "default",
		}, got)
		if err == nil {
			t.Error("Certificate should be deleted after unsetting CN")
		}
	})

	t.Run(
		"cleanup ignores certs not owned by this cluster",
		func(t *testing.T) {
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
			if err := fc.Create(t.Context(), other); err != nil {
				t.Fatalf("failed to create other cert: %v", err)
			}

			// Our cluster has no CN — should not delete the other cert
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c6", Namespace: "default", UID: "uid-6",
				},
			}
			if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
				t.Fatalf("cleanup: %v", err)
			}

			// Other cert should still exist
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: "db.other.supabase.red", Namespace: "default",
			}, got); err != nil {
				t.Fatal("Certificate owned by another cluster should survive")
			}
		},
	)

	t.Run(
		"legacy internal identity retries cleanup after Secret deletion fails",
		func(t *testing.T) {
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
			if err != nil {
				t.Fatalf("build legacy Certificate: %v", err)
			}
			if err := fc.Create(t.Context(), legacyCertificate); err != nil {
				t.Fatalf("create legacy Certificate: %v", err)
			}
			oldSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      oldSecretName,
					Namespace: "default",
				},
			}
			if err := fc.Create(t.Context(), oldSecret); err != nil {
				t.Fatalf("failed to create old secret: %v", err)
			}

			failSecretDelete = true
			err = r.reconcileCertificate(t.Context(), cluster)
			if !errors.Is(err, deleteFailure) {
				t.Fatalf("first cleanup error = %v, want wrapped deletion failure", err)
			}

			staleCertificate := &unstructured.Unstructured{}
			staleCertificate.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: oldCertificateName, Namespace: "default",
			}, staleCertificate); err != nil {
				t.Errorf("stale Certificate should remain after Secret deletion failure: %v", err)
			}
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: oldSecretName, Namespace: "default",
			}, &corev1.Secret{}); err != nil {
				t.Errorf("stale Secret should remain after deletion failure: %v", err)
			}

			if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
				t.Fatalf("cleanup retry: %v", err)
			}
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: oldCertificateName, Namespace: "default",
			}, staleCertificate); err == nil {
				t.Error("stale Certificate should be deleted on cleanup retry")
			}
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: oldSecretName, Namespace: "default",
			}, &corev1.Secret{}); err == nil {
				t.Error("stale internal Secret should be deleted on cleanup retry")
			}
		},
	)

	t.Run("CN change keeps the shared external Secret", func(t *testing.T) {
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
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("create old: %v", err)
		}

		// The external gateway cert always uses the fixed secret name, so it
		// must survive the CN rotation for the new Certificate to reuse it.
		sharedSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      multigresv1alpha1.CertSecretName,
				Namespace: "default",
			},
		}
		if err := fc.Create(t.Context(), sharedSecret); err != nil {
			t.Fatalf("failed to create shared secret: %v", err)
		}

		cluster.Spec.CertCommonName = "db.sharednew.supabase.red"
		if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
			t.Fatalf("create new: %v", err)
		}

		if err := fc.Get(t.Context(), types.NamespacedName{
			Name: multigresv1alpha1.CertSecretName, Namespace: "default",
		}, &corev1.Secret{}); err != nil {
			t.Errorf("shared external Secret should survive CN rotation: %v", err)
		}
	})

	t.Run(
		"re-applies a Certificate with matching spec but no ownerRef",
		func(t *testing.T) {
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
			if err != nil {
				t.Fatalf("buildCertificate: %v", err)
			}
			// Pre-create a Certificate with a matching spec but no
			// ownerRef, simulating one left unmanaged by a prior bug.
			unowned := &unstructured.Unstructured{}
			unowned.SetGroupVersionKind(certGVK)
			unowned.SetName(desired.GetName())
			unowned.SetNamespace("default")
			unowned.Object["spec"] = desired.Object["spec"]
			if err := fc.Create(t.Context(), unowned); err != nil {
				t.Fatalf("failed to create unowned cert: %v", err)
			}

			if err := r.reconcileCertificate(t.Context(), cluster); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: desired.GetName(), Namespace: "default",
			}, got); err != nil {
				t.Fatalf("Certificate should exist: %v", err)
			}
			if len(got.GetOwnerReferences()) == 0 {
				t.Error("Certificate should have been re-applied with an ownerRef")
			}
		},
	)

	t.Run(
		"two clusters in same namespace get independent certs",
		func(t *testing.T) {
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

			if err := rA.reconcileCertificate(
				t.Context(), clusterA,
			); err != nil {
				t.Fatalf("cluster-a reconcile: %v", err)
			}
			if err := rB.reconcileCertificate(
				t.Context(), clusterB,
			); err != nil {
				t.Fatalf("cluster-b reconcile: %v", err)
			}

			// Both certs exist
			for _, name := range []string{
				"db.projA.supabase.red",
				"db.projB.supabase.red",
			} {
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(certGVK)
				if err := fc.Get(t.Context(), types.NamespacedName{
					Name: name, Namespace: "default",
				}, got); err != nil {
					t.Errorf("Certificate %q should exist: %v", name, err)
				}
			}

			// Each cert is owned by the correct cluster
			certA := &unstructured.Unstructured{}
			certA.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projA.supabase.red", Namespace: "default",
			}, certA); err != nil {
				t.Fatalf("failed to get certA: %v", err)
			}
			if certA.GetOwnerReferences()[0].UID != "uid-a" {
				t.Errorf(
					"certA owner UID = %q, want uid-a",
					certA.GetOwnerReferences()[0].UID,
				)
			}

			certB := &unstructured.Unstructured{}
			certB.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projB.supabase.red", Namespace: "default",
			}, certB); err != nil {
				t.Fatalf("failed to get certB: %v", err)
			}
			if certB.GetOwnerReferences()[0].UID != "uid-b" {
				t.Errorf(
					"certB owner UID = %q, want uid-b",
					certB.GetOwnerReferences()[0].UID,
				)
			}

			// Unsetting CN on cluster-a only deletes its cert
			clusterA.Spec.CertCommonName = ""
			if err := rA.reconcileCertificate(
				t.Context(), clusterA,
			); err != nil {
				t.Fatalf("cluster-a cleanup: %v", err)
			}

			gone := &unstructured.Unstructured{}
			gone.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projA.supabase.red", Namespace: "default",
			}, gone); err == nil {
				t.Error("cluster-a cert should be deleted")
			}

			// cluster-b cert is untouched
			still := &unstructured.Unstructured{}
			still.SetGroupVersionKind(certGVK)
			if err := fc.Get(t.Context(), types.NamespacedName{
				Name: "db.projB.supabase.red", Namespace: "default",
			}, still); err != nil {
				t.Errorf(
					"cluster-b cert should survive: %v", err,
				)
			}
		},
	)
}
