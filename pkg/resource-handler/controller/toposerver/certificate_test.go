package toposerver

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func certScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(certs.GVK, &unstructured.Unstructured{})
	listGVK := certs.GVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func certTestTopoServer(tls *multigresv1alpha1.TopoTLSConfig) *multigresv1alpha1.TopoServer {
	return &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-global-topo",
			Namespace: "supabase",
			UID:       "toposerver-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(3))},
			TLS:  tls,
		},
	}
}

func TestBuildServingCertificate(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	})

	got, err := BuildServingCertificate(toposerver, scheme)
	c.Require().NoError(err, "BuildServingCertificate() error =")
	c.Require().NotNil(got, "BuildServingCertificate() = nil, want a Certificate")

	wantName := "test-cluster-global-topo-topo-server-tls"
	c.Eq(wantName, got.GetName(), "name")
	c.Eq("supabase", got.GetNamespace(), "namespace")
	ownerRefs := got.GetOwnerReferences()
	c.Require().
		False(len(ownerRefs) != 1 || ownerRefs[0].Kind != "TopoServer", "ownerReferences = %+v, want one TopoServer ref", ownerRefs)

	spec, ok := got.Object["spec"].(map[string]any)
	c.Require().True(ok, "spec is not a map")

	// Both Services the controller creates have to verify: the client Service
	// (BuildClientService) and the headless peer Service (BuildHeadlessService).
	wantDNSNames := []any{
		"test-cluster-global-topo",
		"test-cluster-global-topo.supabase",
		"test-cluster-global-topo.supabase.svc",
		"test-cluster-global-topo.supabase.svc.cluster.local",
		"test-cluster-global-topo-headless",
		"test-cluster-global-topo-headless.supabase",
		"test-cluster-global-topo-headless.supabase.svc",
		"test-cluster-global-topo-headless.supabase.svc.cluster.local",
		"*.test-cluster-global-topo-headless.supabase.svc.cluster.local",
	}
	c.Eq("", cmp.Diff(wantDNSNames, spec["dnsNames"]), "dnsNames mismatch (-want +got):\n")

	wantSubject := fmt.Sprintf(
		certs.LiteralSubjectTemplate,
		"test-cluster-global-topo.supabase.svc.cluster.local",
	)
	c.Eq(
		"",
		cmp.Diff(wantSubject, spec["literalSubject"]),
		"literalSubject mismatch (-want +got):\n",
	)
	c.Eq("", cmp.Diff(wantName, spec["secretName"]), "secretName mismatch (-want +got):\n")

	// The topology server is shared infrastructure, so it takes the issuer from
	// the topology TLS config rather than any single cluster's issuer.
	wantIssuerRef := map[string]any{
		"name":  "multigres-infra-issuer",
		"kind":  "ClusterIssuer",
		"group": "cert-manager.io",
	}
	c.Eq("", cmp.Diff(wantIssuerRef, spec["issuerRef"]), "issuerRef mismatch (-want +got):\n")
}

func TestBuildServingCertificateSANsCoverBothServices(t *testing.T) {
	c := assert.NewAborting(t)
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})

	clientSvc, err := BuildClientService(toposerver, scheme)
	c.NoError(err, "BuildClientService() error =")
	headlessSvc, err := BuildHeadlessService(toposerver, scheme)
	c.NoError(err, "BuildHeadlessService() error =")

	cert, err := BuildServingCertificate(toposerver, scheme)
	c.NoError(err, "BuildServingCertificate() error =")
	sans, _, err := unstructured.NestedSlice(cert.Object, "spec", "dnsNames")
	c.NoError(err, "NestedSlice(dnsNames) error =")
	covered := make(map[string]struct{}, len(sans))
	for _, s := range sans {
		covered[s.(string)] = struct{}{}
	}

	for _, svc := range []*corev1.Service{clientSvc, headlessSvc} {
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", svc.Name, svc.Namespace)
		if _, ok := covered[fqdn]; !ok {
			t.Errorf("Service %q FQDN %q is not a SAN; SANs = %v", svc.Name, fqdn, sans)
		}
	}
}

func TestBuildServingCertificateDefaultIssuer(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})

	cert, err := BuildServingCertificate(toposerver, scheme)
	c.Require().NoError(err, "BuildServingCertificate() error =")
	issuer, _, _ := unstructured.NestedString(cert.Object, "spec", "issuerRef", "name")
	c.Eq(certs.DefaultIssuerName, issuer, "issuerRef.name")
}

func TestBuildServingCertificateDisabled(t *testing.T) {
	scheme := certScheme()

	for name, tls := range map[string]*multigresv1alpha1.TopoTLSConfig{
		"unset":    nil,
		"disabled": {Enabled: ptr.To(false)},
	} {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got, err := BuildServingCertificate(certTestTopoServer(tls), scheme)
			c.Require().NoError(err, "BuildServingCertificate() error =")
			c.Nil(got, "BuildServingCertificate()")
		})
	}
}

func TestReconcileCertificate(t *testing.T) {
	certName := "test-cluster-global-topo-topo-server-tls"

	t.Run("applies the certificate when enabled", func(t *testing.T) {
		ck := assert.NewAborting(t)
		scheme := certScheme()
		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.NoError(
			r.reconcileCertificate(context.Background(), toposerver),
			"reconcileCertificate() error =",
		)

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		ck.NoError(c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		), "expected serving Certificate, got error")
	})

	t.Run("reports a foreign certificate collision when enabled", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := certScheme()
		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		foreign, err := BuildServingCertificate(toposerver, scheme)
		ck.Require().NoError(err, "BuildServingCertificate() error =")
		foreign.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: "example.com/v1",
			Kind:       "Other",
			Name:       "other",
			UID:        "other-uid",
			Controller: ptr.To(true),
		}})

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver, foreign).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		before := &unstructured.Unstructured{}
		before.SetGroupVersionKind(certs.GVK)
		ck.Require().NoError(c.Get(context.Background(), key, before), "Get() error =")

		ck.Require().
			Error(r.reconcileCertificate(context.Background(), toposerver), "reconcileCertificate() error = nil, want collision error")

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		ck.Require().
			NoError(c.Get(context.Background(), key, got), "foreign Certificate was modified or deleted")
		ck.EqDiff(before.Object, got.Object, "foreign Certificate changed")
	})

	t.Run("prunes the certificate and its secret when disabled", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		existing, err := BuildServingCertificate(enabled, scheme)
		ck.Require().NoError(err, "BuildServingCertificate() error =")
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: certName, Namespace: "supabase"},
		}

		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)})
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, existing, secret).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.Require().
			NoError(r.reconcileCertificate(context.Background(), toposerver), "reconcileCertificate() error =")

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		ck.Error(c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		), "expected serving Certificate to be deleted")
		ck.Error(c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			&corev1.Secret{},
		), "expected generated Secret to be deleted")
	})

	t.Run("issues nothing when TLS is unset", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := certScheme()
		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.Require().
			NoError(r.reconcileCertificate(context.Background(), toposerver), "reconcileCertificate() error =")

		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(certs.GVK)
		ck.Require().NoError(c.List(context.Background(), list), "List() error =")
		ck.Empty(list.Items, "got %d Certificates, want 0", len(list.Items))
	})

	// Removing the TLS block is as common as setting enabled to false, and it
	// must not leave private key material behind either.
	t.Run("cleans up when the TLS block is removed", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		existing, err := BuildServingCertificate(enabled, scheme)
		ck.Require().NoError(err, "BuildServingCertificate() error =")
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: certName, Namespace: "supabase"},
		}

		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, existing, secret).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.Require().
			NoError(r.reconcileCertificate(context.Background(), toposerver), "reconcileCertificate() error =")

		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		ck.Error(
			c.Get(context.Background(), key, got),
			"expected orphaned Certificate to be deleted",
		)
		ck.Error(
			c.Get(context.Background(), key, &corev1.Secret{}),
			"expected orphaned Secret to be deleted",
		)
	})

	// A same-named Certificate owned by something else is not ours to delete.
	t.Run("leaves an unowned certificate alone", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		foreign, err := BuildServingCertificate(enabled, scheme)
		ck.Require().NoError(err, "BuildServingCertificate() error =")
		foreign.SetOwnerReferences(nil)

		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, foreign).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.Require().
			NoError(r.reconcileCertificate(context.Background(), toposerver), "reconcileCertificate() error =")

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		ck.NoError(c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		), "unowned Certificate was deleted")
	})
}

// With topology TLS off the etcd StatefulSet renders exactly as it did before
// enforcement existed: plaintext listeners and no serving certificate mount.
// This is the invariant that keeps the change safe to merge with the gate off.
func TestTopoTLSOffRendersPlaintext(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := certScheme()

	sts, err := BuildStatefulSet(certTestTopoServer(nil), scheme)
	c.Require().NoError(err, "BuildStatefulSet() error =")

	for _, vol := range sts.Spec.Template.Spec.Volumes {
		c.Require().
			NotEq(TopoServerTLSVolumeName, vol.Name, "serving certificate volume present with topology TLS off")
	}
	env := etcdEnvMap(t, sts)
	c.Eq("http://[::]:2379", env["ETCD_LISTEN_CLIENT_URLS"], "ETCD_LISTEN_CLIENT_URLS")
	_, ok := env["ETCD_CLIENT_CERT_AUTH"]
	c.False(ok, "ETCD_CLIENT_CERT_AUTH set with topology TLS off")
}

// With topology TLS on, etcd serves its client and peer listeners over TLS,
// requires a client certificate on each, and mounts the issued serving
// certificate. The metrics listener stays plaintext so the probes keep working.
func TestTopoTLSOnRequiresClientCerts(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := certScheme()

	sts, err := BuildStatefulSet(certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	}), scheme)
	c.Require().NoError(err, "BuildStatefulSet() error =")

	var servingVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == TopoServerTLSVolumeName {
			servingVol = &sts.Spec.Template.Spec.Volumes[i]
		}
	}
	c.Require().NotNil(servingVol, "serving certificate volume missing with topology TLS on")
	if servingVol.Secret == nil ||
		servingVol.Secret.SecretName != multigresv1alpha1.TopoServerCertSecretName(
			"test-cluster-global-topo",
		) {
		t.Errorf("serving certificate volume references the wrong Secret: %+v", servingVol.Secret)
	}

	var mounted bool
	for _, m := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.Name == TopoServerTLSVolumeName {
			mounted = true
			c.False(
				!m.ReadOnly || m.MountPath != TopoServerTLSMountPath,
				"serving certificate mount = %+v, want read-only at %s",
				m,
				TopoServerTLSMountPath,
			)
		}
	}
	c.True(mounted, "serving certificate is not mounted into the etcd container")

	env := etcdEnvMap(t, sts)
	wantEnv := map[string]string{
		"ETCD_LISTEN_CLIENT_URLS":    "https://[::]:2379",
		"ETCD_LISTEN_PEER_URLS":      "https://[::]:2380",
		"ETCD_LISTEN_METRICS_URLS":   "http://[::]:2381",
		"ETCD_CLIENT_CERT_AUTH":      "true",
		"ETCD_PEER_CLIENT_CERT_AUTH": "true",
		"ETCD_CERT_FILE":             TopoServerTLSCertFile,
		"ETCD_KEY_FILE":              TopoServerTLSKeyFile,
		"ETCD_TRUSTED_CA_FILE":       TopoServerTLSCAFile,
		"ETCD_PEER_CERT_FILE":        TopoServerTLSCertFile,
		"ETCD_PEER_KEY_FILE":         TopoServerTLSKeyFile,
		"ETCD_PEER_TRUSTED_CA_FILE":  TopoServerTLSCAFile,
	}
	for k, want := range wantEnv {
		got := env[k]
		c.Eq(want, got, "%s = %q, want", k, got)
	}
}

// etcdEnvMap collects the etcd container's environment variables that carry a
// literal value into a lookup keyed by name.
func etcdEnvMap(t *testing.T, sts *appsv1.StatefulSet) map[string]string {
	t.Helper()
	env := map[string]string{}
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		if e.ValueFrom == nil {
			env[e.Name] = e.Value
		}
	}
	return env
}
