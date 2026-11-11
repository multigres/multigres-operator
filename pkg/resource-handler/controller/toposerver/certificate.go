package toposerver

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

// BuildServingCertificate constructs the cert-manager Certificate for a
// topology server's own serving identity. The SANs cover both Services the
// controller creates — the client Service and the headless peer Service — so
// one certificate serves client and peer traffic alike.
//
// A topology server can back several Multigres clusters, so it is not a tenant
// resource: its issuer comes from the topology TLS configuration rather than
// from any one cluster's issuer.
func BuildServingCertificate(
	toposerver *multigresv1alpha1.TopoServer,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	if !toposerver.Spec.TLS.IsEnabled() {
		return nil, nil
	}

	// Verification is done against the SANs, so a common name truncated to
	// the X.509 limit costs nothing here. That is the opposite of the client
	// credential, whose common name is the identity being authorized.
	commonName := serviceFQDN(toposerver.Name, toposerver.Namespace)

	return certs.Build(toposerver, scheme, certs.Spec{
		Name:       multigresv1alpha1.TopoServerCertName(toposerver.Name),
		SecretName: multigresv1alpha1.TopoServerCertSecretName(toposerver.Name),
		CommonName: commonName,
		DNSNames:   servingDNSNames(toposerver.Name, toposerver.Namespace),
		// etcd peer connections are mutual, so each member presents this
		// certificate as a client to the members it dials as well.
		Usages: []any{
			"digital signature",
			"key encipherment",
			"server auth",
			"client auth",
		},
		IssuerName: toposerver.Spec.TLS.IssuerName,
	})
}

// BuildAdminCertificate constructs the operator's dedicated etcd client identity.
func BuildAdminCertificate(
	toposerver *multigresv1alpha1.TopoServer,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	if !toposerver.Spec.TLS.IsEnabled() {
		return nil, nil
	}
	return certs.Build(toposerver, scheme, certs.Spec{
		Name:       multigresv1alpha1.TopoServerAdminCertName(toposerver.Name),
		SecretName: multigresv1alpha1.TopoServerAdminCertSecretName(toposerver.Name),
		CommonName: operatorEtcdUsername,
		DNSNames:   []any{},
		Usages:     []any{"digital signature", "key encipherment", "client auth"},
		IssuerName: toposerver.Spec.TLS.IssuerName,
	})
}

// servingDNSNames returns the names etcd is reached by: the client Service and
// the headless peer Service, each in its short, namespaced and fully qualified
// form so in-cluster callers verify regardless of how they resolve the name.
func servingDNSNames(toposerverName, namespace string) []any {
	names := make([]any, 0, 8)
	for _, svc := range []string{toposerverName, toposerverName + "-headless"} {
		names = append(names,
			svc,
			svc+"."+namespace,
			svc+"."+namespace+".svc",
			serviceFQDN(svc, namespace),
		)
	}
	// Peers connect to individual pods through the headless Service, so each
	// member's own record has to verify too.
	names = append(names, "*."+toposerverName+"-headless."+namespace+".svc.cluster.local")
	return names
}

func serviceFQDN(name, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", name, namespace)
}

// reconcileCertificate ensures serving and administration credentials match the
// TopoServer spec, removing owned credentials when TLS is disabled.
func (r *TopoServerReconciler) reconcileCertificate(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	for _, certificate := range []struct {
		name  string
		build func(*multigresv1alpha1.TopoServer, *runtime.Scheme) (*unstructured.Unstructured, error)
	}{
		{multigresv1alpha1.TopoServerCertName(toposerver.Name), BuildServingCertificate},
		{multigresv1alpha1.TopoServerAdminCertName(toposerver.Name), BuildAdminCertificate},
	} {
		desired, err := certificate.build(toposerver, r.Scheme)
		if err != nil {
			return fmt.Errorf(
				"failed to build topology server cert-manager Certificate %q: %w",
				certificate.name,
				err,
			)
		}
		if err := r.reconcileOwnedCertificate(
			ctx,
			toposerver,
			certificate.name,
			desired,
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *TopoServerReconciler) reconcileOwnedCertificate(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
	certName string,
	desired *unstructured.Unstructured,
) error {
	existing, err := certs.Get(ctx, r.Client, toposerver.Namespace, certName)
	if err != nil {
		return err
	}
	if desired == nil {
		if existing == nil || !certs.OwnedBy(existing, toposerver.UID) {
			return nil
		}
		return certs.Delete(ctx, r.Client, existing)
	}

	// A same-named Certificate belonging to another resource is a collision:
	// leave it unmodified rather than adopting it through server-side apply.
	if existing != nil && !certs.OwnedBy(existing, toposerver.UID) {
		return fmt.Errorf(
			"cert-manager Certificate %q in namespace %q already exists and is not owned by this TopoServer",
			existing.GetName(),
			existing.GetNamespace(),
		)
	}

	if existing != nil && certs.SpecEqual(existing, desired) {
		return nil
	}
	return certs.ApplyOne(ctx, r.Client, desired)
}
