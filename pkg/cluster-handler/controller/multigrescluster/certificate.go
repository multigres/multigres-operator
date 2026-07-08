package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const (
	// CertIssuerName is the cert-manager ClusterIssuer used for TLS certificates.
	CertIssuerName = "supabase-issuer"

	// CertDuration is the certificate duration (5 years), matching non-HA projects.
	CertDuration = "44640h0m0s"

	// CertLiteralSubjectTemplate is the literal subject template for certificates.
	// The CN placeholder is replaced with the certCommonName.
	CertLiteralSubjectTemplate = "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=%s"
)

var certGVK = schema.GroupVersionKind{
	Group:   "cert-manager.io",
	Version: "v1",
	Kind:    "Certificate",
}

type certSpec struct {
	name       string
	secretName string
	commonName string
	dnsNames   []any
	usages     []any
}

// buildCertificate constructs an unstructured cert-manager Certificate for the
// multigateway TLS certificate. The Certificate spec matches what non-HA
// projects use, with the supabase-issuer ClusterIssuer.
// The owner is the MultigresCluster so there is exactly one reconciler
// and one ownerRef — no conflict when multiple cells share the same CN.
func buildCertificate(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	cn := cluster.Spec.CertCommonName

	dnsNames := publicGatewayDNSNames(cn)

	return buildCertificateFromSpec(cluster, scheme, certSpec{
		name:       cn,
		secretName: multigresv1alpha1.CertSecretName,
		commonName: cn,
		dnsNames:   dnsNames,
		usages: []any{
			"digital signature",
			"key encipherment",
			"server auth",
		},
	})
}

func publicGatewayDNSNames(commonName string) []any {
	dnsNames := []any{commonName}
	if after, ok := strings.CutPrefix(commonName, "db."); ok {
		dnsNames = append(dnsNames, after)
	}
	return dnsNames
}

func buildInternalCertificates(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) ([]*unstructured.Unstructured, error) {
	components := []string{
		multigresv1alpha1.ComponentMultiAdminTLS,
		multigresv1alpha1.ComponentMultiGatewayTLS,
		multigresv1alpha1.ComponentMultiOrchTLS,
		multigresv1alpha1.ComponentMultiPoolerTLS,
	}
	certs := make([]*unstructured.Unstructured, 0, len(components))
	for _, component := range components {
		cn := multigresv1alpha1.ComponentCertCommonName(
			component,
			cluster.Name,
			cluster.Namespace,
		)
		dnsNames := []any{cn}
		if component == multigresv1alpha1.ComponentMultiGatewayTLS {
			// multigateway's --multipooler-grpc-* client flags are reused by
			// multigres core for gateway-to-gateway cancel forwarding, so its
			// certificate also carries the logical pooler server identity.
			dnsNames = append(dnsNames, multigresv1alpha1.ComponentCertCommonName(
				multigresv1alpha1.ComponentMultiPoolerTLS,
				cluster.Name,
				cluster.Namespace,
			))
			// Keep the public gateway DNS identities as SANs while retaining
			// the internal logical identity as this certificate's CN and name.
			dnsNames = append(
				dnsNames,
				publicGatewayDNSNames(cluster.Spec.CertCommonName)...,
			)
		}
		cert, err := buildCertificateFromSpec(cluster, scheme, certSpec{
			name: cn,
			secretName: multigresv1alpha1.ComponentCertSecretName(
				component,
				cluster.Name,
				cluster.Namespace,
			),
			commonName: cn,
			dnsNames:   dnsNames,
			usages: []any{
				"digital signature",
				"key encipherment",
				"server auth",
				"client auth",
			},
		})
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	return certs, nil
}

func buildCertificateFromSpec(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
	spec certSpec,
) (*unstructured.Unstructured, error) {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certGVK)
	cert.SetName(spec.name)
	cert.SetNamespace(cluster.Namespace)

	if err := ctrl.SetControllerReference(cluster, cert, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	cert.Object["spec"] = map[string]any{
		"secretName":     spec.secretName,
		"dnsNames":       spec.dnsNames,
		"duration":       CertDuration,
		"literalSubject": fmt.Sprintf(CertLiteralSubjectTemplate, spec.commonName),
		"issuerRef": map[string]any{
			"name":  CertIssuerName,
			"kind":  "ClusterIssuer",
			"group": "cert-manager.io",
		},
		"privateKey": map[string]any{
			"algorithm": "RSA",
			"size":      int64(2048),
		},
		"usages": spec.usages,
	}

	return cert, nil
}

// reconcileCertificate ensures the cert-manager Certificate matches the
// cluster spec. When CertCommonName is set it creates or updates the
// Certificate. When CertCommonName is empty it deletes a previously-managed
// Certificate (identified by the generated-certs secret convention) so that
// disabling TLS is deterministic.
func (r *MultigresClusterReconciler) reconcileCertificate(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	certList, err := r.listCertificates(ctx, cluster)
	if err != nil {
		return err
	}

	if cluster.Spec.CertCommonName == "" {
		if err := r.deleteOwnedCertificates(ctx, cluster, certList, nil, nil); err != nil {
			return err
		}
		return nil
	}

	desired, err := buildCertificate(cluster, r.Scheme)
	if err != nil {
		return fmt.Errorf(
			"failed to build cert-manager Certificate: %w", err,
		)
	}
	desiredCerts := []*unstructured.Unstructured{desired}
	internalCerts, err := buildInternalCertificates(cluster, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build internal cert-manager Certificates: %w", err)
	}
	desiredCerts = append(desiredCerts, internalCerts...)

	keepNames := make(map[string]struct{}, len(desiredCerts))
	keepSecretNames := make(map[string]struct{}, len(desiredCerts))
	for _, cert := range desiredCerts {
		keepNames[cert.GetName()] = struct{}{}
		if secretName, _, _ := unstructured.NestedString(
			cert.Object, "spec", "secretName",
		); secretName != "" {
			keepSecretNames[secretName] = struct{}{}
		}
	}

	if err := r.deleteOwnedCertificates(
		ctx, cluster, certList, keepNames, keepSecretNames,
	); err != nil {
		return err
	}

	for _, cert := range desiredCerts {
		// Skip the Patch entirely when the live spec already matches the
		// desired spec, so a no-op reconcile doesn't churn resourceVersion.
		if existing := findCertificateByName(certList, cert.GetName()); existing != nil &&
			isOwnedBy(existing, cluster) &&
			apiequality.Semantic.DeepEqual(existing.Object["spec"], cert.Object["spec"]) {
			continue
		}
		if err := r.Patch(
			ctx,
			cert,
			client.Apply,
			client.ForceOwnership,
			client.FieldOwner("multigres-operator"),
		); err != nil {
			return fmt.Errorf(
				"failed to apply cert-manager Certificate %q: %w",
				cert.GetName(),
				err,
			)
		}
	}
	return nil
}

// listCertificates lists cert-manager Certificates owned by this cluster's
// namespace. When the cert-manager CRD is not installed, this returns an
// empty list rather than an error so callers can treat "no Certificates"
// uniformly.
func (r *MultigresClusterReconciler) listCertificates(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*unstructured.UnstructuredList, error) {
	certList := &unstructured.UnstructuredList{}
	certList.SetGroupVersionKind(certGVK)
	if err := r.List(
		ctx,
		certList,
		client.InNamespace(cluster.Namespace),
	); err != nil {
		if apierrors.IsNotFound(err) || isNoMatchError(err) {
			return certList, nil
		}
		return nil, fmt.Errorf(
			"failed to list cert-manager Certificates: %w", err,
		)
	}
	return certList, nil
}

// findCertificateByName returns the Certificate named name from certList, or
// nil if it is not present.
func findCertificateByName(
	certList *unstructured.UnstructuredList,
	name string,
) *unstructured.Unstructured {
	for i := range certList.Items {
		if certList.Items[i].GetName() == name {
			return &certList.Items[i]
		}
	}
	return nil
}

// deleteOwnedCertificates removes cert-manager Certificates owned by this
// cluster whose name is not in keepNames, along with each one's generated
// secret (unless its name is in keepSecretNames. Pass nil for both
// maps to delete all certificates and secrets owned by this cluster (used
// when TLS is disabled).
func (r *MultigresClusterReconciler) deleteOwnedCertificates(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	certList *unstructured.UnstructuredList,
	keepNames map[string]struct{},
	keepSecretNames map[string]struct{},
) error {
	logger := log.FromContext(ctx)

	for i := range certList.Items {
		cert := &certList.Items[i]
		if !isOwnedBy(cert, cluster) {
			continue
		}
		if keepNames != nil {
			if _, ok := keepNames[cert.GetName()]; ok {
				continue
			}
		}
		// Delete the generated secret first. If this operation fails, keeping
		// the certificate makes the Secret discoverable on the next reconcile
		// so cleanup can be retried instead of leaving private key material
		// permanently orphaned.
		secretName, _, _ := unstructured.NestedString(cert.Object, "spec", "secretName")
		_, keepSecret := keepSecretNames[secretName]
		if secretName != "" && !keepSecret {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: cluster.Namespace,
				},
			}
			if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf(
					"failed to delete generated TLS Secret %q: %w",
					secretName, err,
				)
			}
		}

		if err := r.Delete(ctx, cert); err != nil &&
			!apierrors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to delete cert-manager Certificate %q: %w",
				cert.GetName(), err,
			)
		}
		logger.Info(
			"Deleted stale TLS Certificate",
			"certificate", cert.GetName(),
		)
	}

	return nil
}

// isOwnedBy checks whether an unstructured object has an ownerReference
// pointing to the given cluster.
func isOwnedBy(
	obj *unstructured.Unstructured,
	cluster *multigresv1alpha1.MultigresCluster,
) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == cluster.UID {
			return true
		}
	}
	return false
}

// isNoMatchError returns true when the API server has no resource mapping
// for the requested GVK (e.g. cert-manager CRD not installed).
func isNoMatchError(err error) bool {
	noMatch := &apimeta.NoKindMatchError{}
	return errors.As(err, &noMatch)
}
