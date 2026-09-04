package multigrescluster

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

const (
	// CertIssuerName is the cert-manager ClusterIssuer used for TLS
	// certificates when the cluster does not name one.
	CertIssuerName = certs.DefaultIssuerName

	// CertDuration is the certificate duration (5 years), matching non-HA projects.
	CertDuration = certs.Duration

	// CertLiteralSubjectTemplate is the literal subject template for certificates.
	// The CN placeholder is replaced with the certCommonName.
	CertLiteralSubjectTemplate = certs.LiteralSubjectTemplate
)

var certGVK = certs.GVK

// maxCommonNameBytes is the X.509 upper bound for the CN attribute
// (RFC 5280 ub-common-name). cert-manager's webhook rejects longer CNs.
const maxCommonNameBytes = certs.MaxCommonNameBytes

type certSpec struct {
	name       string
	secretName string
	commonName string
	dnsNames   []any
	usages     []any
}

// buildCertificate constructs an unstructured cert-manager Certificate for the
// multigateway TLS certificate. The Certificate spec matches what non-HA
// projects use, with the cluster's configured ClusterIssuer (defaulting to
// supabase-issuer).
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
	built := make([]*unstructured.Unstructured, 0, len(components))
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
		built = append(built, cert)
	}
	return built, nil
}

// buildTopoClientCertificate constructs the client credential this cluster
// presents to the topology server.
//
// The CN is the cluster's topology root, so the identity the cluster proves
// and the key prefix it is entitled to are the same string and cannot drift
// apart. A root longer than the X.509 CN limit is rejected rather than
// truncated, because a truncated identity no longer names the prefix it is
// meant to authorize.
//
// The issuer is the topology CA, not the cluster's own issuer: the credential
// is only useful if it chains to a CA the topology server trusts.
func buildTopoClientCertificate(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) (*unstructured.Unstructured, error) {
	roots, err := topology.NewRoots(cluster.Annotations, cluster.Namespace, cluster.Name)
	if err != nil {
		return nil, fmt.Errorf("deriving topology root for client certificate: %w", err)
	}

	commonName := roots.ClusterRoot()
	if len(commonName) > certs.MaxCommonNameBytes {
		return nil, fmt.Errorf(
			"topology root %q is %d bytes, over the %d byte certificate common name limit",
			commonName, len(commonName), certs.MaxCommonNameBytes,
		)
	}

	return certs.Build(cluster, scheme, certs.Spec{
		Name:       multigresv1alpha1.TopoClientCertName(cluster.Name),
		SecretName: multigresv1alpha1.TopoClientCertSecretName(cluster.Name),
		CommonName: commonName,
		// A client credential is verified by its subject, not by name, so it
		// carries no SANs.
		DNSNames: []any{},
		Usages: []any{
			"digital signature",
			"key encipherment",
			"client auth",
		},
		IssuerName: cluster.Spec.TopoTLS.IssuerName,
	})
}

// topologyIsManaged reports whether the cluster's resolved global topology is a
// managed etcd server the operator runs and issues a serving certificate for,
// as opposed to an external topology server the user operates.
func (r *MultigresClusterReconciler) topologyIsManaged(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (bool, error) {
	res := resolver.NewResolver(r.Client, cluster.Namespace)
	spec, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		return false, fmt.Errorf(
			"failed to resolve global topology for client certificate: %w", err,
		)
	}
	return spec.Etcd != nil, nil
}

// issuerName returns the cluster's configured cert-manager ClusterIssuer
// name, defaulting to CertIssuerName when unset.
func issuerName(cluster *multigresv1alpha1.MultigresCluster) string {
	if cluster.Spec.IssuerName != "" {
		return cluster.Spec.IssuerName
	}
	return CertIssuerName
}

// truncateCommonName returns cn unchanged when it fits the X.509 CN limit.
// Longer values are truncated and given a deterministic hash suffix so the
// result stays unique per identity and stable across reconciles.
func truncateCommonName(cn string) string {
	return certs.TruncateCommonName(cn)
}

func buildCertificateFromSpec(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
	spec certSpec,
) (*unstructured.Unstructured, error) {
	return certs.Build(cluster, scheme, certs.Spec{
		Name:       spec.name,
		SecretName: spec.secretName,
		CommonName: spec.commonName,
		DNSNames:   spec.dnsNames,
		Usages:     spec.usages,
		IssuerName: issuerName(cluster),
	})
}

// reconcileCertificate ensures the cert-manager Certificates match the
// cluster spec: the internal component certificates when internal mTLS is
// enabled, the topology client credential when topology TLS is enabled, and
// the public multigateway certificate when CertCommonName is set.
// Certificates no longer desired are deleted along with their generated
// secrets, so disabling any of these is deterministic.
func (r *MultigresClusterReconciler) reconcileCertificate(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	certList, err := r.listCertificates(ctx, cluster)
	if err != nil {
		return err
	}

	desiredCerts := make([]*unstructured.Unstructured, 0, 6)
	if cluster.Spec.InternalTLS.IsEnabled() {
		internalCerts, err := buildInternalCertificates(cluster, r.Scheme)
		if err != nil {
			return fmt.Errorf("failed to build internal cert-manager Certificates: %w", err)
		}
		desiredCerts = append(desiredCerts, internalCerts...)
	}
	if cluster.Spec.TopoTLS.IsEnabled() {
		managed, err := r.topologyIsManaged(ctx, cluster)
		if err != nil {
			return err
		}
		// An external topology server brings its own CA and client Secrets, so a
		// credential issued from the cluster's own issuer would never be trusted
		// and would sit unused. Only a managed etcd topology, whose serving
		// certificate the operator issues from the same CA, gets a client
		// credential.
		if managed {
			topoClientCert, err := buildTopoClientCertificate(cluster, r.Scheme)
			if err != nil {
				return fmt.Errorf(
					"failed to build topology client cert-manager Certificate: %w", err,
				)
			}
			desiredCerts = append(desiredCerts, topoClientCert)
		}
	}
	if cluster.Spec.CertCommonName != "" {
		publicCert, err := buildCertificate(cluster, r.Scheme)
		if err != nil {
			return fmt.Errorf(
				"failed to build public cert-manager Certificate: %w", err,
			)
		}
		desiredCerts = append(desiredCerts, publicCert)
	}

	keepNames, keepSecretNames := certs.KeepSets(desiredCerts)

	if err := r.deleteOwnedCertificates(
		ctx, cluster, certList, keepNames, keepSecretNames,
	); err != nil {
		return err
	}

	return certs.Apply(ctx, r.Client, certList, cluster.UID, desiredCerts)
}

// listCertificates lists cert-manager Certificates owned by this cluster's
// namespace. When the cert-manager CRD is not installed, this returns an
// empty list rather than an error so callers can treat "no Certificates"
// uniformly.
func (r *MultigresClusterReconciler) listCertificates(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*unstructured.UnstructuredList, error) {
	return certs.List(ctx, r.Client, cluster.Namespace)
}

// deleteOwnedCertificates removes cert-manager Certificates owned by this
// cluster whose name is not in keepNames, along with each one's generated
// secret (unless its name is in keepSecretNames). With empty maps every
// owned certificate and secret is deleted (used when TLS is disabled).
func (r *MultigresClusterReconciler) deleteOwnedCertificates(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	certList *unstructured.UnstructuredList,
	keepNames map[string]struct{},
	keepSecretNames map[string]struct{},
) error {
	return certs.Prune(
		ctx, r.Client, cluster.Namespace, cluster.UID,
		certList, keepNames, keepSecretNames,
	)
}
