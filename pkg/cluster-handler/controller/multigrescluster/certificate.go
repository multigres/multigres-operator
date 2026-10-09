package multigrescluster

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
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
	built := make([]*unstructured.Unstructured, 0, len(components)+1)
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

	// The operator gets a dedicated per-cluster client identity. Keeping it
	// cluster-owned gives it the same issuer and lifecycle as the servers it
	// calls, while allowing the client to verify the exact multipooler SAN.
	operatorName := multigresv1alpha1.ComponentCertCommonName(
		multigresv1alpha1.ComponentOperatorTLS,
		cluster.Name,
		cluster.Namespace,
	)
	operatorCert, err := buildCertificateFromSpec(cluster, scheme, certSpec{
		name: operatorName,
		secretName: multigresv1alpha1.ComponentCertSecretName(
			multigresv1alpha1.ComponentOperatorTLS,
			cluster.Name,
			cluster.Namespace,
		),
		// Keep the client identity cluster-specific so subject authorization can
		// be enabled later. certs.Build safely hash-truncates long common names.
		commonName: operatorName,
		dnsNames:   []any{},
		usages: []any{
			"digital signature",
			"key encipherment",
			"client auth",
		},
	})
	if err != nil {
		return nil, err
	}
	built = append(built, operatorCert)
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
	roots, err := topology.ForCluster(cluster)
	if err != nil {
		return nil, fmt.Errorf("deriving topology root for client certificate: %w", err)
	}

	commonName := roots.ClusterRoot()
	if len(commonName) > certs.MaxCommonNameBytes {
		return nil, fmt.Errorf(
			"topology root %q is %d bytes, over the %d byte certificate common name limit; shorten annotation %s to at most %d bytes after path escaping",
			commonName,
			len(commonName),
			certs.MaxCommonNameBytes,
			metadata.AnnotationProjectRef,
			certs.MaxCommonNameBytes-len("/multigres/"),
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

// issuesTopoClientCert reports whether the operator issues the topology
// client certificate and checks configured roots against the shortened
// certificate prefix.
func (r *MultigresClusterReconciler) issuesTopoClientCert(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (bool, error) {
	if !multigresv1alpha1.OperatorIssuesTopoClientCert(cluster) {
		return false, nil
	}
	res := resolver.NewResolver(r.Client, cluster.Namespace)
	spec, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		return false, fmt.Errorf(
			"failed to resolve global topology for client certificate: %w", err,
		)
	}
	var globalRoot string
	if spec.Etcd != nil {
		globalRoot = spec.Etcd.RootPath
	} else if spec.External != nil {
		globalRoot = spec.External.RootPath
	}
	roots, err := topology.ForCluster(cluster)
	if err != nil {
		return false, fmt.Errorf("deriving topology certificate identity: %w", err)
	}
	unbounded, err := topology.NewRoots(
		cluster.Annotations,
		cluster.Namespace,
		cluster.Name,
		false,
	)
	if err != nil {
		return false, fmt.Errorf("deriving unbounded topology root: %w", err)
	}
	// Changing a saved root would abandon its existing keys.
	// Reject roots outside the certificate prefix.
	if roots == unbounded {
		return true, nil
	}
	if !strings.HasPrefix(globalRoot, roots.KeyPrefix()) {
		return false, fmt.Errorf(
			"configured topology root %q is outside certificate identity %q; set the global topology root to %q and migrate any existing topology data",
			globalRoot,
			roots.ClusterRoot(),
			roots.Global(),
		)
	}
	for _, cell := range cluster.Spec.Cells {
		cell.CellTemplate = cluster.Spec.EffectiveCellTemplate(cell.CellTemplate)
		_, _, local, err := res.ResolveCell(ctx, cluster, &cell)
		if err != nil {
			return false, fmt.Errorf(
				"resolving cell %q topology for certificate: %w",
				cell.Name,
				err,
			)
		}
		if local == nil {
			continue
		}
		var localRoot string
		if local.Etcd != nil {
			localRoot = local.Etcd.RootPath
		} else if local.External != nil {
			localRoot = local.External.RootPath
		}
		if localRoot != "" && !strings.HasPrefix(localRoot, roots.KeyPrefix()) {
			return false, fmt.Errorf(
				"cell %q topology root %q is outside certificate identity %q; align the cell topology root and migrate any existing topology data",
				cell.Name,
				localRoot,
				roots.ClusterRoot(),
			)
		}
	}
	return true, nil
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

	desiredCerts := make([]*unstructured.Unstructured, 0, 7)
	if cluster.Spec.InternalTLS.IsEnabled() {
		internalCerts, err := buildInternalCertificates(cluster, r.Scheme)
		if err != nil {
			return fmt.Errorf("failed to build internal cert-manager Certificates: %w", err)
		}
		desiredCerts = append(desiredCerts, internalCerts...)
	}
	if cluster.Spec.TopoTLS.IsEnabled() {
		issue, err := r.issuesTopoClientCert(ctx, cluster)
		if err != nil {
			r.markTopologyFailed(ctx, cluster, "TopoCertificateFailed", err, log.FromContext(ctx))
			return err
		}
		// The credential chains to the topology issuer, which also signs the
		// serving certificate of a managed topology server and of a shared one
		// named as external. An external server that names its own CA or client
		// Secret brings its own trust, so a credential issued here would sit
		// unused.
		if issue {
			topoClientCert, err := buildTopoClientCertificate(cluster, r.Scheme)
			if err != nil {
				r.markTopologyFailed(
					ctx,
					cluster,
					"TopoCertificateFailed",
					err,
					log.FromContext(ctx),
				)
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
