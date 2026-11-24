package topology

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// AuthManaged reports whether authorization was requested or has persisted
// controller state that must continue to be reconciled.
func AuthManaged(ts *multigresv1alpha1.TopoServer) bool {
	return ts.Spec.Auth.IsEnabled() || ts.Status.AuthEnabled || len(ts.Status.AuthIdentities) != 0
}

// AdminIssuerKind is the kind of every topology issuer. The operator issues
// topology credentials only from ClusterIssuers.
const AdminIssuerKind = "ClusterIssuer"

// AdminIssuerName returns the issuer of the administrator credential for ts,
// which is the issuer of its serving certificate.
func AdminIssuerName(ts *multigresv1alpha1.TopoServer) string {
	if ts.Spec.TLS.IssuerName != "" {
		return ts.Spec.TLS.IssuerName
	}
	return multigresv1alpha1.DefaultIssuerName
}

// AdminSecretKey returns where the operator's administrator credential for
// ts lives. It is one Secret per issuer in the operator's namespace, shared by
// every managed TopoServer that uses the issuer, so it is never readable from
// a project namespace.
func AdminSecretKey(operatorNamespace string, ts *multigresv1alpha1.TopoServer) client.ObjectKey {
	return client.ObjectKey{
		Namespace: operatorNamespace,
		Name:      multigresv1alpha1.TopoAdminCertSecretName(AdminIssuerKind, AdminIssuerName(ts)),
	}
}

// ManagedProbeTLSSecret returns the credential the operator uses to inspect a
// managed TopoServer. A server on which authorization is not managed has no
// administrator credential, so the operator uses its serving credential.
func ManagedProbeTLSSecret(
	ctx context.Context,
	reader client.Reader,
	operatorNamespace string,
	ts *multigresv1alpha1.TopoServer,
) (*corev1.Secret, error) {
	if AuthManaged(ts) {
		if operatorNamespace == "" {
			return nil, fmt.Errorf("operator namespace is not configured")
		}
		adminKey := AdminSecretKey(operatorNamespace, ts)
		secret := &corev1.Secret{}
		if err := reader.Get(ctx, adminKey, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf(
					"managed topology administrator credential %s/%s is not available after authorization was managed",
					adminKey.Namespace,
					adminKey.Name,
				)
			}
			return nil, fmt.Errorf("reading managed topology administrator credential: %w", err)
		}
		return secret, nil
	}

	servingKey := client.ObjectKey{
		Namespace: ts.Namespace,
		Name:      multigresv1alpha1.TopoServerCertSecretName(ts.Name),
	}
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, servingKey, secret); err != nil {
		return nil, fmt.Errorf("reading managed topology serving credential: %w", err)
	}
	return secret, nil
}
