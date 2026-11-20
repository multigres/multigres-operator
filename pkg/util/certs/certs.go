// Package certs builds and reconciles the cert-manager Certificates the
// operator owns. Every controller that issues a Certificate goes through
// here so certificate conventions — issuer, duration, subject, private key —
// stay identical across the operator.
package certs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const (
	// DefaultIssuerName is the cert-manager ClusterIssuer used when a caller
	// does not name one.
	DefaultIssuerName = multigresv1alpha1.DefaultIssuerName

	// Duration is the certificate duration (5 years), matching non-HA projects.
	Duration = "44640h0m0s"

	// LiteralSubjectTemplate is the literal subject template for certificates.
	// The CN placeholder is replaced with the certificate's common name.
	LiteralSubjectTemplate = "C=US, ST=Delware, L=New Castle,O=Supabase Inc, CN=%s"

	// FieldOwner is the server-side apply field manager for owned Certificates.
	FieldOwner = "multigres-operator"

	// DefaultTrustDomain is the SPIFFE trust domain used in URI SANs when the
	// operator is not given one.
	DefaultTrustDomain = "multigres.local"

	// MaxCommonNameBytes is the X.509 upper bound for the CN attribute
	// (RFC 5280 ub-common-name). cert-manager's webhook rejects longer CNs.
	MaxCommonNameBytes = 64
)

// GVK is the cert-manager Certificate GroupVersionKind.
var GVK = schema.GroupVersionKind{
	Group:   "cert-manager.io",
	Version: "v1",
	Kind:    "Certificate",
}

// Spec describes one desired Certificate. IssuerName is optional and falls
// back to DefaultIssuerName.
type Spec struct {
	Name       string
	SecretName string
	CommonName string
	DNSNames   []any
	// URIs are the URI SANs. Omitted from the spec when empty.
	URIs       []any
	Usages     []any
	IssuerName string
}

// SPIFFEURI returns spiffe://<trustDomain>/<path>, falling back to
// DefaultTrustDomain when trustDomain is empty.
func SPIFFEURI(trustDomain, path string) string {
	if trustDomain == "" {
		trustDomain = DefaultTrustDomain
	}
	return "spiffe://" + trustDomain + "/" + path
}

// Build constructs an unstructured cert-manager Certificate in the owner's
// namespace, controlled by owner.
func Build(
	owner client.Object,
	scheme *runtime.Scheme,
	spec Spec,
) (*unstructured.Unstructured, error) {
	cert := BuildShared(owner.GetNamespace(), spec)
	if err := ctrl.SetControllerReference(owner, cert, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}
	return cert, nil
}

// BuildShared constructs an unstructured cert-manager Certificate in
// namespace with no owner reference, for a Certificate shared by resources in
// other namespaces. A namespaced owner cannot own an object elsewhere, so the
// caller is responsible for its lifecycle.
func BuildShared(namespace string, spec Spec) *unstructured.Unstructured {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(GVK)
	cert.SetName(spec.Name)
	cert.SetNamespace(namespace)

	issuer := spec.IssuerName
	if issuer == "" {
		issuer = DefaultIssuerName
	}

	certSpec := map[string]any{
		"secretName": spec.SecretName,
		"dnsNames":   spec.DNSNames,
		"duration":   Duration,
		"literalSubject": fmt.Sprintf(
			LiteralSubjectTemplate,
			TruncateCommonName(spec.CommonName),
		),
		"issuerRef": map[string]any{
			"name":  issuer,
			"kind":  "ClusterIssuer",
			"group": "cert-manager.io",
		},
		"privateKey": map[string]any{
			"algorithm": "RSA",
			"size":      int64(2048),
		},
		"usages": spec.Usages,
	}
	if len(spec.URIs) != 0 {
		certSpec["uris"] = spec.URIs
	}
	cert.Object["spec"] = certSpec
	return cert
}

// TruncateCommonName returns cn unchanged when it fits the X.509 CN limit.
// Longer values are truncated and given a deterministic hash suffix so the
// result stays unique per identity and stable across reconciles.
func TruncateCommonName(cn string) string {
	if len(cn) <= MaxCommonNameBytes {
		return cn
	}
	sum := sha256.Sum256([]byte(cn))
	suffix := "-" + hex.EncodeToString(sum[:4])
	return cn[:MaxCommonNameBytes-len(suffix)] + suffix
}

// List lists cert-manager Certificates in a namespace. When the cert-manager
// CRD is not installed this returns an empty list rather than an error, so
// callers can treat "no Certificates" uniformly.
func List(
	ctx context.Context,
	c client.Reader,
	namespace string,
) (*unstructured.UnstructuredList, error) {
	certList := &unstructured.UnstructuredList{}
	certList.SetGroupVersionKind(GVK)
	if err := c.List(ctx, certList, client.InNamespace(namespace)); err != nil {
		if apierrors.IsNotFound(err) || IsNoMatchError(err) {
			return certList, nil
		}
		return nil, fmt.Errorf("failed to list cert-manager Certificates: %w", err)
	}
	return certList, nil
}

// Get returns the Certificate named name. It returns nil without an error
// when the Certificate is absent, and likewise when the cert-manager CRD is
// not installed, so callers holding a single deterministically named
// Certificate never need to list a whole namespace.
func Get(
	ctx context.Context,
	c client.Reader,
	namespace, name string,
) (*unstructured.Unstructured, error) {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(GVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cert); err != nil {
		if apierrors.IsNotFound(err) || IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get cert-manager Certificate %q: %w", name, err)
	}
	return cert, nil
}

// Delete removes cert and the Secret cert-manager generated for it. The Secret
// goes first: if that fails, leaving the Certificate behind keeps the Secret
// discoverable on the next reconcile so cleanup can be retried instead of
// leaving private key material permanently orphaned.
func Delete(ctx context.Context, c client.Client, cert *unstructured.Unstructured) error {
	secretName, _, _ := unstructured.NestedString(cert.Object, "spec", "secretName")
	if secretName != "" {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: cert.GetNamespace(),
			},
		}
		if err := c.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to delete generated TLS Secret %q: %w", secretName, err,
			)
		}
	}
	if err := c.Delete(ctx, cert); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf(
			"failed to delete cert-manager Certificate %q: %w", cert.GetName(), err,
		)
	}
	log.FromContext(ctx).Info("Deleted stale TLS Certificate", "certificate", cert.GetName())
	return nil
}

// SpecEqual reports whether a live Certificate already matches the desired
// one, so an unchanged reconcile does not churn resourceVersion.
func SpecEqual(existing, desired *unstructured.Unstructured) bool {
	return apiequality.Semantic.DeepEqual(existing.Object["spec"], desired.Object["spec"])
}

// FindByName returns the Certificate named name from certList, or nil if it is
// not present.
func FindByName(
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

// OwnedBy reports whether obj has an ownerReference with the given UID.
func OwnedBy(obj *unstructured.Unstructured, ownerUID types.UID) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == ownerUID {
			return true
		}
	}
	return false
}

// KeepSets returns the certificate names and secret names implied by desired,
// for use as the keep sets passed to Prune.
func KeepSets(
	desired []*unstructured.Unstructured,
) (keepNames, keepSecretNames map[string]struct{}) {
	keepNames = make(map[string]struct{}, len(desired))
	keepSecretNames = make(map[string]struct{}, len(desired))
	for _, cert := range desired {
		keepNames[cert.GetName()] = struct{}{}
		if secretName, _, _ := unstructured.NestedString(
			cert.Object, "spec", "secretName",
		); secretName != "" {
			keepSecretNames[secretName] = struct{}{}
		}
	}
	return keepNames, keepSecretNames
}

// Prune removes Certificates owned by ownerUID whose name is not in keepNames,
// along with each one's generated secret unless its name is in keepSecretNames.
// With empty maps every owned certificate and secret is deleted, which is how
// callers disable a feature deterministically.
func Prune(
	ctx context.Context,
	c client.Client,
	namespace string,
	ownerUID types.UID,
	certList *unstructured.UnstructuredList,
	keepNames map[string]struct{},
	keepSecretNames map[string]struct{},
) error {
	logger := log.FromContext(ctx)

	for i := range certList.Items {
		cert := &certList.Items[i]
		if !OwnedBy(cert, ownerUID) {
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
					Namespace: namespace,
				},
			}
			if err := c.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf(
					"failed to delete generated TLS Secret %q: %w",
					secretName, err,
				)
			}
		}

		if err := c.Delete(ctx, cert); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to delete cert-manager Certificate %q: %w",
				cert.GetName(), err,
			)
		}
		logger.Info("Deleted stale TLS Certificate", "certificate", cert.GetName())
	}

	return nil
}

// ApplyOne server-side applies a single Certificate.
func ApplyOne(ctx context.Context, c client.Client, cert *unstructured.Unstructured) error {
	if err := c.Patch(
		ctx,
		cert,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(FieldOwner),
	); err != nil {
		return fmt.Errorf(
			"failed to apply cert-manager Certificate %q: %w", cert.GetName(), err,
		)
	}
	return nil
}

// Apply server-side applies each desired Certificate. A same-named Certificate
// owned by another resource produces an error rather than being adopted. A
// Certificate whose live spec already matches is skipped so a no-op reconcile
// does not churn resourceVersion.
func Apply(
	ctx context.Context,
	c client.Client,
	certList *unstructured.UnstructuredList,
	ownerUID types.UID,
	desired []*unstructured.Unstructured,
) error {
	for _, cert := range desired {
		if existing := FindByName(certList, cert.GetName()); existing != nil {
			if !OwnedBy(existing, ownerUID) {
				return fmt.Errorf(
					"cert-manager Certificate %q in namespace %q already exists and is not owned by the reconciled resource",
					existing.GetName(),
					existing.GetNamespace(),
				)
			}
			if SpecEqual(existing, cert) {
				continue
			}
		}
		if err := ApplyOne(ctx, c, cert); err != nil {
			return err
		}
	}
	return nil
}

// IsNoMatchError returns true when the API server has no resource mapping for
// the requested GVK (e.g. cert-manager CRD not installed).
func IsNoMatchError(err error) bool {
	noMatch := &apimeta.NoKindMatchError{}
	return errors.As(err, &noMatch)
}
