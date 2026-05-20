package webhook

import (
	"context"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// CASecretName is the name of the Secret that stores the CA certificate and key.
	CASecretName = "multigres-operator-ca-secret" //nolint:gosec // K8s resource name, not a credential

	// ServerSecretName is the name of the Secret that stores the webhook server certificate and key.
	ServerSecretName = "multigres-webhook-certs" //nolint:gosec // K8s resource name, not a credential

	// MutatingWebhookName is the name of the MutatingWebhookConfiguration resource.
	MutatingWebhookName = "multigres-operator-mutating-webhook-configuration"

	// ValidatingWebhookName is the name of the ValidatingWebhookConfiguration resource.
	ValidatingWebhookName = "multigres-operator-validating-webhook-configuration"

	// CertStrategyAnnotation marks how the webhook TLS certificates are managed.
	// The operator sets this to CertStrategySelfSigned when it manages its own PKI.
	CertStrategyAnnotation = "multigres.com/cert-strategy"

	// CertStrategySelfSigned indicates the operator manages its own CA and server certs.
	CertStrategySelfSigned = "self-signed"

	// certFieldOwner is the SSA field manager for caBundle and cert-strategy annotation.
	certFieldOwner = "multigres-operator-cert"
)

// PatchWebhookCABundle injects the CA bundle and cert-strategy annotation into
// both the Mutating and Validating webhook configurations using Server-Side Apply.
// Using SSA with a dedicated field owner ensures that user-side SSA upgrades
// (e.g. kubectl apply --server-side -f install.yaml) do not wipe caBundle,
// because different field managers own different fields.
func PatchWebhookCABundle(ctx context.Context, c client.Client, caBundle []byte) error {
	if err := patchMutatingWebhook(ctx, c, caBundle); err != nil {
		return err
	}
	return patchValidatingWebhook(ctx, c, caBundle)
}

func patchMutatingWebhook(ctx context.Context, c client.Client, caBundle []byte) error {
	existing := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := c.Get(ctx, types.NamespacedName{Name: MutatingWebhookName}, existing); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get mutating webhook config: %w", err)
	}
	if len(existing.Webhooks) == 0 {
		return nil
	}

	patch := &admissionregistrationv1.MutatingWebhookConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admissionregistration.k8s.io/v1",
			Kind:       "MutatingWebhookConfiguration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: MutatingWebhookName,
			Annotations: map[string]string{
				CertStrategyAnnotation: CertStrategySelfSigned,
			},
		},
		Webhooks: make([]admissionregistrationv1.MutatingWebhook, len(existing.Webhooks)),
	}
	for i, wh := range existing.Webhooks {
		patch.Webhooks[i] = admissionregistrationv1.MutatingWebhook{
			Name:                    wh.Name,
			AdmissionReviewVersions: wh.AdmissionReviewVersions,
			SideEffects:             wh.SideEffects,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				CABundle: caBundle,
				Service:  wh.ClientConfig.Service,
			},
		}
	}

	return c.Patch(
		ctx,
		patch,
		client.Apply,
		client.FieldOwner(certFieldOwner),
		client.ForceOwnership,
	)
}

func patchValidatingWebhook(ctx context.Context, c client.Client, caBundle []byte) error {
	existing := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := c.Get(ctx, types.NamespacedName{Name: ValidatingWebhookName}, existing); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get validating webhook config: %w", err)
	}
	if len(existing.Webhooks) == 0 {
		return nil
	}

	patch := &admissionregistrationv1.ValidatingWebhookConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admissionregistration.k8s.io/v1",
			Kind:       "ValidatingWebhookConfiguration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ValidatingWebhookName,
			Annotations: map[string]string{
				CertStrategyAnnotation: CertStrategySelfSigned,
			},
		},
		Webhooks: make([]admissionregistrationv1.ValidatingWebhook, len(existing.Webhooks)),
	}
	for i, wh := range existing.Webhooks {
		patch.Webhooks[i] = admissionregistrationv1.ValidatingWebhook{
			Name:                    wh.Name,
			AdmissionReviewVersions: wh.AdmissionReviewVersions,
			SideEffects:             wh.SideEffects,
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				CABundle: caBundle,
				Service:  wh.ClientConfig.Service,
			},
		}
	}

	return c.Patch(
		ctx,
		patch,
		client.Apply,
		client.FieldOwner(certFieldOwner),
		client.ForceOwnership,
	)
}

// HasCertAnnotation returns true if either webhook configuration carries the
// cert-strategy annotation set by the operator. This is used during startup
// to detect that the operator previously managed its own certs, even when
// cert files exist on disk from surviving projected volumes.
func HasCertAnnotation(ctx context.Context, c client.Client) bool {
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := c.Get(ctx, types.NamespacedName{Name: MutatingWebhookName}, mutating); err == nil {
		if mutating.Annotations[CertStrategyAnnotation] == CertStrategySelfSigned {
			return true
		}
	}

	validating := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := c.Get(
		ctx,
		types.NamespacedName{Name: ValidatingWebhookName},
		validating,
	); err == nil {
		if validating.Annotations[CertStrategyAnnotation] == CertStrategySelfSigned {
			return true
		}
	}

	return false
}

// FindOperatorDeployment locates the operator's own Deployment for use as an owner
// reference on cert secrets. It first tries a label selector match, then falls back
// to an explicit name lookup. Returns (nil, nil) when no deployment is found,
// meaning secrets will be created without an owner reference.
func FindOperatorDeployment(
	ctx context.Context,
	c client.Client,
	namespace string,
	labels map[string]string,
	name string,
) (*appsv1.Deployment, error) {
	// 1. Try by label selector
	if len(labels) > 0 {
		list := &appsv1.DeploymentList{}
		if err := c.List(
			ctx,
			list,
			client.InNamespace(namespace),
			client.MatchingLabels(labels),
		); err != nil {
			return nil, fmt.Errorf("failed to list deployments by labels: %w", err)
		}
		if len(list.Items) > 0 {
			// Developer tools such as DevSpace may temporarily scale down the
			// deployed operator Deployment and create a dev Deployment with
			// similar labels, in which case the oldest Deployment should be
			// used.
			oldest := &list.Items[0]
			for i := 1; i < len(list.Items); i++ {
				if list.Items[i].CreationTimestamp.Before(&oldest.CreationTimestamp) {
					oldest = &list.Items[i]
				}
			}
			return oldest, nil
		}
	}

	// 2. Try by explicit name
	if name != "" {
		dep := &appsv1.Deployment{}
		if err := c.Get(
			ctx,
			types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			},
			dep,
		); err != nil {
			if errors.IsNotFound(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to get operator deployment by name: %w", err)
		}
		return dep, nil
	}

	return nil, nil
}
