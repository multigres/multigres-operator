package webhook

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/multigres/testkit/assert"
)

func pkiScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	c := assert.NewAborting(tb)
	s := runtime.NewScheme()
	c.NoError(admissionregistrationv1.AddToScheme(s))
	c.NoError(appsv1.AddToScheme(s))
	return s
}

func sideEffectNone() *admissionregistrationv1.SideEffectClass {
	return ptr.To(admissionregistrationv1.SideEffectClassNone)
}

func TestPatchWebhookCABundle(t *testing.T) {
	t.Parallel()

	caBundle := []byte("test-ca-bundle")

	t.Run("Patches Both Webhook Configs via SSA", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: MutatingWebhookName},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				{
					Name:                    "wh1.example.com",
					ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
					AdmissionReviewVersions: []string{"v1"},
					SideEffects:             sideEffectNone(),
				},
			},
		}
		validating := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: ValidatingWebhookName},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{
				{
					Name:                    "wh2.example.com",
					ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
					AdmissionReviewVersions: []string{"v1"},
					SideEffects:             sideEffectNone(),
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithObjects(mutating, validating).
			Build()

		c.Require().
			NoError(PatchWebhookCABundle(context.Background(), cl, caBundle), "unexpected error")

		// Verify mutating: caBundle + annotation
		got := &admissionregistrationv1.MutatingWebhookConfiguration{}
		c.Require().NoError(cl.Get(
			context.Background(),
			client.ObjectKeyFromObject(mutating),
			got,
		))
		c.Eq(
			string(caBundle),
			string(got.Webhooks[0].ClientConfig.CABundle),
			"mutating CABundle = %q, want %q",
			got.Webhooks[0].ClientConfig.CABundle,
			caBundle,
		)
		c.Eq(CertStrategySelfSigned, got.Annotations[CertStrategyAnnotation], "mutating annotation")

		// Verify validating: caBundle + annotation
		gotV := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		c.Require().NoError(cl.Get(
			context.Background(),
			client.ObjectKeyFromObject(validating),
			gotV,
		))
		c.Eq(
			string(caBundle),
			string(gotV.Webhooks[0].ClientConfig.CABundle),
			"validating CABundle = %q, want %q",
			gotV.Webhooks[0].ClientConfig.CABundle,
			caBundle,
		)
		c.Eq(
			CertStrategySelfSigned,
			gotV.Annotations[CertStrategyAnnotation],
			"validating annotation",
		)
	})

	t.Run("Tolerates NotFound", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()

		assert.NewAborting(t).
			NoError(PatchWebhookCABundle(context.Background(), cl, caBundle), "expected no error for missing configs, got")
	})

	t.Run("Error: Mutating Get Failure", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*admissionregistrationv1.MutatingWebhookConfiguration); ok {
						return errors.New("network error")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}).
			Build()

		err := PatchWebhookCABundle(context.Background(), cl, caBundle)
		assert.NewCollecting(t).
			False(err == nil || !strings.Contains(err.Error(), "failed to get mutating webhook config"), "expected mutating get error, got: %v", err)
	})

	t.Run("Error: Mutating Patch Failure", func(t *testing.T) {
		t.Parallel()

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: MutatingWebhookName},
			Webhooks: []admissionregistrationv1.MutatingWebhook{
				{
					Name:                    "wh.example.com",
					ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
					AdmissionReviewVersions: []string{"v1"},
					SideEffects:             sideEffectNone(),
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithObjects(mutating).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*admissionregistrationv1.MutatingWebhookConfiguration); ok {
						return errors.New("patch fail")
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			}).
			Build()

		err := PatchWebhookCABundle(context.Background(), cl, caBundle)
		assert.NewCollecting(t).
			False(err == nil || !strings.Contains(err.Error(), "patch fail"), "expected patch error, got: %v", err)
	})

	t.Run("Error: Validating Get Failure", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
						return errors.New("network error")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}).
			Build()

		err := PatchWebhookCABundle(context.Background(), cl, caBundle)
		assert.NewCollecting(t).
			False(err == nil || !strings.Contains(err.Error(), "failed to get validating webhook config"), "expected validating get error, got: %v", err)
	})

	t.Run("Error: Validating Patch Failure", func(t *testing.T) {
		t.Parallel()

		validating := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: ValidatingWebhookName},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{
				{
					Name:                    "wh.example.com",
					ClientConfig:            admissionregistrationv1.WebhookClientConfig{},
					AdmissionReviewVersions: []string{"v1"},
					SideEffects:             sideEffectNone(),
				},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithObjects(validating).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
						return errors.New("patch fail")
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			}).
			Build()

		err := PatchWebhookCABundle(context.Background(), cl, caBundle)
		assert.NewCollecting(t).
			False(err == nil || !strings.Contains(err.Error(), "patch fail"), "expected patch error, got: %v", err)
	})

	t.Run("Skips Patching When No Webhooks", func(t *testing.T) {
		t.Parallel()

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: MutatingWebhookName},
			Webhooks:   []admissionregistrationv1.MutatingWebhook{},
		}
		validating := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: ValidatingWebhookName},
			Webhooks:   []admissionregistrationv1.ValidatingWebhook{},
		}

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithObjects(mutating, validating).
			Build()

		assert.NewAborting(t).
			NoError(PatchWebhookCABundle(context.Background(), cl, caBundle), "unexpected error")
	})
}

func TestHasCertAnnotation(t *testing.T) {
	t.Parallel()

	t.Run("True When Mutating Has Annotation", func(t *testing.T) {
		t.Parallel()

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name:        MutatingWebhookName,
				Annotations: map[string]string{CertStrategyAnnotation: CertStrategySelfSigned},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(mutating).Build()
		assert.NewCollecting(t).
			True(HasCertAnnotation(context.Background(), cl), "expected true when mutating has annotation")
	})

	t.Run("True When Validating Has Annotation", func(t *testing.T) {
		t.Parallel()

		validating := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name:        ValidatingWebhookName,
				Annotations: map[string]string{CertStrategyAnnotation: CertStrategySelfSigned},
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(validating).Build()
		assert.NewCollecting(t).
			True(HasCertAnnotation(context.Background(), cl), "expected true when validating has annotation")
	})

	t.Run("False When No Annotation", func(t *testing.T) {
		t.Parallel()

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: MutatingWebhookName},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(mutating).Build()
		assert.NewCollecting(t).
			False(HasCertAnnotation(context.Background(), cl), "expected false when no annotation")
	})

	t.Run("False When Configs Missing", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		assert.NewCollecting(t).
			False(HasCertAnnotation(context.Background(), cl), "expected false when configs don't exist")
	})
}

func TestFindOperatorDeployment(t *testing.T) {
	t.Parallel()

	namespace := "test-ns"
	labels := map[string]string{"app.kubernetes.io/name": "multigres-operator"}

	t.Run("Found by Labels", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-operator",
				Namespace: namespace,
				Labels:    labels,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(dep).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, labels, "")
		c.Require().NoError(err, "unexpected error")
		c.False(
			got == nil || got.Name != "my-operator",
			"expected deployment 'my-operator', got %v",
			got,
		)
	})

	t.Run("Found by Name", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "explicit-name",
				Namespace: namespace,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(dep).Build()
		got, err := FindOperatorDeployment(
			context.Background(),
			cl,
			namespace,
			nil,
			"explicit-name",
		)
		c.Require().NoError(err, "unexpected error")
		c.False(
			got == nil || got.Name != "explicit-name",
			"expected deployment 'explicit-name', got %v",
			got,
		)
	})

	t.Run("Not Found Returns nil", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, nil, "nonexistent")
		c.Require().NoError(err, "unexpected error")
		c.Nil(got, "expected nil, got")
	})

	t.Run("No Labels No Name Returns nil", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, nil, "")
		c.Require().NoError(err, "unexpected error")
		c.Nil(got, "expected nil, got")
	})

	t.Run("Multiple Matches Picks Oldest", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)

		older := metav1.NewTime(time.Now().Add(-1 * time.Hour))
		newer := metav1.NewTime(time.Now())
		dep1 := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "op-newer",
				Namespace:         namespace,
				Labels:            labels,
				CreationTimestamp: newer,
			},
		}
		dep2 := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "op-older",
				Namespace:         namespace,
				Labels:            labels,
				CreationTimestamp: older,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(dep1, dep2).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, labels, "")
		c.Require().NoError(err, "unexpected error")
		c.False(
			got == nil || got.Name != "op-older",
			"expected oldest deployment 'op-older', got %v",
			got,
		)
	})

	t.Run("Error: List Failure", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					return errors.New("list error")
				},
			}).
			Build()

		_, err := FindOperatorDeployment(context.Background(), cl, namespace, labels, "")
		assert.NewCollecting(t).
			False(err == nil || !strings.Contains(err.Error(), "failed to list deployments by labels"), "expected list error, got: %v", err)
	})

	t.Run("Error: Get by Name Failure", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().
			WithScheme(pkiScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					return errors.New("get error")
				},
			}).
			Build()

		_, err := FindOperatorDeployment(context.Background(), cl, namespace, nil, "some-name")
		assert.NewCollecting(t).False(err == nil ||
			!strings.Contains(
				err.Error(),
				"failed to get operator deployment by name",
			), "expected get error, got: %v", err)
	})
}
