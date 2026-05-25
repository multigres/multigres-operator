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
)

func pkiScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	s := runtime.NewScheme()
	if err := admissionregistrationv1.AddToScheme(s); err != nil {
		tb.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		tb.Fatal(err)
	}
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

		if err := PatchWebhookCABundle(context.Background(), cl, caBundle); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify mutating: caBundle + annotation
		got := &admissionregistrationv1.MutatingWebhookConfiguration{}
		if err := cl.Get(
			context.Background(),
			client.ObjectKeyFromObject(mutating),
			got,
		); err != nil {
			t.Fatal(err)
		}
		if string(got.Webhooks[0].ClientConfig.CABundle) != string(caBundle) {
			t.Errorf(
				"mutating CABundle = %q, want %q",
				got.Webhooks[0].ClientConfig.CABundle,
				caBundle,
			)
		}
		if got.Annotations[CertStrategyAnnotation] != CertStrategySelfSigned {
			t.Errorf(
				"mutating annotation = %q, want %q",
				got.Annotations[CertStrategyAnnotation],
				CertStrategySelfSigned,
			)
		}

		// Verify validating: caBundle + annotation
		gotV := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		if err := cl.Get(
			context.Background(),
			client.ObjectKeyFromObject(validating),
			gotV,
		); err != nil {
			t.Fatal(err)
		}
		if string(gotV.Webhooks[0].ClientConfig.CABundle) != string(caBundle) {
			t.Errorf(
				"validating CABundle = %q, want %q",
				gotV.Webhooks[0].ClientConfig.CABundle,
				caBundle,
			)
		}
		if gotV.Annotations[CertStrategyAnnotation] != CertStrategySelfSigned {
			t.Errorf(
				"validating annotation = %q, want %q",
				gotV.Annotations[CertStrategyAnnotation],
				CertStrategySelfSigned,
			)
		}
	})

	t.Run("Tolerates NotFound", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()

		if err := PatchWebhookCABundle(context.Background(), cl, caBundle); err != nil {
			t.Fatalf("expected no error for missing configs, got: %v", err)
		}
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
		if err == nil || !strings.Contains(err.Error(), "failed to get mutating webhook config") {
			t.Errorf("expected mutating get error, got: %v", err)
		}
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
		if err == nil || !strings.Contains(err.Error(), "patch fail") {
			t.Errorf("expected patch error, got: %v", err)
		}
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
		if err == nil || !strings.Contains(err.Error(), "failed to get validating webhook config") {
			t.Errorf("expected validating get error, got: %v", err)
		}
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
		if err == nil || !strings.Contains(err.Error(), "patch fail") {
			t.Errorf("expected patch error, got: %v", err)
		}
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

		if err := PatchWebhookCABundle(context.Background(), cl, caBundle); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
		if !HasCertAnnotation(context.Background(), cl) {
			t.Error("expected true when mutating has annotation")
		}
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
		if !HasCertAnnotation(context.Background(), cl) {
			t.Error("expected true when validating has annotation")
		}
	})

	t.Run("False When No Annotation", func(t *testing.T) {
		t.Parallel()

		mutating := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: MutatingWebhookName},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(mutating).Build()
		if HasCertAnnotation(context.Background(), cl) {
			t.Error("expected false when no annotation")
		}
	})

	t.Run("False When Configs Missing", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		if HasCertAnnotation(context.Background(), cl) {
			t.Error("expected false when configs don't exist")
		}
	})
}

func TestFindOperatorDeployment(t *testing.T) {
	t.Parallel()

	namespace := "test-ns"
	labels := map[string]string{"app.kubernetes.io/name": "multigres-operator"}

	t.Run("Found by Labels", func(t *testing.T) {
		t.Parallel()

		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-operator",
				Namespace: namespace,
				Labels:    labels,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).WithObjects(dep).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, labels, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != "my-operator" {
			t.Errorf("expected deployment 'my-operator', got %v", got)
		}
	})

	t.Run("Found by Name", func(t *testing.T) {
		t.Parallel()

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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != "explicit-name" {
			t.Errorf("expected deployment 'explicit-name', got %v", got)
		}
	})

	t.Run("Not Found Returns nil", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, nil, "nonexistent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("No Labels No Name Returns nil", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(pkiScheme(t)).Build()
		got, err := FindOperatorDeployment(context.Background(), cl, namespace, nil, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("Multiple Matches Picks Oldest", func(t *testing.T) {
		t.Parallel()

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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != "op-older" {
			t.Errorf("expected oldest deployment 'op-older', got %v", got)
		}
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
		if err == nil || !strings.Contains(err.Error(), "failed to list deployments by labels") {
			t.Errorf("expected list error, got: %v", err)
		}
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
		if err == nil ||
			!strings.Contains(err.Error(), "failed to get operator deployment by name") {
			t.Errorf("expected get error, got: %v", err)
		}
	})
}
