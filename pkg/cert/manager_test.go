package cert

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/multigres-operator/pkg/testutil"
)

const (
	testCASecretName     = "test-ca-secret"     //nolint:gosec // test constant
	testServerSecretName = "test-server-secret" //nolint:gosec // test constant
)

func defaultTestOptions(certDir string) Options {
	return Options{
		Namespace:         "test-ns",
		CASecretName:      testCASecretName,
		ServerSecretName:  testServerSecretName,
		ServiceName:       "test-svc",
		CertDir:           certDir,
		WaitForProjection: true,
	}
}

func TestManager_EnsureCerts(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "test-ns"
		serviceName = "test-svc"
	)

	expectedDNSName := serviceName + "." + namespace + ".svc"

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	// Helper to generate a dummy CA and a signed cert
	validCABytes, validCAKeyBytes := generateCAPEM(t)
	ca, _ := ParseCA(validCABytes, validCAKeyBytes)

	validCert := generateSignedCertPEM(
		t,
		ca,
		time.Now().Add(365*24*time.Hour),
		[]string{expectedDNSName},
	)
	expiredCert := generateSignedCertPEM(
		t,
		ca,
		time.Now().Add(-1*time.Hour),
		[]string{expectedDNSName},
	)
	nearExpiryCert := generateSignedCertPEM(
		t,
		ca,
		time.Now().Add(15*24*time.Hour),
		[]string{expectedDNSName},
	)

	otherCA, _ := GenerateCA("")
	signedByOtherCACert := generateSignedCertPEM(
		t,
		otherCA,
		time.Now().Add(time.Hour),
		[]string{expectedDNSName},
	)
	signedByOtherCAValidCert := generateSignedCertPEM(
		t,
		otherCA,
		time.Now().Add(365*24*time.Hour),
		[]string{expectedDNSName},
	)

	corruptCertBody := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: []byte("this is not a valid der certificate"),
	})

	tests := map[string]struct {
		existingObjects []client.Object
		failureConfig   *testutil.FailureConfig
		customCertDir   string
		customOptions   *Options
		wantErr         bool
		errContains     string
		wantGenerated   bool
		checkFiles      bool
	}{
		"Bootstrap: Fresh Install": {
			checkFiles:    true,
			wantGenerated: true,
		},
		"Idempotency: Valid Secret Exists": {
			existingObjects: []client.Object{
				// CA Secret
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				// Server Secret
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Type:       corev1.SecretTypeTLS,
					Data: map[string][]byte{
						"tls.crt": validCert,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: false,
		},
		"Rotation: Expired Server Cert": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": expiredCert,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: Near Expiry Server Cert": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": nearExpiryCert,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: Corrupt Cert Body": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": corruptCertBody,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: CA Near Expiry": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": nearExpiryCert, // Using near expiry cert as CA cert
						"ca.key": validCAKeyBytes,
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: Signed by Different CA": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": signedByOtherCACert,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: Wrong CA (Still Valid)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": signedByOtherCAValidCert,
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Rotation: Corrupt Cert Data (No Block)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": []byte("not pem"),
						"tls.key": []byte("key"),
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Recreation: Corrupt CA Secret": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": []byte("corrupt-pem-data"),
						"ca.key": validCAKeyBytes,
					},
				},
			},
			checkFiles:    true,
			wantGenerated: true,
		},
		"Error: Get CA Secret Failed": {
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName(testCASecretName, errors.New("injected get error")),
			},
			wantErr:     true,
			errContains: "injected get error",
		},
		"Error: Create CA Secret Failed": {
			failureConfig: &testutil.FailureConfig{
				OnCreate: testutil.FailOnObjectName(
					testCASecretName,
					errors.New("injected create error"),
				),
			},
			wantErr:     true,
			errContains: "failed to create CA secret",
		},
		"Error: File System (Mkdir/Write)": {
			customCertDir: "/dev/null/invalid-dir",
			wantErr:       true,
			errContains:   "mkdir",
		},
		"Error: Update Server Cert Failed": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data:       map[string][]byte{"tls.crt": expiredCert, "tls.key": []byte("key")},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnUpdate: testutil.FailOnObjectName(
					testServerSecretName,
					errors.New("injected update error"),
				),
			},
			wantErr:     true,
			errContains: "failed to update server cert secret",
		},
		"Error: Delete Failed (Corrupt CA)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": []byte("corrupt"),
						"ca.key": []byte("key"),
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(testCASecretName, errors.New("delete fail")),
			},
			wantErr:     true,
			errContains: "failed to delete corrupt CA secret",
		},
		"Error: Delete Failed (Expiring CA)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": generateSignedCertPEM(
							t,
							ca,
							time.Now().Add(15*24*time.Hour),
							[]string{"ca"},
						),
						"ca.key": validCAKeyBytes,
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(testCASecretName, errors.New("delete fail")),
			},
			wantErr:     true,
			errContains: "failed to delete expiring CA secret",
		},
		"Error: Delete Failed (Corrupt Server Cert)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": []byte("corrupt"),
						"tls.key": []byte("key"),
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(
					testServerSecretName,
					errors.New("delete fail"),
				),
			},
			wantErr:     true,
			errContains: "failed to delete corrupt server cert secret",
		},
		"Error: Delete Failed (Corrupt Server Cert Data)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": []byte("not pem"),
						"tls.key": []byte("key"),
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(
					testServerSecretName,
					errors.New("delete fail"),
				),
			},
			wantErr:     true,
			errContains: "failed to delete corrupt server cert secret",
		},
		"Error: Delete Failed (Unparseable Server Cert)": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
					Data: map[string][]byte{
						"tls.crt": corruptCertBody,
						"tls.key": []byte("key"),
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnDelete: testutil.FailOnObjectName(
					testServerSecretName,
					errors.New("delete fail"),
				),
			},
			wantErr:     true,
			errContains: "failed to delete unparseable server cert secret",
		},
		"Error: Create Server Secret Failed": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnCreate: testutil.FailOnObjectName(
					testServerSecretName,
					errors.New("server create fail"),
				),
			},
			wantErr:     true,
			errContains: "failed to create server cert secret",
		},
		"Error: Get Server Secret Failed": {
			existingObjects: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
					Data: map[string][]byte{
						"ca.crt": validCABytes,
						"ca.key": validCAKeyBytes,
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName(testServerSecretName, errors.New("server get fail")),
			},
			wantErr:     true,
			errContains: "failed to get server cert secret",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fakeClient := fake.NewClientBuilder().
				WithScheme(s).
				WithObjects(tc.existingObjects...).
				Build()

			var cl client.Client = fakeClient
			if tc.failureConfig != nil {
				cl = testutil.NewFakeClientWithFailures(fakeClient, tc.failureConfig)
			}

			certDir := tc.customCertDir
			if certDir == "" {
				certDir = t.TempDir()
			}

			// Pre-populate files for "Existing Secret" scenarios.
			for _, obj := range tc.existingObjects {
				if s, ok := obj.(*corev1.Secret); ok && s.Name == testServerSecretName {
					if _, err := os.Stat(certDir); err == nil {
						_ = os.WriteFile(
							filepath.Join(certDir, CertFileName),
							s.Data["tls.crt"],
							0o600,
						)
						_ = os.WriteFile(
							filepath.Join(certDir, KeyFileName),
							s.Data["tls.key"],
							0o600,
						)
					}
				}
			}

			// Mock Kubelet for *updates* during the test
			hookClient := &mockProjectionClient{
				Client:  cl,
				CertDir: certDir,
			}

			opts := defaultTestOptions(certDir)
			if tc.customOptions != nil {
				opts = *tc.customOptions
				opts.Namespace = namespace
				opts.CertDir = certDir
				opts.ServiceName = serviceName
			}

			mgr := NewManager(hookClient, record.NewFakeRecorder(10), opts)

			err := mgr.Bootstrap(t.Context())

			if tc.wantErr {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf(
						"Error message mismatch. Got: %v, Want substring: %s",
						err,
						tc.errContains,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if tc.checkFiles {
				if _, err := os.Stat(filepath.Join(certDir, CertFileName)); os.IsNotExist(err) {
					t.Errorf("Cert file not found at %s", CertFileName)
				}
			}

			if tc.wantGenerated {
				secret := &corev1.Secret{}
				_ = fakeClient.Get(
					t.Context(),
					types.NamespacedName{Name: testServerSecretName, Namespace: namespace},
					secret,
				)

				var original []byte
				for _, obj := range tc.existingObjects {
					if s, ok := obj.(*corev1.Secret); ok && s.Name == testServerSecretName {
						original = s.Data["tls.crt"]
						break
					}
				}
				if len(original) > 0 && bytes.Equal(secret.Data["tls.crt"], original) {
					t.Error("Expected rotation, but cert did not change")
				}
			}
		})
	}
}

// mockProjectionClient intercepts Secret updates and writes to disk to simulate Kubelet volume projection
type mockProjectionClient struct {
	client.Client
	CertDir string
}

func (m *mockProjectionClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	err := m.Client.Create(ctx, obj, opts...)
	if err == nil {
		m.syncToDisk(obj)
	}
	return err
}

func (m *mockProjectionClient) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.UpdateOption,
) error {
	err := m.Client.Update(ctx, obj, opts...)
	if err == nil {
		m.syncToDisk(obj)
	}
	return err
}

func (m *mockProjectionClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	err := m.Client.Patch(ctx, obj, patch, opts...)
	if err == nil {
		m.syncToDisk(obj)
	}
	return err
}

func (m *mockProjectionClient) syncToDisk(obj client.Object) {
	if secret, ok := obj.(*corev1.Secret); ok && secret.Name == testServerSecretName {
		_ = os.WriteFile(filepath.Join(m.CertDir, CertFileName), secret.Data["tls.crt"], 0o600)
		_ = os.WriteFile(filepath.Join(m.CertDir, KeyFileName), secret.Data["tls.key"], 0o600)
	}
}

// badSchemeClient wraps a real client but returns an empty scheme from Scheme(),
// causing SetControllerReference to fail when looking up GVKs.
type badSchemeClient struct {
	client.Client
}

func (b *badSchemeClient) Scheme() *runtime.Scheme {
	return runtime.NewScheme()
}

func generateCAPEM(tb testing.TB) ([]byte, []byte) {
	tb.Helper()
	ca, err := GenerateCA("")
	if err != nil {
		tb.Fatal(err)
	}
	return ca.CertPEM, ca.KeyPEM
}

func generateSignedCertPEM(
	tb testing.TB,
	ca *CAArtifacts,
	expiry time.Time,
	dnsNames []string,
) []byte {
	tb.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		tb.Fatal(err)
	}

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "server"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     expiry,
		DNSNames:     dnsNames,
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, ca.Cert, &priv.PublicKey, ca.Key)
	if err != nil {
		tb.Fatal(err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestManager_PostReconcileHook(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	t.Run("Hook Called with CA Bundle", func(t *testing.T) {
		t.Parallel()

		var hookCalled atomic.Bool
		var receivedCABundle []byte

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			PostReconcileHook: func(_ context.Context, caBundle []byte) error {
				hookCalled.Store(true)
				receivedCABundle = caBundle
				return nil
			},
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !hookCalled.Load() {
			t.Error("PostReconcileHook was not called")
		}
		if len(receivedCABundle) == 0 {
			t.Error("PostReconcileHook received empty CA bundle")
		}

		// Verify the CA bundle is valid PEM
		block, _ := pem.Decode(receivedCABundle)
		if block == nil {
			t.Error("CA bundle is not valid PEM")
		}
	})

	t.Run("Hook Error Propagates", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			PostReconcileHook: func(_ context.Context, _ []byte) error {
				return errors.New("hook failure")
			},
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		err := mgr.Bootstrap(t.Context())
		if err == nil || !strings.Contains(err.Error(), "post-reconcile hook failed") {
			t.Errorf("Expected hook error, got %v", err)
		}
	})

	t.Run("No Hook (nil) Succeeds", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			// PostReconcileHook is nil
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

func TestManager_OwnerRef(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	t.Run("Owner Set: Secrets Get Owner Reference", func(t *testing.T) {
		t.Parallel()

		owner := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-operator",
				Namespace: "test-ns",
				UID:       "test-uid-123",
			},
		}

		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(owner).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			Owner:            owner,
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Verify CA secret has owner reference
		caSecret := &corev1.Secret{}
		if err := cl.Get(t.Context(), types.NamespacedName{
			Name:      testCASecretName,
			Namespace: "test-ns",
		}, caSecret); err != nil {
			t.Fatalf("Failed to get CA secret: %v", err)
		}
		if len(caSecret.OwnerReferences) == 0 {
			t.Error("Expected CA secret to have owner reference")
		}

		// Verify server secret has owner reference
		srvSecret := &corev1.Secret{}
		if err := cl.Get(t.Context(), types.NamespacedName{
			Name:      testServerSecretName,
			Namespace: "test-ns",
		}, srvSecret); err != nil {
			t.Fatalf("Failed to get server secret: %v", err)
		}
		if len(srvSecret.OwnerReferences) == 0 {
			t.Error("Expected server secret to have owner reference")
		}
	})

	t.Run("No Owner: Secrets Created Without Owner Reference", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			// Owner is nil
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		caSecret := &corev1.Secret{}
		if err := cl.Get(t.Context(), types.NamespacedName{
			Name:      testCASecretName,
			Namespace: "test-ns",
		}, caSecret); err != nil {
			t.Fatalf("Failed to get CA secret: %v", err)
		}
		if len(caSecret.OwnerReferences) != 0 {
			t.Error("Expected CA secret to have no owner references")
		}
	})

	t.Run("Owner with Bad Scheme: SetControllerReference Fails", func(t *testing.T) {
		t.Parallel()

		owner := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-operator",
				Namespace: "test-ns",
				UID:       "test-uid-123",
			},
		}

		// Use a real client (with proper scheme for CRUD) but wrap it to return
		// an empty scheme from Scheme() — this is what SetControllerReference
		// uses to discover GVKs.
		realCl := fake.NewClientBuilder().WithScheme(s).Build()
		cl := &badSchemeClient{Client: realCl}
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			Owner:            owner,
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		err := mgr.Bootstrap(t.Context())
		if err == nil || !strings.Contains(err.Error(), "failed to set controller reference") {
			t.Errorf("Expected controller ref error, got %v", err)
		}
	})

	t.Run("Owner with Bad Scheme: SetControllerReference Fails on Server Cert", func(t *testing.T) {
		t.Parallel()

		validCABytes, validCAKeyBytes := generateCAPEM(t)

		owner := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-operator",
				Namespace: "test-ns",
				UID:       "test-uid-456",
			},
		}

		// Pre-create CA secret so ensureCA finds it and skips setOwner.
		caSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: "test-ns"},
			Data:       map[string][]byte{"ca.crt": validCABytes, "ca.key": validCAKeyBytes},
		}

		realCl := fake.NewClientBuilder().WithScheme(s).WithObjects(caSecret).Build()
		cl := &badSchemeClient{Client: realCl}
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			Owner:            owner,
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		err := mgr.Bootstrap(t.Context())
		if err == nil ||
			!strings.Contains(err.Error(), "failed to set owner for server cert secret") {
			t.Errorf("Expected server cert owner error, got %v", err)
		}
	})
}

func TestManager_WaitForProjection(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	t.Run("Disabled: No File Wait", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:         "test-ns",
			CASecretName:      testCASecretName,
			ServerSecretName:  testServerSecretName,
			ServiceName:       "test-svc",
			WaitForProjection: false,
			// CertDir not set — should not matter since projection is off
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("Enabled: Mismatch Timeout", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		_ = os.WriteFile(filepath.Join(dir, CertFileName), []byte("wrong"), 0o600)

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		mgr := NewManager(cl, nil, Options{
			Namespace:         "test-ns",
			CASecretName:      testCASecretName,
			ServerSecretName:  testServerSecretName,
			ServiceName:       "test-svc",
			CertDir:           dir,
			WaitForProjection: true,
		})

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		// waitForProjection is called internally by reconcilePKI, bypass Bootstrap
		// and test it directly
		_ = mgr.waitForProjection(ctx, []byte("expected"))
	})

	t.Run("Enabled: File Not Found Retries", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// No cert file on disk — waitForProjection should retry until timeout
		cl := fake.NewClientBuilder().WithScheme(s).Build()
		mgr := NewManager(cl, nil, Options{
			Namespace:         "test-ns",
			CASecretName:      testCASecretName,
			ServerSecretName:  testServerSecretName,
			ServiceName:       "test-svc",
			CertDir:           dir,
			WaitForProjection: true,
		})

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		err := mgr.waitForProjection(ctx, []byte("expected"))
		if err == nil {
			t.Error("Expected timeout error for missing file")
		}
	})

	t.Run("Enabled: File Matches Immediately", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		expected := []byte("matching-cert-content")
		_ = os.WriteFile(filepath.Join(dir, CertFileName), expected, 0o600)

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		mgr := NewManager(cl, nil, Options{
			Namespace:         "test-ns",
			CASecretName:      testCASecretName,
			ServerSecretName:  testServerSecretName,
			ServiceName:       "test-svc",
			CertDir:           dir,
			WaitForProjection: true,
		})

		if err := mgr.waitForProjection(t.Context(), expected); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

func TestManager_ExtKeyUsages(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	t.Run("Custom ExtKeyUsages Flow Through to Cert", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			ExtKeyUsages: []x509.ExtKeyUsage{
				x509.ExtKeyUsageServerAuth,
				x509.ExtKeyUsageClientAuth,
			},
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Read the generated server cert and verify ExtKeyUsages
		secret := &corev1.Secret{}
		if err := cl.Get(t.Context(), types.NamespacedName{
			Name:      testServerSecretName,
			Namespace: "test-ns",
		}, secret); err != nil {
			t.Fatalf("Failed to get server secret: %v", err)
		}

		block, _ := pem.Decode(secret.Data["tls.crt"])
		if block == nil {
			t.Fatal("Failed to decode server cert PEM")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("Failed to parse server cert: %v", err)
		}

		if len(cert.ExtKeyUsage) != 2 {
			t.Fatalf("Expected 2 ExtKeyUsages, got %d", len(cert.ExtKeyUsage))
		}
		if cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
			t.Errorf("Expected ServerAuth, got %v", cert.ExtKeyUsage[0])
		}
		if cert.ExtKeyUsage[1] != x509.ExtKeyUsageClientAuth {
			t.Errorf("Expected ClientAuth, got %v", cert.ExtKeyUsage[1])
		}
	})

	t.Run("Default ExtKeyUsages (ServerAuth Only)", func(t *testing.T) {
		t.Parallel()

		cl := fake.NewClientBuilder().WithScheme(s).Build()
		opts := Options{
			Namespace:        "test-ns",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			// ExtKeyUsages not set — should default to ServerAuth
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), opts)
		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		secret := &corev1.Secret{}
		if err := cl.Get(t.Context(), types.NamespacedName{
			Name:      testServerSecretName,
			Namespace: "test-ns",
		}, secret); err != nil {
			t.Fatalf("Failed to get server secret: %v", err)
		}

		block, _ := pem.Decode(secret.Data["tls.crt"])
		cert, _ := x509.ParseCertificate(block.Bytes)

		if len(cert.ExtKeyUsage) != 1 || cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
			t.Errorf("Expected default [ServerAuth], got %v", cert.ExtKeyUsage)
		}
	})
}

func TestManager_Misc(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	cl := fake.NewClientBuilder().WithScheme(s).Build()

	t.Run("Start Loop", func(t *testing.T) {
		t.Parallel()
		timeoutCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		mgr := NewManager(cl, nil, Options{
			Namespace:        "default",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			RotationInterval: 10 * time.Millisecond,
		})

		_ = mgr.Start(timeoutCtx)
	})

	t.Run("Start Loop: Default Interval", func(t *testing.T) {
		t.Parallel()
		timeoutCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()

		mgr := NewManager(cl, nil, Options{
			Namespace:        "default",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			// Interval will be Hour
		})

		_ = mgr.Start(timeoutCtx)
	})

	t.Run("Start Loop: ReconcilePKI Error Logged", func(t *testing.T) {
		t.Parallel()

		// Use a failing client so reconcilePKI fails on tick
		failClient := testutil.NewFakeClientWithFailures(
			fake.NewClientBuilder().WithScheme(s).Build(),
			&testutil.FailureConfig{
				OnGet: func(_ types.NamespacedName) error {
					return errors.New("forced get failure")
				},
			},
		)

		timeoutCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		mgr := NewManager(failClient, nil, Options{
			Namespace:        "default",
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
			RotationInterval: 10 * time.Millisecond,
		})

		// Start should not return an error — it logs reconcile failures
		if err := mgr.Start(timeoutCtx); err != nil {
			t.Fatalf("Start should only return nil, got %v", err)
		}
	})

	t.Run("ComponentName Default", func(t *testing.T) {
		t.Parallel()
		opts := Options{}
		if got := opts.componentName(); got != "cert" {
			t.Errorf("Expected default componentName 'cert', got %q", got)
		}
	})

	t.Run("ComponentName Custom", func(t *testing.T) {
		t.Parallel()
		opts := Options{ComponentName: "webhook"}
		if got := opts.componentName(); got != "webhook" {
			t.Errorf("Expected componentName 'webhook', got %q", got)
		}
	})

	t.Run("ExtKeyUsages Default", func(t *testing.T) {
		t.Parallel()
		opts := Options{}
		usages := opts.extKeyUsages()
		if len(usages) != 1 || usages[0] != x509.ExtKeyUsageServerAuth {
			t.Errorf("Expected default [ServerAuth], got %v", usages)
		}
	})

	t.Run("Organization Default", func(t *testing.T) {
		t.Parallel()
		opts := Options{}
		if got := opts.organization(); got != Organization {
			t.Errorf("Expected default organization %q, got %q", Organization, got)
		}
	})

	t.Run("Organization Custom", func(t *testing.T) {
		t.Parallel()
		opts := Options{Organization: "Acme Corp"}
		if got := opts.organization(); got != "Acme Corp" {
			t.Errorf("Expected organization 'Acme Corp', got %q", got)
		}
	})

	t.Run("RecorderEvent with Nil Recorder", func(t *testing.T) {
		t.Parallel()
		mgr := NewManager(cl, nil, Options{})
		// Should not panic
		mgr.emitEvent(&corev1.Secret{}, "Normal", "Test", "test message")
	})

	t.Run("RecorderEvent with Nil Object", func(t *testing.T) {
		t.Parallel()
		mgr := NewManager(cl, record.NewFakeRecorder(10), Options{})
		// Should not panic
		mgr.emitEvent(nil, "Normal", "Test", "test message")
	})
}

func TestManager_EntropyFailures(t *testing.T) {
	// Not parallel - modifies package-level randReader.
	// With go >= 1.26 the crypto packages ignore custom entropy readers for
	// key generation and signing (GODEBUG cryptocustomrand), so GenerateCA
	// can no longer fail via entropy injection (its serial number is fixed).
	// GenerateServerCert still fails via its serial-number generation.
	oldReader := randReader
	defer func() { randReader = oldReader }()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	namespace := "test-ns"

	t.Run("ensureServerCert: GenerateServerCert Failure (Creation)", func(t *testing.T) {
		randReader = oldReader
		caArt, _ := GenerateCA("")
		caSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
			Data:       map[string][]byte{"ca.crt": caArt.CertPEM, "ca.key": caArt.KeyPEM},
		}

		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(caSecret).Build()
		mgr := NewManager(cl, nil, Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "svc",
		})

		randReader = errorReader{}

		err := mgr.reconcilePKI(t.Context())
		if err == nil || !strings.Contains(err.Error(), "failed to generate server cert") {
			t.Errorf("Expected server cert gen error, got %v", err)
		}
	})

	t.Run("ensureServerCert: GenerateServerCert Failure (Rotation)", func(t *testing.T) {
		randReader = oldReader
		caArt, _ := GenerateCA("")

		priv, _ := rsa.GenerateKey(rand.Reader, 2048)
		tmpl := x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: "server"},
			NotBefore:    time.Now().Add(-2 * time.Hour),
			NotAfter:     time.Now().Add(-1 * time.Hour), // Expired
			DNSNames:     []string{"svc.test-ns.svc"},
		}
		der, _ := x509.CreateCertificate(rand.Reader, &tmpl, caArt.Cert, &priv.PublicKey, caArt.Key)
		expiredPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

		caSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
			Data:       map[string][]byte{"ca.crt": caArt.CertPEM, "ca.key": caArt.KeyPEM},
		}
		srvSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testServerSecretName, Namespace: namespace},
			Data:       map[string][]byte{"tls.crt": expiredPEM, "tls.key": []byte("key")},
		}

		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(caSecret, srvSecret).Build()
		mgr := NewManager(cl, nil, Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "svc",
		})

		randReader = errorReader{}

		err := mgr.reconcilePKI(t.Context())
		if err == nil || !strings.Contains(err.Error(), "failed to generate new server cert") {
			t.Errorf("Expected server cert rotation error, got %v", err)
		}
	})
}

// alreadyExistsOnCreateClient wraps a client and returns AlreadyExists on Create
// for a specific secret name, while still persisting the object in the backing store
// so that subsequent Get calls find it (simulating a cache-vs-API-server race).
type alreadyExistsOnCreateClient struct {
	client.Client
	targetName string
	persist    bool
}

func (c *alreadyExistsOnCreateClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	if obj.GetName() == c.targetName {
		if c.persist {
			_ = c.Client.Create(ctx, obj, opts...)
		}
		return apierrors.NewAlreadyExists(
			schema.GroupResource{Group: "", Resource: "secrets"},
			c.targetName,
		)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestManager_CacheRaceConditions(t *testing.T) {
	t.Parallel()

	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)

	const namespace = "test-ns"

	t.Run("ensureCA: MaxRecursionDepth", func(t *testing.T) {
		t.Parallel()

		cl := &alreadyExistsOnCreateClient{
			Client:     fake.NewClientBuilder().WithScheme(s).Build(),
			targetName: testCASecretName,
			persist:    false, // Get keeps returning NotFound
		}

		mgr := NewManager(cl, nil, Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
		})

		err := mgr.Bootstrap(t.Context())
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to ensure CA secret") {
			t.Errorf("Expected max recursion error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "informer cache") {
			t.Errorf("Expected cache label hint in error, got: %v", err)
		}
	})

	t.Run("ensureCA: AlreadyExists Retry Succeeds", func(t *testing.T) {
		t.Parallel()

		cl := &alreadyExistsOnCreateClient{
			Client:     fake.NewClientBuilder().WithScheme(s).Build(),
			targetName: testCASecretName,
			persist:    true, // Object persists so retry Get finds it
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
		})

		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Expected success after retry, got: %v", err)
		}
	})

	t.Run("ensureServerCert: MaxRecursionDepth", func(t *testing.T) {
		t.Parallel()

		validCABytes, validCAKeyBytes := generateCAPEM(t)
		caSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
			Data:       map[string][]byte{"ca.crt": validCABytes, "ca.key": validCAKeyBytes},
		}

		cl := &alreadyExistsOnCreateClient{
			Client:     fake.NewClientBuilder().WithScheme(s).WithObjects(caSecret).Build(),
			targetName: testServerSecretName,
			persist:    false,
		}

		mgr := NewManager(cl, nil, Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
		})

		err := mgr.Bootstrap(t.Context())
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to ensure server cert secret") {
			t.Errorf("Expected max recursion error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "informer cache") {
			t.Errorf("Expected cache label hint in error, got: %v", err)
		}
	})

	t.Run("ensureServerCert: AlreadyExists Retry Succeeds", func(t *testing.T) {
		t.Parallel()

		validCABytes, validCAKeyBytes := generateCAPEM(t)
		caSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: testCASecretName, Namespace: namespace},
			Data:       map[string][]byte{"ca.crt": validCABytes, "ca.key": validCAKeyBytes},
		}

		cl := &alreadyExistsOnCreateClient{
			Client:     fake.NewClientBuilder().WithScheme(s).WithObjects(caSecret).Build(),
			targetName: testServerSecretName,
			persist:    true,
		}

		mgr := NewManager(cl, record.NewFakeRecorder(10), Options{
			Namespace:        namespace,
			CASecretName:     testCASecretName,
			ServerSecretName: testServerSecretName,
			ServiceName:      "test-svc",
		})

		if err := mgr.Bootstrap(t.Context()); err != nil {
			t.Fatalf("Expected success after retry, got: %v", err)
		}
	})
}
