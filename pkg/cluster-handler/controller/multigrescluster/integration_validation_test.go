//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"
	"time"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestMultigresCluster_Validation(t *testing.T) {
	t.Parallel()

	t.Run("Explicit Empty GlobalTopoServer (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-empty-struct", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{}, // Not nil, but empty
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with empty GlobalTopoServer struct, got nil")
		}
		// We expect CEL validation error here
		if !strings.Contains(err.Error(), "must specify exactly one of") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	t.Run("Multiadmin XOR Violation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-xor-admin", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					Spec:        &multigresv1alpha1.StatelessSpec{},
					TemplateRef: "some-template",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with Multiadmin XOR violation, got nil")
		}
		if !strings.Contains(err.Error(), "cannot specify both") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	// The API server refuses an S3 block whose TLS settings the sidecar
	// would refuse to start with, and one whose CA names no source or two.
	t.Run("S3 TLS settings", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		for name, tc := range map[string]struct {
			s3      func(*multigresv1alpha1.S3BackupConfig)
			wantErr string
		}{
			"a CA with verification off": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						Secret: &multigresv1alpha1.S3CAKeyReference{Name: "ca"},
					}
					c.VerifyTLS = ptr.To(false)
				},
				wantErr: "ca and verifyTLS: false are mutually exclusive",
			},
			"a CA with no source": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{}
				},
				wantErr: "exactly one of secret and configMap must be set",
			},
			"a CA with two sources": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						Secret:    &multigresv1alpha1.S3CAKeyReference{Name: "ca"},
						ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "ca"},
					}
				},
				wantErr: "exactly one of secret and configMap must be set",
			},
			// Names and keys no Secret or ConfigMap can have: no object
			// created later could make such a reference work.
			"a CA object name in upper case": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						Secret: &multigresv1alpha1.S3CAKeyReference{Name: "CorpCA"},
					}
				},
				wantErr: "name",
			},
			"a CA object name with a slash": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "other/ca"},
					}
				},
				wantErr: "name",
			},
			"a CA key that is one dot": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "ca", Key: "."},
					}
				},
				wantErr: "must be a key a Secret or ConfigMap can have",
			},
			"a CA key that starts with two dots": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						Secret: &multigresv1alpha1.S3CAKeyReference{Name: "ca", Key: "..data"},
					}
				},
				wantErr: "must be a key a Secret or ConfigMap can have",
			},
			"a CA object name with dots and a key with one": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						Secret: &multigresv1alpha1.S3CAKeyReference{
							Name: "corp.ca-2026", Key: ".bundle.pem",
						},
					}
				},
			},
			"a CA with verification on": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) {
					c.CA = &multigresv1alpha1.S3CAReference{
						ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "ca", Key: "tls.crt"},
					}
					c.VerifyTLS = ptr.To(true)
				},
			},
			"verification off": {
				s3: func(c *multigresv1alpha1.S3BackupConfig) { c.VerifyTLS = ptr.To(false) },
			},
		} {
			s3 := &multigresv1alpha1.S3BackupConfig{
				Bucket: "b", Region: "r", CredentialsSecret: "s3-creds",
			}
			tc.s3(s3)
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "s3-tls-", Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3, S3: s3,
					},
				},
			}
			setTestPostgresPasswordSecretRef(cluster)
			err := k8sClient.Create(t.Context(), cluster)
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("%s: %v", name, err)
				}
				continue
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: want an error containing %q, got %v", name, tc.wantErr, err)
			}
		}
	})

	t.Run("Multiple Databases (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-multi-db", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", Default: true},
					{Name: "analytics"},
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with multiple databases, got nil")
		}
		// Expect MaxItems=1 or system database rule violation
		if !strings.Contains(err.Error(), "Invalid value") &&
			!strings.Contains(err.Error(), "only the single system database") {
			t.Errorf("Expected validation error regarding DB count/rules, got: %v", err)
		}
	})

	// Topology access is only as narrow as the CA that backs it, so an enabled
	// configuration has to name its issuer rather than inherit one.
	t.Run("Topology TLS Without Issuer (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with topology TLS and no issuer, got nil")
		}
		if !strings.Contains(err.Error(), "issuerName is required when topology TLS is enabled") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	t.Run("Topology TLS With Issuer (Should Pass)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pass-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{
					Enabled:    ptr.To(true),
					IssuerName: "multigres-infra-issuer",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf(
				"Expected cluster with topology TLS and an issuer to be accepted, got: %v",
				err,
			)
		}
	})

	// Disabled and absent configurations stay valid without an issuer, so the
	// rule cannot break clusters that never opt in.
	t.Run("Topology TLS Disabled Without Issuer (Should Pass)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pass-topo-tls-off", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected cluster with topology TLS disabled to be accepted, got: %v", err)
		}
	})

	// Enablement is fixed at creation: flipping it on a running cluster cannot be
	// rolled out safely, so the API rejects the transition.
	t.Run("Enabling Topology TLS After Creation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "mutate-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		cluster.Spec.TopoTLS = &multigresv1alpha1.TopoTLSConfig{
			Enabled:    ptr.To(true),
			IssuerName: "multigres-infra-issuer",
		}
		if err := k8sClient.Update(t.Context(), cluster); err == nil {
			t.Fatal("Expected enabling topology TLS after creation to be rejected")
		}
	})

	// Rotating the issuer rotates the CA every certificate chains to, which the
	// running clients cannot follow, so changing it after creation is rejected
	// too.
	t.Run("Changing Topology TLS Issuer After Creation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "rotate-topo-issuer", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{
					Enabled:    ptr.To(true),
					IssuerName: "issuer-a",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		cluster.Spec.TopoTLS.IssuerName = "issuer-b"
		if err := k8sClient.Update(t.Context(), cluster); err == nil {
			t.Fatal("Expected changing the topology TLS issuer after creation to be rejected")
		}
	})

	// A cluster can be created in Lite mode.
	t.Run("Lite Mode At Creation (Should Pass)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pass-lite-create", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Mode:  multigresv1alpha1.ModeLite,
				Cells: []multigresv1alpha1.CellConfig{{Name: "z1", ZoneID: "us-central1-a"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected cluster in lite mode to be accepted, got: %v", err)
		}
	})

	t.Run("Unknown Mode (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-unknown-mode", Namespace: testNamespace},
			Spec:       multigresv1alpha1.MultigresClusterSpec{Mode: "mini"},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with an unknown mode, got nil")
		}
		if !strings.Contains(err.Error(), "spec.mode") {
			t.Errorf("Expected enum validation error on spec.mode, got: %v", err)
		}
	})

	// A cluster cannot enter Lite mode after creation.
	t.Run("Entering Lite Mode After Creation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "enter-lite", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "z1", ZoneID: "us-central1-a"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		err := updateClusterMode(t, k8sClient, cluster, multigresv1alpha1.ModeLite)
		if err == nil {
			t.Fatal("Expected entering lite mode after creation to be rejected")
		}
		if !strings.Contains(
			err.Error(),
			"mode is immutable and can only be set when the cluster is created",
		) {
			t.Errorf("Expected CEL transition error, got: %v", err)
		}
	})

	// A cluster cannot leave Lite mode after creation.
	t.Run("Leaving Lite Mode (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "leave-lite", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Mode:  multigresv1alpha1.ModeLite,
				Cells: []multigresv1alpha1.CellConfig{{Name: "z1", ZoneID: "us-central1-a"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		err := updateClusterMode(t, k8sClient, cluster, "")
		if err == nil {
			t.Fatal("Expected leaving lite mode after creation to be rejected")
		}
		if !strings.Contains(
			err.Error(),
			"mode is immutable and can only be set when the cluster is created",
		) {
			t.Errorf("Expected CEL transition error, got: %v", err)
		}
	})
}

// updateClusterMode sets spec.mode on a fresh read of the cluster. It retries
// while the cached client has not seen the cluster yet and on conflicts, so
// neither cache lag nor a concurrent controller write can stand in for a
// validation error.
func updateClusterMode(
	t *testing.T,
	c client.Client,
	cluster *multigresv1alpha1.MultigresCluster,
	mode string,
) error {
	t.Helper()
	backoff := wait.Backoff{Steps: 50, Duration: 100 * time.Millisecond, Factor: 1}
	retriable := func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsNotFound(err)
	}
	return retry.OnError(backoff, retriable, func() error {
		current := &multigresv1alpha1.MultigresCluster{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(cluster), current); err != nil {
			return err
		}
		current.Spec.Mode = mode
		return c.Update(t.Context(), current)
	})
}
