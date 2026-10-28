//go:build e2e

package minigres_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestMinigresPublicTLS exercises the operator-issued certificate through the
// gateway Service from a separate PostgreSQL client. verify-full requires both
// a trusted CA and the requested hostname; it cannot fall back to plaintext.
func TestMinigresPublicTLS(t *testing.T) {
	const commonName = "db.example.com"
	cr, pod := startLiteRuntime(t, commonName)
	ns := cr.Namespace
	pvc := dataPVC(t, ns, pod)

	secret, err := cluster.Clientset.CoreV1().Secrets(ns).
		Get(t.Context(), multigresv1alpha1.CertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, secret.Data["ca.crt"])
	block, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	require.NoError(t, cert.VerifyHostname(commonName))

	service, err := cluster.Clientset.CoreV1().Services(ns).
		Get(t.Context(), "lite-multigateway", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, service.Spec.ClusterIP)
	clientPod := publicTLSClient(t, cr)
	query := func(hostname, ca, sql string) (string, error) {
		return publicTLSSQL(t.Context(), ns, clientPod, service.Spec.ClusterIP, hostname, ca, sql)
	}
	const trustedCA = "/server-ca/ca.crt"
	eventually(t, "SQL with verified public TLS", func() (bool, error) {
		out, err := query(commonName, trustedCA, "SELECT 1")
		return out == "1", err
	})

	// pg_stat_ssl describes the backend PostgreSQL connection, not the client's
	// TLS session terminated by Minigres. Ask libpq about its actual connection.
	out, err := query(commonName, trustedCA, `\conninfo`)
	require.NoError(t, err)
	require.Contains(t, out, "SSL connection")
	t.Logf("verified client connection: %s", out)
	out, err = query(commonName, trustedCA, "CREATE TABLE public_tls_e2e (id integer PRIMARY KEY)")
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE", out)
	out, err = query(commonName, trustedCA, "INSERT INTO public_tls_e2e VALUES (1)")
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", out)
	out, err = query(commonName, trustedCA, "SELECT count(*) FROM public_tls_e2e")
	require.NoError(t, err)
	require.Equal(t, "1", out)

	for _, tc := range []struct {
		name, hostname, ca, wantError string
	}{
		{"wrong hostname", "wrong.example.com", trustedCA, "does not match host name"},
		{"untrusted CA", commonName, "/untrusted-ca/ca.crt", "certificate verify failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := query(tc.hostname, tc.ca, "SELECT 1")
			require.ErrorContains(t, err, tc.wantError)
		})
	}
	// Public SQL TLS and internal admin mTLS must work on the same Lite pod.
	admin(t, ns, pod.Name, "status")

	require.NoError(t, cluster.Clientset.CoreV1().Pods(ns).
		Delete(t.Context(), pod.Name, metav1.DeleteOptions{}))
	replacement := waitPod(t, ns, cr.Name, pod.UID)
	require.Equal(t, pvc.UID, dataPVC(t, ns, replacement).UID)
	eventually(t, "verified public TLS after pod replacement", func() (bool, error) {
		out, err := query(commonName, trustedCA, "SELECT count(*) FROM public_tls_e2e")
		return out == "1", err
	})
	out, err = query(commonName, trustedCA, "INSERT INTO public_tls_e2e VALUES (2)")
	require.NoError(t, err)
	require.Equal(t, "INSERT 0 1", out)
	out, err = query(commonName, trustedCA, "SELECT count(*) FROM public_tls_e2e")
	require.NoError(t, err)
	require.Equal(t, "2", out)
	admin(t, ns, replacement.Name, "status")
	t.Log(
		"public SQL TLS passed: verified write/read, hostname and CA rejection, and pod replacement with internal mTLS",
	)
}

func publicTLSClient(t *testing.T, cr *multigresv1alpha1.MultigresCluster) string {
	t.Helper()
	ns := cr.Namespace
	_, err := cluster.Clientset.CoreV1().ConfigMaps(ns).Create(t.Context(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "untrusted-public-tls-ca"},
		Data:       map[string]string{"ca.crt": string(untrustedPublicTLSCA(t))},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	pod, err := cluster.Clientset.CoreV1().Pods(ns).Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "public-tls-client"},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "psql",
				Image:   string(cr.Spec.Images.Postgres),
				Command: []string{"sleep", "3600"},
				Env: []corev1.EnvVar{
					{Name: "PGCONNECT_TIMEOUT", Value: "5"},
					{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: cr.Spec.PostgresPasswordSecretRef.Name,
							},
							Key: cr.Spec.PostgresPasswordSecretRef.Key,
						},
					}},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "server-ca", MountPath: "/server-ca", ReadOnly: true},
					{Name: "untrusted-ca", MountPath: "/untrusted-ca", ReadOnly: true},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "server-ca", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: multigresv1alpha1.CertSecretName,
						// The SQL client needs the CA, never the server's private key.
						Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
					},
				}},
				{Name: "untrusted-ca", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "untrusted-public-tls-ca",
						},
					},
				}},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	eventually(t, "public TLS client pod", func() (bool, error) {
		p, err := cluster.Clientset.CoreV1().
			Pods(ns).
			Get(t.Context(), pod.Name, metav1.GetOptions{})
		return err == nil && ready(p), err
	})
	return pod.Name
}

func publicTLSSQL(
	ctx context.Context,
	ns, pod, address, hostname, ca, query string,
) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// hostaddr routes through the Service while host controls certificate name
	// verification, without needing public DNS in the disposable Kind cluster.
	dsn := fmt.Sprintf(
		"host=%s hostaddr=%s port=5432 user=postgres dbname=postgres sslmode=verify-full sslrootcert=%s",
		hostname,
		address,
		ca,
	)
	// #nosec G204 -- Fixed executable and test-owned arguments; no shell expansion.
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", cluster.Kubeconfig,
		"-n", ns, "exec", pod, "-c", "psql", "--",
		"psql", "-XAt", "-v", "ON_ERROR_STOP=1", dsn, "-c", query).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("public TLS SQL: %w: %s", err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func untrustedPublicTLSCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "untrusted-e2e-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
