//go:build e2e

package framework

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing"
	"time"
)

// certManagerVersion is the cert-manager release the topology TLS scenario
// installs. Bump it in one place.
const certManagerVersion = "v1.16.2"

// topoTLSIssuerName is the CA ClusterIssuer the topology TLS scenario points
// topoTLS.issuerName at. The etcd serving certificate and the per-cluster
// client credential both chain to this one CA, so the server trusts the client.
const topoTLSIssuerName = "topo-e2e-ca"

// caIssuerManifest is a self-signed root, a CA certificate issued from it, and a
// CA ClusterIssuer backed by that certificate. A self-signed issuer alone would
// sign each certificate under a different key, so the topology server would not
// trust the client; the shared CA is what makes mutual verification work. The
// CA Secret lives in the cert-manager namespace because that is where a
// ClusterIssuer of kind CA reads it from by default.
const caIssuerManifest = `apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: topo-e2e-selfsigned
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: topo-e2e-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: topo-e2e-ca
  secretName: topo-e2e-ca
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: topo-e2e-selfsigned
    kind: ClusterIssuer
    group: cert-manager.io
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: topo-e2e-ca
spec:
  ca:
    secretName: topo-e2e-ca
`

// EnsureTopoTLSIssuer installs cert-manager and creates the CA ClusterIssuer the
// topology TLS scenario signs with. It is idempotent, so a retained cluster can
// be reused. It returns the issuer name to put in topoTLS.issuerName.
func (c *Cluster) EnsureTopoTLSIssuer(t testing.TB) string {
	t.Helper()

	url := fmt.Sprintf(
		"https://github.com/cert-manager/cert-manager/releases/download/%s/cert-manager.yaml",
		certManagerVersion,
	)
	if err := c.kubectl("apply", "-f", url); err != nil {
		t.Fatalf("install cert-manager: %v", err)
	}
	for _, deploy := range []string{"cert-manager", "cert-manager-webhook", "cert-manager-cainjector"} {
		if err := c.kubectl(
			"-n", "cert-manager", "rollout", "status", "deployment/"+deploy, "--timeout=300s",
		); err != nil {
			t.Fatalf("wait for %s: %v", deploy, err)
		}
	}

	// The webhook can still reject requests briefly after its Deployment reports
	// available, so apply the issuer chain with a short retry.
	if err := c.retry(10, 3*time.Second, func() error {
		return c.kubectlStdin(caIssuerManifest, "apply", "-f", "-")
	}); err != nil {
		t.Fatalf("apply CA issuer: %v", err)
	}

	if err := c.kubectl(
		"wait", "--for=condition=Ready", "clusterissuer/"+topoTLSIssuerName, "--timeout=120s",
	); err != nil {
		t.Fatalf("wait for CA issuer ready: %v", err)
	}
	return topoTLSIssuerName
}

func (c *Cluster) kubectl(args ...string) error {
	full := append([]string{"--kubeconfig", c.Kubeconfig}, args...)
	cmd := exec.Command("kubectl", full...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl %v: %w\n%s", args, err, out)
	}
	return nil
}

func (c *Cluster) kubectlStdin(stdin string, args ...string) error {
	full := append([]string{"--kubeconfig", c.Kubeconfig}, args...)
	cmd := exec.Command("kubectl", full...)
	cmd.Stdin = bytes.NewBufferString(stdin)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl %v: %w\n%s", args, err, out)
	}
	return nil
}

func (c *Cluster) retry(attempts int, wait time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(wait)
	}
	return err
}
