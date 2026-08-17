package cert

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	CertFileName = "tls.crt"
	KeyFileName  = "tls.key"

	// RotationThreshold is the buffer before expiry at which certs are rotated (30 days).
	RotationThreshold = 30 * 24 * time.Hour

	// maxRecursionDepth guards against infinite recursion in ensureCA/ensureServerCert.
	// If the cached client repeatedly disagrees with the API server (e.g. missing cache
	// labels), the recursive retry will fail fast instead of hanging the goroutine.
	maxRecursionDepth = 3
)

// Options configures the CertRotator behavior. Consumers provide their own
// secret names, hooks, and owner references to make this package fully generic.
type Options struct {
	// Required fields.
	Namespace        string
	CASecretName     string
	ServerSecretName string
	ServiceName      string

	// Owner is an optional Kubernetes object to set as the controller owner reference
	// on the generated secrets. When set, secrets are garbage-collected if the owner
	// is deleted. When nil, secrets are created without an owner reference.
	Owner client.Object

	// CertDir is the directory where the server cert is expected on disk.
	// Only used when WaitForProjection is true.
	CertDir string

	// RotationInterval is how often the background rotation loop runs.
	// Defaults to 1 hour.
	RotationInterval time.Duration

	// ComponentName is used in event messages (e.g., "Generated new <ComponentName> server certificate").
	// Defaults to "cert".
	ComponentName string

	// Organization overrides the certificate Organization field.
	// Defaults to the package-level Organization constant ("Multigres Operator").
	Organization string

	// AdditionalDNSNames are extra DNS SANs appended to the auto-generated
	// service DNS names (<svc>.<ns>.svc, <svc>.<ns>.svc.cluster.local).
	AdditionalDNSNames []string

	// ExtKeyUsages overrides the extended key usages for generated server certificates.
	// Defaults to [x509.ExtKeyUsageServerAuth].
	ExtKeyUsages []x509.ExtKeyUsage

	// WaitForProjection controls whether Bootstrap blocks until the cert file
	// appears on disk matching the Secret content. Set to true when using
	// Kubelet projected volumes (e.g., webhook servers).
	WaitForProjection bool

	// PostReconcileHook is called after the CA and server cert have been ensured.
	// Receives the CA bundle PEM bytes. Use this for consumer-specific tasks
	// like patching webhook configurations with the CA bundle.
	// If nil, no hook is called.
	PostReconcileHook func(ctx context.Context, caBundle []byte) error

	// Labels are applied to generated Secret objects. This is required when
	// secrets are created outside the operator namespace, because the informer
	// cache filters secrets by label (e.g., managed-by) in non-operator namespaces.
	// Without the correct labels, secrets are invisible to the cached client,
	// causing Get to return NotFound even after Create succeeds.
	Labels map[string]string
}

func (o *Options) componentName() string {
	if o.ComponentName != "" {
		return o.ComponentName
	}
	return "cert"
}

func (o *Options) organization() string {
	if o.Organization != "" {
		return o.Organization
	}
	return Organization
}

func (o *Options) extKeyUsages() []x509.ExtKeyUsage {
	if len(o.ExtKeyUsages) > 0 {
		return o.ExtKeyUsages
	}
	return []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
}

// CertRotator manages certificate lifecycle: creation, rotation, and optional
// post-reconcile hooks. It is not webhook-specific — consumers configure
// behavior via Options.
type CertRotator struct {
	Client   client.Client
	Recorder record.EventRecorder
	Options  Options
}

// NewManager creates a new CertRotator with the provided client, recorder, and options.
func NewManager(c client.Client, recorder record.EventRecorder, opts Options) *CertRotator {
	return &CertRotator{
		Client:   c,
		Recorder: recorder,
		Options:  opts,
	}
}

// Bootstrap runs at startup to ensure PKI is healthy.
// When WaitForProjection is true, it blocks until the cert file on disk
// matches the Secret content.
func (m *CertRotator) Bootstrap(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Info("bootstrapping PKI")

	if m.Options.WaitForProjection {
		if err := os.MkdirAll(m.Options.CertDir, 0o750); err != nil {
			return fmt.Errorf("failed to create cert directory: %w", err)
		}
	}

	return m.reconcilePKI(ctx)
}

// Start runs the background rotation loop. It blocks until ctx is cancelled.
// Implements the controller-runtime Runnable interface.
func (m *CertRotator) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("pki-rotation")
	logger.Info("starting PKI rotation loop")

	interval := m.Options.RotationInterval
	if interval == 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.reconcilePKI(ctx); err != nil {
				logger.Error(err, "periodic PKI reconciliation failed")
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// reconcilePKI is the main control loop.
// 1. Ensure CA is valid.
// 2. Ensure Server Cert is valid and signed by CA.
// 3. Run PostReconcileHook (if set).
// 4. Wait for projection (if enabled).
func (m *CertRotator) reconcilePKI(ctx context.Context) error {
	ca, err := m.ensureCA(ctx, 0)
	if err != nil {
		return err
	}

	serverCertPEM, err := m.ensureServerCert(ctx, ca, 0)
	if err != nil {
		return err
	}

	if m.Options.PostReconcileHook != nil {
		if err := m.Options.PostReconcileHook(ctx, ca.CertPEM); err != nil {
			return fmt.Errorf("post-reconcile hook failed: %w", err)
		}
	}

	if m.Options.WaitForProjection {
		return m.waitForProjection(ctx, serverCertPEM)
	}

	return nil
}

func (m *CertRotator) ensureCA(ctx context.Context, depth int) (*CAArtifacts, error) {
	if depth >= maxRecursionDepth {
		return nil, fmt.Errorf(
			"failed to ensure CA secret %q after %d attempts: "+
				"secret exists on API server but is not visible in the informer cache "+
				"(check that the secret has the required cache labels)",
			m.Options.CASecretName, maxRecursionDepth,
		)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Options.CASecretName,
			Namespace: m.Options.Namespace,
			Labels:    m.Options.Labels,
		},
	}

	if err := m.Client.Get(
		ctx,
		types.NamespacedName{Name: m.Options.CASecretName, Namespace: m.Options.Namespace},
		secret,
	); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get CA secret: %w", err)
		}

		artifacts, err := GenerateCA(m.Options.organization())
		if err != nil {
			return nil, fmt.Errorf("failed to generate CA: %w", err)
		}

		secret.Data = map[string][]byte{
			"ca.crt": artifacts.CertPEM,
			"ca.key": artifacts.KeyPEM,
		}

		if err := m.setOwner(secret); err != nil {
			return nil, fmt.Errorf("failed to set owner for CA secret: %w", err)
		}

		if err := m.Client.Create(ctx, secret); err != nil {
			if errors.IsAlreadyExists(err) {
				log.FromContext(ctx).Info(
					"CA secret exists on API server but was not found in cache, retrying",
					"secret", m.Options.CASecretName,
					"attempt", depth+1,
				)
				return m.ensureCA(ctx, depth+1)
			}
			return nil, fmt.Errorf("failed to create CA secret: %w", err)
		}

		m.emitEvent(secret, "Normal", "Generated",
			fmt.Sprintf("Generated new %s CA certificate", m.Options.componentName()))
		return artifacts, nil
	}

	// Found secret, validate it
	artifacts, err := ParseCA(secret.Data["ca.crt"], secret.Data["ca.key"])
	if err != nil {
		// If corrupt, recreate
		log.FromContext(ctx).Error(err, "CA secret is corrupt, recreating")
		if err := m.Client.Delete(ctx, secret); err != nil {
			return nil, fmt.Errorf("failed to delete corrupt CA secret: %w", err)
		}
		return m.ensureCA(ctx, depth+1)
	}

	// Check if near expiry
	if time.Until(artifacts.Cert.NotAfter) < RotationThreshold {
		log.FromContext(ctx).Info("CA is near expiry, rotating")
		if err := m.Client.Delete(ctx, secret); err != nil {
			return nil, fmt.Errorf("failed to delete expiring CA secret: %w", err)
		}
		return m.ensureCA(ctx, depth+1)
	}

	return artifacts, nil
}

func (m *CertRotator) ensureServerCert(
	ctx context.Context,
	ca *CAArtifacts,
	depth int,
) ([]byte, error) {
	if depth >= maxRecursionDepth {
		return nil, fmt.Errorf(
			"failed to ensure server cert secret %q after %d attempts: "+
				"secret exists on API server but is not visible in the informer cache "+
				"(check that the secret has the required cache labels)",
			m.Options.ServerSecretName, maxRecursionDepth,
		)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Options.ServerSecretName,
			Namespace: m.Options.Namespace,
			Labels:    m.Options.Labels,
		},
	}

	dnsNames := []string{
		fmt.Sprintf("%s.%s.svc", m.Options.ServiceName, m.Options.Namespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", m.Options.ServiceName, m.Options.Namespace),
	}
	dnsNames = append(dnsNames, m.Options.AdditionalDNSNames...)

	if err := m.Client.Get(
		ctx,
		types.NamespacedName{Name: m.Options.ServerSecretName, Namespace: m.Options.Namespace},
		secret,
	); err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get server cert secret: %w", err)
		}

		artifacts, err := GenerateServerCert(ca, m.Options.ServiceName, dnsNames,
			WithExtKeyUsages(m.Options.extKeyUsages()...),
			WithOrganization(m.Options.organization()))
		if err != nil {
			return nil, fmt.Errorf("failed to generate server cert: %w", err)
		}

		secret.Data = map[string][]byte{
			"tls.crt": artifacts.CertPEM,
			"tls.key": artifacts.KeyPEM,
		}

		if err := m.setOwner(secret); err != nil {
			return nil, fmt.Errorf("failed to set owner for server cert secret: %w", err)
		}

		if err := m.Client.Create(ctx, secret); err != nil {
			if errors.IsAlreadyExists(err) {
				log.FromContext(ctx).Info(
					"server cert secret exists on API server but was not found in cache, retrying",
					"secret", m.Options.ServerSecretName,
					"attempt", depth+1,
				)
				return m.ensureServerCert(ctx, ca, depth+1)
			}
			return nil, fmt.Errorf("failed to create server cert secret: %w", err)
		}

		m.emitEvent(secret, "Normal", "Generated",
			fmt.Sprintf("Generated new %s server certificate", m.Options.componentName()))
		return artifacts.CertPEM, nil
	}

	// Validate existing
	certBlock, _ := pem.Decode(secret.Data["tls.crt"])
	if certBlock == nil {
		log.FromContext(ctx).Error(nil, "server cert secret is corrupt, recreating")
		if err := m.Client.Delete(ctx, secret); err != nil {
			return nil, fmt.Errorf("failed to delete corrupt server cert secret: %w", err)
		}
		return m.ensureServerCert(ctx, ca, depth+1)
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed to parse server cert, recreating")
		if err := m.Client.Delete(ctx, secret); err != nil {
			return nil, fmt.Errorf("failed to delete unparseable server cert secret: %w", err)
		}
		return m.ensureServerCert(ctx, ca, depth+1)
	}

	// Check if near expiry OR if CA changed OR if desired DNS SANs changed
	needsRotation := time.Until(cert.NotAfter) < RotationThreshold
	if !needsRotation {
		// Check if signed by current CA
		if err := cert.CheckSignatureFrom(ca.Cert); err != nil {
			log.FromContext(ctx).Info("server cert was not signed by current CA, rotating")
			needsRotation = true
		}
	}
	if !needsRotation && !dnsNameSetsEqual(cert.DNSNames, dnsNames) {
		log.FromContext(ctx).Info("server cert are out of date, rotating",
			"current", cert.DNSNames, "desired", dnsNames)
		needsRotation = true
	}

	if needsRotation {
		srv, err := GenerateServerCert(ca, m.Options.ServiceName, dnsNames,
			WithExtKeyUsages(m.Options.extKeyUsages()...),
			WithOrganization(m.Options.organization()))
		if err != nil {
			return nil, fmt.Errorf("failed to generate new server cert: %w", err)
		}
		secret.Data = map[string][]byte{
			"tls.crt": srv.CertPEM,
			"tls.key": srv.KeyPEM,
		}
		if err := m.Client.Update(ctx, secret); err != nil {
			return nil, fmt.Errorf("failed to update server cert secret: %w", err)
		}
		m.emitEvent(secret, "Normal", "Rotated",
			fmt.Sprintf("Rotated %s server certificate", m.Options.componentName()))
		return srv.CertPEM, nil
	}

	return secret.Data["tls.crt"], nil
}

// dnsNameSetsEqual reports whether a and b contain the same DNS names,
// ignoring order and duplicates.
func dnsNameSetsEqual(a, b []string) bool {
	toSet := func(names []string) map[string]struct{} {
		set := make(map[string]struct{}, len(names))
		for _, n := range names {
			set[n] = struct{}{}
		}
		return set
	}
	setA, setB := toSet(a), toSet(b)
	if len(setA) != len(setB) {
		return false
	}
	for n := range setA {
		if _, ok := setB[n]; !ok {
			return false
		}
	}
	return true
}

func (m *CertRotator) waitForProjection(ctx context.Context, expectedCertPEM []byte) error {
	logger := log.FromContext(ctx)
	return wait.PollUntilContextTimeout(
		ctx,
		100*time.Millisecond,
		2*time.Minute,
		true,
		func(ctx context.Context) (bool, error) {
			certPath := filepath.Join(m.Options.CertDir, CertFileName)
			diskBytes, err := os.ReadFile(certPath) //nolint:gosec // path is from trusted config
			if err != nil {
				logger.V(1).Info("Waiting for certificate file", "path", certPath, "err", err)
				return false, nil
			}

			if string(diskBytes) == string(expectedCertPEM) {
				return true, nil
			}

			logger.V(1).Info("Certificate on disk does not match Secret yet")
			return false, nil
		},
	)
}

func (m *CertRotator) setOwner(secret *corev1.Secret) error {
	if m.Options.Owner == nil {
		return nil
	}

	if err := controllerutil.SetControllerReference(
		m.Options.Owner,
		secret,
		m.Client.Scheme(),
	); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}
	return nil
}

func (m *CertRotator) emitEvent(object runtime.Object, eventtype, reason, message string) {
	if m.Recorder != nil && object != nil {
		m.Recorder.AnnotatedEventf(object, nil, eventtype, reason, message)
	}
}
