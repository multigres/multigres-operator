/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// The multigres-operator command runs the Kubernetes operator for Multigres distributed database clusters.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"os"
	"path/filepath"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	// Import topology implementations for multigres
	"github.com/multigres/multigres/go/common/rpcclient"
	_ "github.com/multigres/multigres/go/common/topoclient/etcdtopo"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	multigresclustercontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/resolver"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	multigreswebhook "github.com/multigres/multigres-operator/pkg/webhook"

	gencert "github.com/multigres/multigres-operator/pkg/cert"

	"github.com/multigres/multigres-operator/pkg/monitoring"
)

var (
	// Version information - set via ldflags at build time
	version   = "dev"
	buildDate = "unknown"
	gitCommit = "unknown"
	scheme    = runtime.NewScheme()
	setupLog  = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(multigresv1alpha1.AddToScheme(scheme))
	utilruntime.Must(admissionregistrationv1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)

	// Webhook Flags
	var webhookPort int
	var webhookEnabled bool
	var webhookCertDir string
	var webhookServiceNamespace string
	var webhookServiceAccount string
	var webhookServiceName string

	defaultNS := os.Getenv("POD_NAMESPACE")
	if defaultNS == "" {
		setupLog.Error(
			errors.New("POD_NAMESPACE environment variable must be set"),
			"invalid configuration",
		)
		os.Exit(1)
	}

	defaultSA := os.Getenv("POD_SERVICE_ACCOUNT")
	if defaultSA == "" {
		setupLog.Error(
			errors.New("POD_SERVICE_ACCOUNT environment variable must be set"),
			"invalid configuration",
		)
		os.Exit(1)
	}

	// General Flags
	flag.StringVar(
		&metricsAddr,
		"metrics-bind-address",
		":8443",
		"The address the metrics endpoint binds to. Use '0' to disable.",
	)
	flag.StringVar(
		&probeAddr,
		"health-probe-bind-address",
		":8081",
		"The address the probe endpoint binds to.",
	)
	flag.BoolVar(
		&enableLeaderElection,
		"leader-elect",
		false,
		"Enable leader election for Multigres Operator.",
	)
	flag.BoolVar(
		&secureMetrics,
		"metrics-secure",
		true,
		"If set, the metrics endpoint is served securely via HTTPS.",
	)
	flag.BoolVar(
		&enableHTTP2,
		"enable-http2",
		false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers",
	)

	// Webhook Flag Configuration
	flag.IntVar(&webhookPort, "webhook-port", 9443, " The port that the webhook server serves at.")
	flag.BoolVar(&webhookEnabled, "webhook-enable", true, "Enable the admission webhook server")
	flag.StringVar(
		&webhookCertDir,
		"webhook-cert-dir",
		"/var/run/secrets/webhook",
		"Directory to store/read webhook certificates",
	)
	flag.StringVar(
		&webhookServiceNamespace,
		"webhook-service-namespace",
		defaultNS,
		"Namespace where the webhook service resides",
	)
	flag.StringVar(
		&webhookServiceAccount,
		"webhook-service-account",
		defaultSA,
		"Service Account name of the operator",
	)
	flag.StringVar(
		&webhookServiceName,
		"webhook-service-name",
		"multigres-operator-webhook-service",
		"Name of the Kubernetes Service for the webhook",
	)

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Initialize distributed tracing (noop if OTEL_EXPORTER_OTLP_ENDPOINT is unset).
	shutdownTracing, err := monitoring.InitTracing(
		context.Background(),
		"multigres-operator",
		version,
	)
	if err != nil {
		setupLog.Error(err, "failed to initialise tracing")
		os.Exit(1)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			setupLog.Error(err, "failed to shut down tracing")
		}
	}()

	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// 1. Auto-Detect Certificate Strategy
	// If cert files already exist on disk AND the operator didn't previously
	// manage them (no cert-strategy annotation), we assume an external provider
	// (e.g. cert-manager) is managing the certificates.
	useInternalCerts := false
	var tmpClient client.Client
	if webhookEnabled {
		var err error
		tmpClient, err = client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
		if err != nil {
			setupLog.Error(err, "failed to create bootstrap client")
			os.Exit(1)
		}

		switch {
		case !certsExist(webhookCertDir):
			setupLog.Info(
				"webhook certificates not found on disk; enabling internal certificate rotation",
			)
			useInternalCerts = true
		case multigreswebhook.HasCertAnnotation(context.Background(), tmpClient):
			setupLog.Info(
				"webhook certificates found on disk with operator cert-strategy annotation; resuming internal certificate rotation",
			)
			useInternalCerts = true
		default:
			setupLog.Info(
				"webhook certificates found on disk; using external certificate management",
			)
		}
	}

	// -------------------------------------------------------------------------
	// Cache Configuration (The "Hybrid" Strategy)
	// -------------------------------------------------------------------------
	// We implement a Split-Brain Caching strategy to balance Scalability vs. Usability.
	//
	// 1. GLOBAL FILTER ("The Noise Cancelling"):
	//    For high-volume resources (Secrets, Services, StatefulSets, Pods), we strictly
	//    filter the cache to ONLY store objects managed by this operator.
	//    This prevents the "Memory Bomb" where the operator caches 5,000+ Helm
	//    secrets from other tenants, leading to OOMs.
	//
	// 2. LOCAL EXCEPTION ("The Safe Zone"):
	//    For the Operator's own namespace (defaultNS), we cache EVERYTHING.
	//    This is critical for:
	//    - Cert-Manager Secrets (which are created by another controller and lack our label).
	//    - Leader Election Leases.
	//    - The Operator's own Deployment (managed by Kustomize).
	//
	// 3. UNFILTERED RESOURCES ("The Flexibility"):
	//    We do NOT filter ConfigMaps. Users frequently provide their own unlabeled
	//    ConfigMaps for Postgres configuration (postgresql.conf). The operator needs
	//    to read and hash these to trigger rolling updates. Since ConfigMaps are
	//    generally lower volume than Secrets, the trade-off favors Usability here.
	// -------------------------------------------------------------------------

	// 1. Create the Label Selector for "app.kubernetes.io/managed-by = multigres-operator"
	labelReq, _ := labels.NewRequirement(
		metadata.LabelAppManagedBy,
		selection.Equals,
		[]string{metadata.ManagedByMultigres},
	)
	selector := labels.NewSelector().Add(*labelReq)

	// 2. Define Cache Configs
	// Global Config: Strictly filter by label to prevent OOM
	filteredConfig := cache.Config{
		LabelSelector: selector,
	}
	// Local Config: Cache everything (for Cert-Manager / Leader Election)
	unfilteredConfig := cache.Config{}

	// Adjust the client config to prevent throttling with high concurrency
	config := ctrl.GetConfigOrDie()
	config.QPS = 50
	config.Burst = 100

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "multigres-operator.multigres.com",
		// RELEASE LEADER ON CANCEL: Enables faster failover during rolling upgrades
		LeaderElectionReleaseOnCancel: true,

		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				// -----------------------------------------------------------
				// SECRETS: The "Memory Bomb" Fix
				// -----------------------------------------------------------
				&corev1.Secret{}: {
					Namespaces: map[string]cache.Config{
						// Rule 1: In the Operator's Namespace, watch EVERYTHING (Cert-Manager, etc.)
						defaultNS: unfilteredConfig,
						// Rule 2: In ALL OTHER namespaces, ONLY watch labeled secrets
						cache.AllNamespaces: filteredConfig,
					},
				},
				// -----------------------------------------------------------
				// STATEFULSETS, SERVICES, PODS: High Volume Resources
				// -----------------------------------------------------------
				&appsv1.StatefulSet{}: {
					Namespaces: map[string]cache.Config{
						defaultNS:           unfilteredConfig,
						cache.AllNamespaces: filteredConfig,
					},
				},
				&corev1.Service{}: {
					Namespaces: map[string]cache.Config{
						defaultNS:           unfilteredConfig,
						cache.AllNamespaces: filteredConfig,
					},
				},
				&corev1.Pod{}: {
					Namespaces: map[string]cache.Config{
						defaultNS:           unfilteredConfig,
						cache.AllNamespaces: filteredConfig,
					},
				},
				// -----------------------------------------------------------
				// CONFIGMAPS: Left Unfiltered
				// -----------------------------------------------------------
				// We deliberately do NOT list ConfigMaps here. They fall back to
				// global defaults (Unfiltered in All Namespaces). This allows
				// the operator to read/hash user-provided ConfigMaps without labels.
			},
		},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Port:    webhookPort,
			CertDir: webhookCertDir,
			TLSOpts: tlsOpts,
		}),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// 2. Setup Internal Certificate Rotation (If enabled)
	if webhookEnabled && useInternalCerts {
		// tmpClient was already created above for the annotation check

		// Resolve owner deployment for cert secret garbage collection
		operatorLabels := map[string]string{
			"app.kubernetes.io/name": "multigres-operator",
		}
		ownerDep, err := multigreswebhook.FindOperatorDeployment(
			context.Background(), tmpClient, webhookServiceNamespace, operatorLabels, "",
		)
		if err != nil {
			setupLog.Error(err, "failed to find operator deployment for owner reference")
			os.Exit(1)
		}

		rotator := gencert.NewManager(
			tmpClient,
			mgr.GetEventRecorderFor("cert-rotator"),
			gencert.Options{
				Namespace:         webhookServiceNamespace,
				CASecretName:      multigreswebhook.CASecretName,
				ServerSecretName:  multigreswebhook.ServerSecretName,
				ServiceName:       webhookServiceName,
				CertDir:           webhookCertDir,
				Owner:             ownerDep,
				WaitForProjection: true,
				ComponentName:     "webhook",
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "multigres-operator",
				},
				PostReconcileHook: func(ctx context.Context, caBundle []byte) error {
					return multigreswebhook.PatchWebhookCABundle(ctx, tmpClient, caBundle)
				},
			},
		)

		// Bootstrap immediately to unblock Webhook Server start
		if err := rotator.Bootstrap(context.Background()); err != nil {
			setupLog.Error(err, "failed to bootstrap certificates")
			os.Exit(1)
		}

		// Register rotator as a background runnable (forever rotation)
		if err := mgr.Add(rotator); err != nil {
			setupLog.Error(err, "unable to add cert rotator to manager")
			os.Exit(1)
		}
	}

	// 3. Initialize Resolver & Controllers
	globalResolver := resolver.NewResolver(
		mgr.GetClient(),
		webhookServiceNamespace,
	)

	if err = (&multigresclustercontroller.MultigresClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("multigrescluster-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MultigresCluster")
		os.Exit(1)
	}

	if err = (&tablegroupcontroller.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TableGroup")
		os.Exit(1)
	}

	if err = (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Cell")
		os.Exit(1)
	}

	rpcClient := rpcclient.NewMultipoolerClient(
		100,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer rpcClient.Close()

	if err = (&toposervercontroller.TopoServerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TopoServer")
		os.Exit(1)
	}

	if err = (&shardcontroller.ShardReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("shard-controller"),
		APIReader: mgr.GetAPIReader(),
		RPCClient: rpcClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Shard")
		os.Exit(1)
	}

	// 4. Register Webhook Handlers
	if webhookEnabled {
		if err := multigreswebhook.Setup(mgr, globalResolver, multigreswebhook.Options{
			Namespace:          webhookServiceNamespace,
			ServiceAccountName: webhookServiceAccount,
		}); err != nil {
			setupLog.Error(err, "unable to set up webhook")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting Multigres Operator",
		"version", version,
		"buildDate", buildDate,
		"gitCommit", gitCommit,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running Multigres Operator")
		os.Exit(1)
	}
}

func certsExist(dir string) bool {
	_, errCrt := os.Stat(filepath.Join(dir, "tls.crt"))
	_, errKey := os.Stat(filepath.Join(dir, "tls.key"))
	return !os.IsNotExist(errCrt) && !os.IsNotExist(errKey)
}
