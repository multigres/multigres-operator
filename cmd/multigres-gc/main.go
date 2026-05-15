// The multigres-gc command runs a one-shot clean of orphaned multigres resources.
//
// It is intended to be invoked from a k8s cronjob, separate from the
// operator's reconcile loop. New resource types are added by implementing
// gc.Cleankeeper and registering them in registerCleankeepers().
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/multigres/multigres-operator/pkg/gc"
	gcpvc "github.com/multigres/multigres-operator/pkg/gc/pvc"
)

var (
	// Set via -ldflags at build time.
	version   = "dev"
	buildDate = "unknown"
	gitCommit = "unknown"

	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("multigres-gc")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var (
		retention time.Duration
		namespace string
		dryRun    bool
	)

	flag.DurationVar(&retention, "retention", 30*24*time.Hour,
		"Minimum age of the orphan label before a resource is deleted "+
			"(e.g. 720h for 30d). 0 means delete as soon as an object is "+
			"marked orphan. Negative values are rejected.")
	flag.StringVar(&namespace, "namespace", "",
		"Restrict the clean to a single namespace. Empty means all namespaces.")
	flag.BoolVar(&dryRun, "dry-run", false,
		"Log what would be deleted without actually deleting.")

	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	if retention < 0 {
		setupLog.Error(
			fmt.Errorf("invalid retention %s", retention),
			"--retention must be >= 0 (0 means delete as soon as an object is marked orphan)",
		)
		os.Exit(1)
	}

	setupLog.Info("starting multigres garbage collector",
		"version", version,
		"buildDate", buildDate,
		"gitCommit", gitCommit,
		"retention", retention.String(),
		"namespace", namespaceOrAll(namespace),
		"dry_run", dryRun,
	)

	cfg, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "failed to load kubernetes config")
		os.Exit(1)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "failed to build kubernetes client")
		os.Exit(1)
	}

	opts := gc.Options{
		Retention: retention,
		Namespace: namespace,
		DryRun:    dryRun,
	}

	cleankeepers := registerCleankeepers(c, setupLog, opts)
	ctx := ctrl.SetupSignalHandler()

	exit := 0
	for _, ck := range cleankeepers {
		log := setupLog.WithValues("kind", ck.Kind())
		res, err := ck.Clean(ctx)
		if err != nil {
			log.Error(err, "clean aborted")
			exit = 1
			continue
		}
		if res.Errors > 0 {
			log.Info("clean finished with per-object errors", "errors", res.Errors)
			if exit == 0 {
				exit = 2
			}
		}
	}

	os.Exit(exit)
}

// registerCleankeepers returns the list of cleankeepers run on each invocation.
// Add new resource kinds here.
func registerCleankeepers(c client.Client, log logr.Logger, opts gc.Options) []gc.Cleankeeper {
	return []gc.Cleankeeper{
		gcpvc.New(c, log, opts),
	}
}

func namespaceOrAll(ns string) string {
	if ns == "" {
		return "<all>"
	}
	return ns
}
