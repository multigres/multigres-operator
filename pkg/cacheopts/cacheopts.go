// Package cacheopts holds the controller-runtime cache configuration shared by
// the operator binary and the multi-controller test suite.
//
// It exists as its own package for one reason: cmd/multigres-operator is
// package main and cannot be imported, so a test harness that copied this
// configuration would have no way to assert the copy still matched. Cache
// behaviour is load-bearing for controller correctness (ShardReconciler keeps
// an APIReader precisely because the cached client cannot see unlabelled
// Secrets), so a harness running against default cache options would quietly
// test something the operator never does.
package cacheopts

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

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

// New returns the manager cache options, given the namespace the operator runs
// in. Objects in that namespace are cached unfiltered; everywhere else the
// high-volume kinds are restricted to objects this operator manages.
//
// With laptopNamespace set the cache holds that one namespace and nothing else;
// "Laptop-Mode Cache Scoping" in docs/development/caching-strategy.md has the
// rules. Laptop mode takes precedence over a watch allowlist. Otherwise an explicit
// watch allowlist scopes every namespaced cache to it plus the operator namespace.
func New(operatorNamespace, laptopNamespace string, watchNamespaces ...string) cache.Options {
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

	hybrid := map[string]cache.Config{
		operatorNamespace:   unfilteredConfig,
		cache.AllNamespaces: filteredConfig,
	}
	var defaultNamespaces map[string]cache.Config
	if strings.TrimSpace(strings.Join(watchNamespaces, "")) != "" {
		defaultNamespaces = map[string]cache.Config{operatorNamespace: unfilteredConfig}
		for _, names := range watchNamespaces {
			for _, namespace := range strings.Split(names, ",") {
				if namespace = strings.TrimSpace(namespace); namespace != "" {
					defaultNamespaces[namespace] = unfilteredConfig
				}
			}
		}
		// ByObject overrides DefaultNamespaces, so every override must be scoped too.
		hybrid = defaultNamespaces
	}
	if laptopNamespace != "" {
		// Only the claimed namespace. The operator's own namespace holds its
		// leader-election lease and webhook cert material, and laptop mode
		// refuses both leader election and webhooks, so nothing reads it.
		hybrid = map[string]cache.Config{laptopNamespace: filteredConfig}
		defaultNamespaces = map[string]cache.Config{laptopNamespace: {}}
	}

	return cache.Options{
		DefaultNamespaces: defaultNamespaces,
		ByObject: map[client.Object]cache.ByObject{
			// SECRETS: the "memory bomb" fix. Unfiltered in the operator's own
			// namespace so cert-manager Secrets, which carry no label of ours,
			// stay visible.
			&corev1.Secret{}: {Namespaces: hybrid},
			// STATEFULSETS, SERVICES, PODS: high volume resources.
			&appsv1.StatefulSet{}: {Namespaces: hybrid},
			&corev1.Service{}:     {Namespaces: hybrid},
			&corev1.Pod{}:         {Namespaces: hybrid},
			// CONFIGMAPS are deliberately absent, so they fall back to
			// DefaultNamespaces (unfiltered): users supply their own unlabelled
			// ConfigMaps for postgresql.conf and the operator has to hash them.
		},
	}
}
