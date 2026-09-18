package ctrltest

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"go.uber.org/goleak"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// shared is the harness this package tests itself against. A consumer declares
// its own; this package declares none for anyone else.
var shared *Suite

// TestMain boots the suite once, runs the package, tears it down, and only then
// checks for leaked goroutines.
//
// Deliberately not goleak.VerifyTestMain: that calls m.Run() and checks
// immediately afterwards, with no hook in between. Since envtest and the
// manager live for the whole package rather than for one test, the check would
// run while both were still up and report the entire manager as leaked.
//
// The ignore list is empty on purpose. A clean start/stop leaks nothing
// measurable, so every future entry should be justified against a real stack
// trace rather than pre-loaded against suspects. A pre-loaded ignore is a
// permanent hole.
func TestMain(m *testing.M) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		appsv1.AddToScheme,
		corev1.AddToScheme,
		policyv1.AddToScheme,
		networkingv1.AddToScheme,
		storagev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			fmt.Fprintf(os.Stderr, "add to scheme: %v\n", err)
			os.Exit(1)
		}
	}

	s, teardown, err := Boot(Options{
		Scheme:            scheme,
		CRDPaths:          nil,
		CacheOptions:      cache.Options{},
		OperatorNamespace: "ctrltest-system",
		WatchedKinds: []client.ObjectList{
			&corev1.PodList{},
			&corev1.ConfigMapList{},
			&corev1.PersistentVolumeClaimList{},
			&appsv1.DeploymentList{},
			&appsv1.StatefulSetList{},
		},
		QPS:         50,
		Burst:       100,
		SimInterval: 250 * time.Millisecond,
		// Registering nothing is the point: it proves Boot works for a
		// consumer with no reconcilers at all, which is what makes this
		// package's own green run evidence that the boundary holds.
		Register: func(context.Context, manager.Manager, *Suite) error { return nil },
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "suite boot failed: %v\n", err)
		os.Exit(1)
	}
	shared = s

	code := m.Run()

	if err := teardown(); err != nil {
		fmt.Fprintf(os.Stderr, "suite teardown failed: %v\n", err)
		if code == 0 {
			code = 1
		}
	}

	// Only leak-check a passing run: a failed test may have left its own
	// goroutines behind, and reporting those on top of a real failure buries
	// the real failure.
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak after suite teardown: %v\n", err)
			dumpGoroutineLeakProfile()
			code = 1
		}
	}
	os.Exit(code)
}

// dumpGoroutineLeakProfile writes the runtime's own leak profile when the
// toolchain has one. It returns nil before Go 1.27, so this is a no-op today
// and becomes a diagnostic on the next toolchain bump, with no build tag.
//
// Count() does not run the leak detector but WriteTo() does, so the profile has
// to be driven through WriteTo and the count read afterwards, never before.
func dumpGoroutineLeakProfile() {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return
	}
	_ = p.WriteTo(os.Stderr, 1)
}
