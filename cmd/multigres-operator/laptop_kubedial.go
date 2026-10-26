//go:build kubedial

package main

import (
	"context"
	"flag"
	"net"

	"github.com/multigres/kubedial"
	"k8s.io/client-go/rest"
)

var namespaceFlag string

func init() {
	flag.StringVar(&namespaceFlag, "namespace", "",
		"The one namespace to reconcile exclusively (laptop mode). It must exist and "+
			"already be labelled multigres.com/reconcile=disabled.")
}

func laptopNamespace() string { return namespaceFlag }

// laptopDialer reaches in-cluster addresses from outside the cluster. No
// WithMode: ModeAuto dials directly in-cluster and port-forwards otherwise,
// whereas ModeDirect would never port-forward and so could reach nothing
// without a VPN routing pod networking.
func laptopDialer(
	cfg *rest.Config,
) (func(context.Context, string, string) (net.Conn, error), error) {
	d, err := kubedial.New(cfg)
	if err != nil {
		return nil, err
	}
	return d.DialContext, nil
}
