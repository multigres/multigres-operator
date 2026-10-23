//go:build !kubedial

package main

import (
	"context"
	"net"

	"k8s.io/client-go/rest"
)

func laptopNamespace() string { return "" }

// laptopDialer is never reached: a stock build has no --namespace, so main
// never asks for a laptop dialer.
func laptopDialer(*rest.Config) (func(context.Context, string, string) (net.Conn, error), error) {
	return nil, nil
}
