package topology

import (
	"net/url"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// EndpointMatchesTopoServer reports whether endpoint names ts's client Service,
// headless Service or one of its pods on the etcd client port. A bare Service
// name matches only from ts's own namespace. IP addresses and DNS aliases are
// not recognized.
func EndpointMatchesTopoServer(
	ts *multigresv1alpha1.TopoServer,
	namespace, endpoint string,
) bool {
	host, ok := etcdEndpointHost(endpoint)
	if !ok {
		return false
	}
	name, serverNamespace := strings.ToLower(ts.Name), strings.ToLower(ts.Namespace)
	if namespace == ts.Namespace && (host == name || host == name+"-headless") {
		return true
	}
	base := name + "." + serverNamespace
	headless := name + "-headless." + serverNamespace
	if host == base || host == base+".svc" || host == base+".svc.cluster.local" ||
		host == headless || host == headless+".svc" || host == headless+".svc.cluster.local" {
		return true
	}
	for _, suffix := range []string{"." + headless + ".svc", "." + headless + ".svc.cluster.local"} {
		pod, ok := strings.CutSuffix(host, suffix)
		if !ok {
			continue
		}
		ordinal, ok := strings.CutPrefix(pod, name+"-")
		if ok && ordinal != "" {
			if _, err := strconv.ParseUint(ordinal, 10, 32); err == nil {
				return true
			}
		}
	}
	return false
}

// ExternalMatchesTopoServer reports whether an etcd external topology spec
// names ts, with endpoints resolved from namespace.
func ExternalMatchesTopoServer(
	ts *multigresv1alpha1.TopoServer,
	namespace string,
	external *multigresv1alpha1.ExternalTopoServerSpec,
) bool {
	if external == nil || (external.Implementation != "" && external.Implementation != "etcd") {
		return false
	}
	return slices.ContainsFunc(external.Endpoints, func(e multigresv1alpha1.EndpointUrl) bool {
		return EndpointMatchesTopoServer(ts, namespace, string(e))
	})
}

// CellReferencesTopoServer reports whether cell's global topology address or
// external local topology names ts.
func CellReferencesTopoServer(
	ts *multigresv1alpha1.TopoServer,
	cell *multigresv1alpha1.Cell,
) bool {
	if EndpointMatchesTopoServer(ts, cell.Namespace, cell.Spec.GlobalTopoServer.Address) {
		return true
	}
	local := cell.Spec.TopoServer
	return local != nil && ExternalMatchesTopoServer(ts, cell.Namespace, local.External)
}

// EndpointTopoServerKeys names the TopoServers an etcd client endpoint may
// address, reading the Service name and namespace from the host. A bare Service
// name resolves in namespace. The client Service is named after the TopoServer
// and the headless one adds "-headless", so a host ending in "-headless" is
// either, and both candidates are returned, the host's name first. The keys are
// candidates, so callers confirm each against the TopoServer with
// EndpointMatchesTopoServer.
func EndpointTopoServerKeys(namespace, endpoint string) []client.ObjectKey {
	host, ok := etcdEndpointHost(endpoint)
	if !ok {
		return nil
	}
	labels := strings.Split(host, ".")
	service, serverNamespace := "", ""
	clusterLocal := strings.HasSuffix(host, ".svc.cluster.local")
	switch {
	case len(labels) == 1:
		service, serverNamespace = labels[0], namespace
	case len(labels) == 2:
		service, serverNamespace = labels[0], labels[1]
	case len(labels) == 3 && labels[2] == "svc", len(labels) == 5 && clusterLocal:
		service, serverNamespace = labels[0], labels[1]
	case len(labels) == 4 && labels[3] == "svc", len(labels) == 6 && clusterLocal:
		service, serverNamespace = labels[1], labels[2]
	default:
		return nil
	}
	if service == "" || serverNamespace == "" {
		return nil
	}
	keys := []client.ObjectKey{{Namespace: serverNamespace, Name: service}}
	if name := strings.TrimSuffix(service, "-headless"); name != service && name != "" {
		keys = append(keys, client.ObjectKey{Namespace: serverNamespace, Name: name})
	}
	return keys
}

// etcdEndpointHost returns the lowercased host of a plain http or https etcd
// client endpoint on the client port, without a trailing dot.
func etcdEndpointHost(endpoint string) (string, bool) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		(u.Port() != "" && u.Port() != "2379") ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}
	return strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), true
}
