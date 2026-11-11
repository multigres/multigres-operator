package topology

import (
	"net/url"
	"strconv"
	"strings"

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
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") ||
		(u.Port() != "" && u.Port() != "2379") ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
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
