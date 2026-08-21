// Package topology defines the logical root paths used when multiple
// Multigres clusters share one physical topology server.
package topology

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const rootPrefix = "/multigres"

// Roots builds canonical, disjoint topology roots for one Multigres cluster.
type Roots struct {
	clusterRoot string
}

// NewRoots uses the downstream project reference as the stable cluster
// identity when it is present. Otherwise namespace and cluster name form the
// identity, preventing equal names in different namespaces from colliding.
func NewRoots(annotations map[string]string, namespace, clusterName string) (Roots, error) {
	if projectRef := annotations[metadata.AnnotationProjectRef]; projectRef != "" {
		encoded, err := encodeSegment("project reference", projectRef)
		if err != nil {
			return Roots{}, err
		}
		return Roots{clusterRoot: rootPrefix + "/" + encoded}, nil
	}

	encodedNamespace, err := encodeSegment("namespace", namespace)
	if err != nil {
		return Roots{}, err
	}
	encodedClusterName, err := encodeSegment("cluster name", clusterName)
	if err != nil {
		return Roots{}, err
	}
	return Roots{clusterRoot: rootPrefix + "/" + encodedNamespace + "/" + encodedClusterName}, nil
}

// Global returns the root for cluster-global topology records.
func (r Roots) Global() string {
	return r.clusterRoot + "/global"
}

// Cell returns the root for one cell's local topology records.
func (r Roots) Cell(cellName string) (string, error) {
	encoded, err := encodeSegment("cell name", cellName)
	if err != nil {
		return "", err
	}
	return r.clusterRoot + "/" + encoded, nil
}

func encodeSegment(kind, value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("%s must not be empty when deriving topology root", kind)
	}
	if strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s must not contain a NUL byte", kind)
	}

	encoded := url.PathEscape(value)
	// PathEscape intentionally leaves dot segments unchanged. Encode them so
	// downstream path-cleaning cannot change the intended identity.
	switch encoded {
	case ".":
		encoded = "%2E"
	case "..":
		encoded = "%2E%2E"
	}
	return encoded, nil
}
