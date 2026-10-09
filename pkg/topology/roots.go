// Package topology defines the logical root paths used when multiple
// Multigres clusters share one physical topology server.
package topology

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const rootPrefix = "/multigres"

// maxTLSRootBytes is the X.509 common name limit.
const maxTLSRootBytes = 64

// Roots builds canonical, disjoint topology roots for one Multigres cluster.
type Roots struct {
	clusterRoot string
}

// ForCluster applies the CN length limit only when the operator issues the
// cluster's topology client certificate, whose CN is the cluster root.
func ForCluster(cluster *multigresv1alpha1.MultigresCluster) (Roots, error) {
	return NewRoots(
		cluster.Annotations,
		cluster.Namespace,
		cluster.Name,
		multigresv1alpha1.OperatorIssuesTopoClientCert(cluster),
	)
}

// NewRoots uses the project ref, or namespace/name if absent.
// With topoTLS, fallbacks over the CN limit are hashed. Explicit refs and
// plaintext roots are unchanged.
func NewRoots(
	annotations map[string]string,
	namespace, clusterName string,
	topoTLS bool,
) (Roots, error) {
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
	clusterRoot := rootPrefix + "/" + encodedNamespace + "/" + encodedClusterName
	if topoTLS && len(clusterRoot) > maxTLSRootBytes {
		// Keep hashes outside /multigres/ so existing prefixes cannot authorize them.
		// Hashing the escaped path preserves namespace/name boundaries.
		digest := sha256.Sum256([]byte(clusterRoot))
		clusterRoot = rootPrefix + "-fallback/" + base64.RawURLEncoding.EncodeToString(digest[:])
	}
	return Roots{clusterRoot: clusterRoot}, nil
}

// ClusterRoot returns the prefix that encloses every topology record this
// cluster owns. Credentials that authorize a cluster against a shared
// topology server carry this exact string, so the identity presented and the
// keys reachable under it stay the same value.
func (r Roots) ClusterRoot() string {
	return r.clusterRoot
}

// KeyPrefix returns the range that encloses this cluster's records, including
// the trailing separator. Authorization has to grant on this rather than on
// ClusterRoot: cluster identity "proj_123" is a string prefix of "proj_1234",
// so a range opened at the bare root would reach a sibling cluster's keys.
func (r Roots) KeyPrefix() string {
	return r.clusterRoot + "/"
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
