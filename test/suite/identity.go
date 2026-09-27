package suite

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/testkit/ctrltest"
)

// Members is a snapshot of a Shard's pod roles, taken at the moment of a call.
type Members struct {
	Primary     string   // pod name with role PRIMARY
	Replicas    []string // pod names with role REPLICA, sorted
	Quarantined []string // pod names with role QUARANTINED, sorted
}

// MembersOf reads the Shard's status.podRoles and classifies every pod.
// Error-returning because callers include KnownDefect bodies.
//
// The role set here is {PRIMARY, REPLICA, QUARANTINED}, not the
// {PRIMARY, REPLICA, DRAINED} the CRD doc comment on PodRoles still claims
// (api/v1alpha1/shard_types.go:329, stale since e3677f0). PodRoles has one
// writer, reconcile_data_plane.go, fed entirely by GetPoolerStatus in
// pkg/data-handler/topo/pooler.go, where roleName is one of exactly those
// three literals. DRAINED cannot be produced by the real operator today, so
// it falls to the default arm below like any other unrecognized value.
func MembersOf(ctx context.Context, c client.Client, key client.ObjectKey) (Members, error) {
	shard := &Shard{}
	if err := c.Get(ctx, key, shard); err != nil {
		return Members{}, fmt.Errorf("get shard %s: %w", key, err)
	}

	roles := shard.Status.PodRoles
	if len(roles) == 0 {
		return Members{}, fmt.Errorf("shard %s: status.podRoles is empty", key)
	}

	var primaries, replicas, quarantined []string
	for pod, role := range roles {
		switch role {
		case "PRIMARY":
			primaries = append(primaries, pod)
		case "REPLICA":
			replicas = append(replicas, pod)
		case "QUARANTINED":
			quarantined = append(quarantined, pod)
		default:
			return Members{}, fmt.Errorf(
				"shard %s: pod %s has unrecognized role %q", key, pod, role,
			)
		}
	}

	switch len(primaries) {
	case 0:
		return Members{}, fmt.Errorf("shard %s: no pod has role PRIMARY", key)
	case 1:
	default:
		sort.Strings(primaries)
		return Members{}, fmt.Errorf(
			"shard %s: more than one pod has role PRIMARY: %s",
			key, strings.Join(primaries, ", "),
		)
	}

	sort.Strings(replicas)
	sort.Strings(quarantined)

	return Members{
		Primary:     primaries[0],
		Replicas:    replicas,
		Quarantined: quarantined,
	}, nil
}

// ShardPVCOf returns the PVC bound by the named pool pod's data volume.
//
// Pool pods only. The toposerver controller declares its own
// DataVolumeName ("data", in its statefulset builder), so this returns a
// not-found error for a toposerver pod rather than that pod's data PVC. A
// loud error rather than a wrong answer, but the restriction is not
// visible in the signature.
//
// A pool pod can carry a second PVC-backed volume (the filesystem backup
// volume, when shard.Spec.Backup.Type is Filesystem; see
// buildSharedBackupVolume in the shard controller), which is why the
// underlying lookup selects by volume name rather than requiring the pod
// to have exactly one PVC volume.
func ShardPVCOf(
	ctx context.Context, c client.Client, ns, pod string,
) (string, error) {
	return ctrltest.PVCOf(ctx, c, ns, pod, shardcontroller.DataVolumeName)
}
