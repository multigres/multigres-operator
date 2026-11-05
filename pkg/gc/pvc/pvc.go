// Package pvc implements the gc.Cleankeeper for orphaned PersistentVolumeClaims.
//
// A PVC is eligible for garbage collection when it carries both:
//
//   - app.kubernetes.io/managed-by = multigres-operator
//   - multigres.com/orphan-since   = <UTC timestamp, OrphanTimestampFormat>
//
// PVCs whose orphan-since timestamp is older than the configured retention are
// deleted, which (assuming the StorageClass reclaim policy is Delete) also
// reclaims the underlying backend volume.
//
// Two kinds of expired PVC are left alone. One any pod references: an orphan
// is by definition unused, so such a label is stale, and deleting the PVC
// would leave it Terminating under the pod (held by kubernetes.io/pvc-protection,
// which counts a pod in any phase until the pod object is deleted) until the
// pod goes, after which the operator recreates it empty under the same name.
// And one already being deleted, which needs nothing more from this run.
package pvc

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/gc"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// ObjectKind is the short identifier used in logs and Results.
const ObjectKind = "pvc"

type Cleankeeper struct {
	c    client.Client
	log  logr.Logger
	opts gc.Options
}

func New(c client.Client, log logr.Logger, opts gc.Options) *Cleankeeper {
	return &Cleankeeper{
		c:    c,
		log:  log.WithValues("cleankeeper", ObjectKind),
		opts: opts.WithDefaults(),
	}
}

func (s *Cleankeeper) Kind() string { return ObjectKind }

func (s *Cleankeeper) Clean(ctx context.Context) (*gc.Results, error) {
	sel, err := buildSelector()
	if err != nil {
		return nil, fmt.Errorf("build label selector: %w", err)
	}

	var pvcs corev1.PersistentVolumeClaimList
	listOpts := &client.ListOptions{
		LabelSelector: sel,
		Namespace:     s.opts.Namespace,
	}
	if err := s.c.List(ctx, &pvcs, listOpts); err != nil {
		return nil, fmt.Errorf("list PVCs: %w", err)
	}

	res := &gc.Results{Kind: ObjectKind, Scanned: len(pvcs.Items)}
	cutoff := s.opts.Now().Add(-s.opts.Retention)
	users := &claimUsers{c: s.c, byNamespace: map[string]map[string]string{}}

	for i := range pvcs.Items {
		if err := s.processPVC(ctx, &pvcs.Items[i], cutoff, users, res); err != nil {
			return res, err
		}
	}

	s.log.Info("clean complete",
		"scanned", res.Scanned,
		"deleted", res.Deleted,
		"would_delete", res.WouldDelete,
		"skipped", res.Skipped,
		"in_use", res.InUse,
		"malformed", res.Malformed,
		"errors", res.Errors,
		"dry_run", s.opts.DryRun,
	)
	return res, nil
}

// processPVC returns an error only when it cannot tell whether a PVC is in use,
// which aborts the run: deleting without knowing is the one outcome that can
// destroy live data.
func (s *Cleankeeper) processPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	cutoff time.Time,
	users *claimUsers,
	res *gc.Results,
) error {
	log := s.log.WithValues("namespace", pvc.Namespace, "name", pvc.Name)

	raw, ok := pvc.Labels[metadata.LabelOrphan]
	if !ok {
		// Selector guarantees presence; treat as malformed if absent.
		log.Info("orphan label missing despite selector match")
		res.Malformed++
		return nil
	}

	orphanedAt, err := time.Parse(metadata.OrphanTimestampFormat, raw)
	if err != nil {
		log.Info("malformed orphan label, skipping", "value", raw, "error", err.Error())
		res.Malformed++
		return nil
	}

	if orphanedAt.After(cutoff) {
		log.V(1).Info("PVC inside retention window, skipping", "orphaned_at", raw)
		res.Skipped++
		return nil
	}

	if !pvc.DeletionTimestamp.IsZero() {
		log.V(1).Info("PVC already being deleted, skipping", "orphaned_at", raw)
		res.Skipped++
		return nil
	}

	pod, err := users.podUsing(ctx, pvc.Namespace, pvc.Name)
	if err != nil {
		return fmt.Errorf("list pods in namespace %s: %w", pvc.Namespace, err)
	}
	if pod != "" {
		log.Info("orphan-labelled PVC is still used by a pod, not deleting",
			"orphaned_at", raw, "pod", pod)
		res.InUse++
		return nil
	}

	if s.opts.DryRun {
		log.Info("would delete orphaned PVC", "orphaned_at", raw)
		res.WouldDelete++
		return nil
	}

	// Use the observed resourceVersion as a precondition so we don't race with
	// the operator re-adopting a PVC between List and Delete.
	preconditions := client.Preconditions{
		ResourceVersion: &pvc.ResourceVersion,
	}
	if err := s.c.Delete(ctx, pvc, preconditions); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PVC already gone, skipping")
			return nil
		}
		log.Error(err, "failed to delete PVC")
		res.Errors++
		return nil
	}
	log.Info("deleted orphaned PVC", "orphaned_at", raw)
	res.Deleted++
	return nil
}

// claimUsers answers "which pod uses this PVC" from one pod List per
// namespace, taken the first time a PVC in that namespace reaches the check.
//
// A pod created after its namespace was listed is not seen. The operator
// clears the orphan label, under a resourceVersion precondition, before it
// creates a pod on a reclaimed PVC, so such a pod's PVC no longer matches the
// resourceVersion the Delete below is conditioned on.
type claimUsers struct {
	c client.Client
	// byNamespace maps namespace to claim name to the name of one pod using it.
	byNamespace map[string]map[string]string
}

func (u *claimUsers) podUsing(ctx context.Context, namespace, claim string) (string, error) {
	claims, ok := u.byNamespace[namespace]
	if !ok {
		var pods corev1.PodList
		if err := u.c.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
			return "", err
		}
		claims = map[string]string{}
		for i := range pods.Items {
			pod := &pods.Items[i]
			for _, vol := range pod.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					claims[vol.PersistentVolumeClaim.ClaimName] = pod.Name
				}
			}
		}
		u.byNamespace[namespace] = claims
	}
	return claims[claim], nil
}

func buildSelector() (labels.Selector, error) {
	managedReq, err := labels.NewRequirement(
		metadata.LabelAppManagedBy,
		selection.Equals,
		[]string{metadata.ManagedByMultigres},
	)
	if err != nil {
		return nil, err
	}
	orphanReq, err := labels.NewRequirement(
		metadata.LabelOrphan,
		selection.Exists,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return labels.NewSelector().Add(*managedReq, *orphanReq), nil
}
