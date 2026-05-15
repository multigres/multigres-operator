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

	for i := range pvcs.Items {
		s.processPVC(ctx, &pvcs.Items[i], cutoff, res)
	}

	s.log.Info("clean complete",
		"scanned", res.Scanned,
		"deleted", res.Deleted,
		"would_delete", res.WouldDelete,
		"skipped", res.Skipped,
		"malformed", res.Malformed,
		"errors", res.Errors,
		"dry_run", s.opts.DryRun,
	)
	return res, nil
}

func (s *Cleankeeper) processPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	cutoff time.Time,
	res *gc.Results,
) {
	log := s.log.WithValues("namespace", pvc.Namespace, "name", pvc.Name)

	raw, ok := pvc.Labels[metadata.LabelOrphan]
	if !ok {
		// Selector guarantees presence; treat as malformed if absent.
		log.Info("orphan label missing despite selector match")
		res.Malformed++
		return
	}

	orphanedAt, err := time.Parse(metadata.OrphanTimestampFormat, raw)
	if err != nil {
		log.Info("malformed orphan label, skipping", "value", raw, "error", err.Error())
		res.Malformed++
		return
	}

	if orphanedAt.After(cutoff) {
		log.V(1).Info("PVC inside retention window, skipping", "orphaned_at", raw)
		res.Skipped++
		return
	}

	if s.opts.DryRun {
		log.Info("would delete orphaned PVC", "orphaned_at", raw)
		res.WouldDelete++
		return
	}

	// Use the observed resourceVersion as a precondition so we don't race with
	// the operator re-adopting a PVC between List and Delete.
	preconditions := client.Preconditions{
		ResourceVersion: &pvc.ResourceVersion,
	}
	if err := s.c.Delete(ctx, pvc, preconditions); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PVC already gone, skipping")
			return
		}
		log.Error(err, "failed to delete PVC")
		res.Errors++
		return
	}
	log.Info("deleted orphaned PVC", "orphaned_at", raw)
	res.Deleted++
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
