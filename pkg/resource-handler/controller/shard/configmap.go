package shard

import (
	_ "embed"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// DefaultPgHbaTemplate is the default pg_hba.conf template for pooler instances.
// It requires SCRAM authentication for local, client, and replication connections.
//
//go:embed templates/pg_hba_template.conf
var DefaultPgHbaTemplate string

// BuildPgHbaConfigMap creates a ConfigMap containing the pg_hba.conf template.
// This ConfigMap is shared across all pools in a shard and mounted into postgres containers.
func BuildPgHbaConfigMap(
	shard *multigresv1alpha1.Shard,
	scheme *runtime.Scheme,
) (*corev1.ConfigMap, error) {
	// TODO: Add Shard.Spec.PgHbaTemplate field to allow custom templates
	template := DefaultPgHbaTemplate

	clusterName := shard.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, "pg-hba-config")
	labels = metadata.MergeLabels(labels, shard.GetObjectMeta().GetLabels())

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PgHbaConfigMapName(shard.Name),
			Namespace: shard.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"pg_hba_template.conf": template,
		},
	}

	if err := ctrl.SetControllerReference(shard, cm, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return cm, nil
}
