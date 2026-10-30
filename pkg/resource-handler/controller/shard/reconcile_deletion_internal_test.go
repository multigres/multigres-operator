package shard

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestHandleDeletion_ErrorPaths(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	shardLabels := map[string]string{
		metadata.LabelMultigresCluster:    "test-cluster",
		metadata.LabelMultigresDatabase:   "testdb",
		metadata.LabelMultigresTableGroup: "default",
		metadata.LabelMultigresShard:      "shard0",
	}

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-shard",
			Namespace:         "default",
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
			Finalizers:        []string{"kubernetes.io/test"},
			Labels:            shardLabels,
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard0",
		},
	}

	t.Run("error listing pods", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnList: func(list client.ObjectList) error {
				if _, ok := list.(*corev1.PodList); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{
			Client:          withShardOwner(c),
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		_, err := r.handleDeletion(context.Background(), shard)
		if err == nil {
			t.Error("expected error on Pod list failure")
		}
	})

	t.Run("error listing deployments", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
		c := testutil.NewFakeClientWithFailures(base, &testutil.FailureConfig{
			OnList: func(list client.ObjectList) error {
				if _, ok := list.(*appsv1.DeploymentList); ok {
					return testutil.ErrNetworkTimeout
				}
				return nil
			},
		})
		r := &ShardReconciler{
			Client:          withShardOwner(c),
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		_, err := r.handleDeletion(context.Background(), shard)
		if err == nil {
			t.Error("expected error on Deployment list failure")
		}
	})

	t.Run("deletion with existing deployments deletes them", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mo-deploy",
				Namespace: "default",
				Labels:    shardLabels,
			},
		}

		c := newTestClient(t, shard, deploy)
		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		_, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}

		got := &appsv1.Deployment{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "mo-deploy", Namespace: "default"},
			got,
		)
		if !errors.IsNotFound(err) {
			t.Errorf("deployment should have been deleted, but Get returned: %v", err)
		}
	})
}
