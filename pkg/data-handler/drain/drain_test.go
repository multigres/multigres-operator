package drain_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/drain"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestExecuteDrainStateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from string
		want string
	}{
		{name: "requested", from: metadata.DrainStateRequested, want: metadata.DrainStateDraining},
		{
			name: "draining",
			from: metadata.DrainStateDraining,
			want: metadata.DrainStateAcknowledged,
		},
		{
			name: "acknowledged",
			from: metadata.DrainStateAcknowledged,
			want: metadata.DrainStateReadyForDeletion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewAborting(t)
			shard, pod, k8sClient := testObjects(t, tt.from)

			requeue, err := drain.ExecuteDrainStateMachine(
				context.Background(), k8sClient, record.NewFakeRecorder(1), shard, pod,
			)
			c.NoError(err, "execute drain state machine")
			c.True(requeue, "expected a requeue after a state transition")

			updated := &corev1.Pod{}
			c.NoError(k8sClient.Get(
				context.Background(),
				client.ObjectKeyFromObject(pod),
				updated,
			), "get updated pod")
			c.Eq(tt.want, updated.Annotations[metadata.AnnotationDrainState], "drain state")
		})
	}
}

func TestExecuteDrainStateMachineTimeout(t *testing.T) {
	c := assert.NewAborting(t)
	shard, pod, k8sClient := testObjects(t, metadata.DrainStateDraining)
	pod.Annotations[metadata.AnnotationDrainRequestedAt] = time.Now().
		Add(-drain.DrainTimeout - time.Second).
		Format(time.RFC3339)
	c.NoError(k8sClient.Update(context.Background(), pod), "update pod")

	requeue, err := drain.ExecuteDrainStateMachine(context.Background(), k8sClient, nil, shard, pod)
	c.NoError(err, "execute timed out drain")
	c.True(requeue, "expected a requeue after the timeout transition")

	updated := &corev1.Pod{}
	c.NoError(k8sClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(pod),
		updated,
	), "get updated pod")
	c.Eq(
		metadata.DrainStateReadyForDeletion,
		updated.Annotations[metadata.AnnotationDrainState],
		"drain state",
	)
}

func TestExecuteDrainStateMachineNoop(t *testing.T) {
	for _, state := range []string{"", metadata.DrainStateReadyForDeletion} {
		t.Run(state, func(t *testing.T) {
			c := assert.NewAborting(t)
			shard, pod, k8sClient := testObjects(t, state)
			requeue, err := drain.ExecuteDrainStateMachine(
				context.Background(),
				k8sClient,
				nil,
				shard,
				pod,
			)
			c.NoError(err, "execute drain state machine")
			c.False(requeue, "did not expect a requeue")
		})
	}
}

func testObjects(
	t testing.TB,
	state string,
) (*multigresv1alpha1.Shard, *corev1.Pod, client.Client) {
	t.Helper()
	c := assert.NewAborting(t)
	scheme := runtime.NewScheme()
	c.NoError(multigresv1alpha1.AddToScheme(scheme), "add Multigres scheme")
	c.NoError(corev1.AddToScheme(scheme), "add core scheme")

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "cluster"},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pooler-0",
			Namespace:   shard.Namespace,
			Annotations: map[string]string{metadata.AnnotationDrainState: state},
		},
	}
	return shard, pod, fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
}
