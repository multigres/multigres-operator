package toposerver

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
)

func TestBuildPodIdentityEnv(t *testing.T) {
	got := buildPodIdentityEnv()

	want := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("buildPodIdentityEnv() mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildEtcdConfigEnv(t *testing.T) {
	tests := map[string]struct {
		toposerverName string
		serviceName    string
		namespace      string
		want           []corev1.EnvVar
	}{
		"basic configuration": {
			toposerverName: "my-toposerver",
			serviceName:    "my-toposerver-headless",
			namespace:      "default",
			want: []corev1.EnvVar{
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).my-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).my-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "my-toposerver"},
			},
		},
		"different namespace": {
			toposerverName: "test-toposerver",
			serviceName:    "test-toposerver-headless",
			namespace:      "production",
			want: []corev1.EnvVar{
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "test-toposerver"},
			},
		},
		"long names": {
			toposerverName: "very-long-toposerver-cluster-name",
			serviceName:    "very-long-toposerver-cluster-name-headless",
			namespace:      "kube-system",
			want: []corev1.EnvVar{
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).very-long-toposerver-cluster-name-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).very-long-toposerver-cluster-name-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "very-long-toposerver-cluster-name"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildEtcdConfigEnv(tc.toposerverName, tc.serviceName, tc.namespace, false)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("buildEtcdConfigEnv() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildEtcdClusterPeerList(t *testing.T) {
	tests := map[string]struct {
		toposerverName string
		serviceName    string
		namespace      string
		replicas       int32
		want           string
	}{
		"single replica": {
			toposerverName: "my-toposerver",
			serviceName:    "my-toposerver-headless",
			namespace:      "default",
			replicas:       1,
			want:           "my-toposerver-0=http://my-toposerver-0.my-toposerver-headless.default.svc.cluster.local:2380",
		},
		"three replicas (typical HA)": {
			toposerverName: "my-toposerver",
			serviceName:    "my-toposerver-headless",
			namespace:      "default",
			replicas:       3,
			want:           "my-toposerver-0=http://my-toposerver-0.my-toposerver-headless.default.svc.cluster.local:2380,my-toposerver-1=http://my-toposerver-1.my-toposerver-headless.default.svc.cluster.local:2380,my-toposerver-2=http://my-toposerver-2.my-toposerver-headless.default.svc.cluster.local:2380",
		},
		"five replicas": {
			toposerverName: "toposerver-prod",
			serviceName:    "toposerver-prod-headless",
			namespace:      "production",
			replicas:       5,
			want:           "toposerver-prod-0=http://toposerver-prod-0.toposerver-prod-headless.production.svc.cluster.local:2380,toposerver-prod-1=http://toposerver-prod-1.toposerver-prod-headless.production.svc.cluster.local:2380,toposerver-prod-2=http://toposerver-prod-2.toposerver-prod-headless.production.svc.cluster.local:2380,toposerver-prod-3=http://toposerver-prod-3.toposerver-prod-headless.production.svc.cluster.local:2380,toposerver-prod-4=http://toposerver-prod-4.toposerver-prod-headless.production.svc.cluster.local:2380",
		},
		"zero replicas": {
			toposerverName: "my-toposerver",
			serviceName:    "my-toposerver-headless",
			namespace:      "default",
			replicas:       0,
			want:           "",
		},
		"negative replicas": {
			toposerverName: "my-toposerver",
			serviceName:    "my-toposerver-headless",
			namespace:      "default",
			replicas:       -1,
			want:           "",
		},
		"different namespace": {
			toposerverName: "kube-toposerver",
			serviceName:    "kube-toposerver-headless",
			namespace:      "kube-system",
			replicas:       3,
			want:           "kube-toposerver-0=http://kube-toposerver-0.kube-toposerver-headless.kube-system.svc.cluster.local:2380,kube-toposerver-1=http://kube-toposerver-1.kube-toposerver-headless.kube-system.svc.cluster.local:2380,kube-toposerver-2=http://kube-toposerver-2.kube-toposerver-headless.kube-system.svc.cluster.local:2380",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildEtcdClusterPeerList(
				tc.toposerverName,
				tc.serviceName,
				tc.namespace,
				tc.replicas,
				"http",
			)
			if got != tc.want {
				t.Errorf("buildEtcdClusterPeerList() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildContainerEnv(t *testing.T) {
	tests := map[string]struct {
		toposerverName string
		namespace      string
		replicas       int32
		serviceName    string
		want           []corev1.EnvVar
	}{
		"complete environment with 3 replicas": {
			toposerverName: "my-toposerver",
			namespace:      "default",
			replicas:       3,
			serviceName:    "my-toposerver-headless",
			want: []corev1.EnvVar{
				{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
				{
					Name: "POD_NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).my-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).my-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "my-toposerver"},
				{
					Name:  "ETCD_INITIAL_CLUSTER",
					Value: "my-toposerver-0=http://my-toposerver-0.my-toposerver-headless.default.svc.cluster.local:2380,my-toposerver-1=http://my-toposerver-1.my-toposerver-headless.default.svc.cluster.local:2380,my-toposerver-2=http://my-toposerver-2.my-toposerver-headless.default.svc.cluster.local:2380",
				},
			},
		},
		"single replica": {
			toposerverName: "test-toposerver",
			namespace:      "test",
			replicas:       1,
			serviceName:    "test-toposerver-headless",
			want: []corev1.EnvVar{
				{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
				{
					Name: "POD_NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				// Cluster setup won't happen in a single cluster, and these
				// env variables are only used at startup.
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "test-toposerver"},
				{
					Name:  "ETCD_INITIAL_CLUSTER",
					Value: "test-toposerver-0=http://test-toposerver-0.test-toposerver-headless.test.svc.cluster.local:2380",
				},
			},
		},
		"zero replicas - no ETCD_INITIAL_CLUSTER": {
			toposerverName: "empty-toposerver",
			namespace:      "default",
			replicas:       0,
			serviceName:    "empty-toposerver-headless",
			want: []corev1.EnvVar{
				{
					Name: "POD_NAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
				{
					Name: "POD_NAMESPACE",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.namespace",
						},
					},
				},
				{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
				{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
				{Name: "ETCD_LISTEN_CLIENT_URLS", Value: "http://[::]:2379"},
				{Name: "ETCD_LISTEN_PEER_URLS", Value: "http://[::]:2380"},
				{Name: "ETCD_LISTEN_METRICS_URLS", Value: "http://[::]:2381"},
				{
					Name:  "ETCD_ADVERTISE_CLIENT_URLS",
					Value: "http://$(POD_NAME).empty-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
				},
				{
					Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
					Value: "http://$(POD_NAME).empty-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
				},
				// Cluster setup won't happen in a single cluster, and these
				// env variables are only used at startup. In case of scaling up
				// from zero replica, the updated env variable will be picked up
				// correctly, and thus an empty variable like this will be OK.
				{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
				{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: "empty-toposerver"},
				{Name: "ETCD_INITIAL_CLUSTER"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildContainerEnv(
				tc.toposerverName,
				tc.namespace,
				tc.replicas,
				tc.serviceName,
				false,
			)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildContainerEnv() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
