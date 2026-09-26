package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

// topoTLSShard returns a shard whose global topology reference carries the
// client credential and CA bundle, i.e. the managed etcd case where one
// cert-manager Secret holds tls.crt, tls.key and ca.crt.
func topoTLSShard() *multigresv1alpha1.Shard {
	shard := newTestShard()
	secret := multigresv1alpha1.TopoClientCertSecretName("test-cluster")
	shard.Spec.GlobalTopoServer = multigresv1alpha1.GlobalTopoServerRef{
		Address:          "test-cluster-global-topo.default.svc:2379",
		RootPath:         "/multigres/global",
		Implementation:   "etcd",
		CASecret:         secret,
		ClientCertSecret: secret,
	}
	return shard
}

func containerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func mountsVolume(c *corev1.Container, volumeName string) bool {
	if c == nil {
		return false
	}
	for _, m := range c.VolumeMounts {
		if m.Name == volumeName {
			return true
		}
	}
	return false
}

func hasArg(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

// The topo client certificate is the only thing separating multipooler from the
// postgres superuser it shares a pod, an IP, the data PVC and the socket
// directory with. This asserts the mount boundary that makes that separation
// real: the credential reaches only the container that speaks to the topology
// server, never a container that also mounts postgres state, and the pod never
// shares a process namespace.
func TestPoolPod_TopoClientTLSMountBoundary(t *testing.T) {
	ck := assert.NewCollecting(t)
	pod, err := BuildPoolPod(topoTLSShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	ck.Require().NoError(err, "BuildPoolPod() error =")

	multipooler := containerByName(pod.Spec.Containers, "multipooler")
	ck.Require().NotNil(multipooler, "multipooler container missing")
	ck.True(
		mountsVolume(multipooler, multigresv1alpha1.TopoClientTLSVolumeName),
		"topo client certificate is not mounted into multipooler",
	)

	// No other container in the pod may mount the credential. pgctld runs as an
	// init (native sidecar) container named postgres; the exporter sits beside
	// multipooler. Neither speaks to the topology server.
	all := append([]corev1.Container{}, pod.Spec.InitContainers...)
	all = append(all, pod.Spec.Containers...)
	for i := range all {
		c := &all[i]
		if c.Name == "multipooler" {
			continue
		}
		if mountsVolume(c, multigresv1alpha1.TopoClientTLSVolumeName) {
			t.Errorf("topo client certificate leaked into container %q", c.Name)
		}
	}

	// The credential must not ride on a volume the postgres or pgctld containers
	// also mount: those share the data PVC and the socket directory, so anything
	// projected onto them is readable by the postgres superuser.
	postgres := containerByName(pod.Spec.InitContainers, "postgres")
	ck.Require().NotNil(postgres, "postgres (pgctld) container missing")
	sharedWithPostgres := map[string]struct{}{}
	for _, m := range postgres.VolumeMounts {
		sharedWithPostgres[m.Name] = struct{}{}
	}
	_, shared := sharedWithPostgres[multigresv1alpha1.TopoClientTLSVolumeName]
	ck.False(shared, "topo client certificate shares a volume with the postgres container")
	for _, m := range multipooler.VolumeMounts {
		if m.Name != multigresv1alpha1.TopoClientTLSVolumeName {
			continue
		}
		// The mount path must be its own, not nested under the shared data or
		// socket directories.
		ck.False(
			m.MountPath == DataMountPath || m.MountPath == SocketDirMountPath,
			"topo client certificate mounted on a shared path %q",
			m.MountPath,
		)
	}

	// Shared PID plus ptrace would expose one container's mounted files through
	// /proc, defeating the boundary regardless of which volume carries them.
	if pod.Spec.ShareProcessNamespace != nil {
		t.Errorf("shareProcessNamespace = %v, want unset", *pod.Spec.ShareProcessNamespace)
	}
}

// With no topo client credential on the reference (topology TLS off), the pool
// pod renders exactly as before: no topo client volume, mount or flags anywhere.
func TestPoolPod_TopoClientTLSOffRendersUnchanged(t *testing.T) {
	ck := assert.NewAborting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	ck.NoError(err, "BuildPoolPod() error =")

	for _, v := range pod.Spec.Volumes {
		ck.NotEq(
			multigresv1alpha1.TopoClientTLSVolumeName,
			v.Name,
			"topo client volume present with topology TLS off",
		)
	}
	all := append([]corev1.Container{}, pod.Spec.InitContainers...)
	all = append(all, pod.Spec.Containers...)
	for i := range all {
		c := &all[i]
		if mountsVolume(c, multigresv1alpha1.TopoClientTLSVolumeName) {
			t.Errorf("container %q mounts the topo client volume with topology TLS off", c.Name)
		}
		if hasArg(c.Args, "--topo-etcd-tls-cert") {
			t.Errorf("container %q has topo TLS flags with topology TLS off", c.Name)
		}
	}
}

// multipooler and multiorch both open topology connections, so both present the
// client certificate through the three etcd TLS flags when it is configured.
func TestTopoSpeakingContainers_PresentClientCert(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := topoTLSShard()
	pool := newTestPoolSpec()

	mp := buildMultipoolerContainer(shard, pool, "main", "z1", "p-test-id")
	orch := buildMultiorchContainer(shard, "z1")

	for name, args := range map[string][]string{
		"multipooler": mp.Args,
		"multiorch":   orch.Args,
	} {
		c.False(!hasArg(args, "--topo-etcd-tls-cert") ||
			!hasArg(args, "--topo-etcd-tls-key") ||
			!hasArg(
				args,
				"--topo-etcd-tls-ca",
			), "%s is missing topo client TLS flags: %v", name, args)
	}
	c.True(
		mountsVolume(&mp, multigresv1alpha1.TopoClientTLSVolumeName),
		"multipooler does not mount the topo client certificate",
	)
	c.True(
		mountsVolume(&orch, multigresv1alpha1.TopoClientTLSVolumeName),
		"multiorch does not mount the topo client certificate",
	)
}
