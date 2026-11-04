package multigrescluster

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// s3TLS is how one component is told the S3 endpoint's certificate is
// verified: the CA file's path, or that verification is off.
type s3TLS struct {
	CAFile string
	Skip   bool
}

// recordedS3TLS reads the two settings from a topology record.
func recordedS3TLS(t *testing.T, p provisioned) s3TLS {
	t.Helper()
	s3 := p.topology.GetBackupLocation().GetS3()
	require.NotNil(t, s3)
	require.Empty(t, s3.ProtoReflect().GetUnknown())
	return s3TLS{CAFile: s3.GetCaFile(), Skip: s3.GetSkipTlsVerify()}
}

// sidecarS3TLS reads the two settings from the sidecar's arguments.
func sidecarS3TLS(t *testing.T, pod *corev1.Pod) s3TLS {
	t.Helper()
	var got s3TLS
	for _, c := range pod.Spec.InitContainers {
		if c.Name != shard.PgBackRestSidecarContainerName {
			continue
		}
		for _, arg := range c.Args {
			switch {
			case arg == "--s3-skip-tls-verify":
				got.Skip = true
			case len(arg) > len("--s3-ca-file=") && arg[:len("--s3-ca-file=")] == "--s3-ca-file=":
				got.CAFile = arg[len("--s3-ca-file="):]
			}
		}
	}
	return got
}

// caMounts returns the names of the containers that mount the CA volume, and
// whether every such mount is read-only at the directory of the CA file.
func caMounts(pod *corev1.Pod) (containers []string, readOnlyAtPath bool) {
	readOnlyAtPath = true
	all := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	for _, c := range all {
		for _, m := range c.VolumeMounts {
			if m.Name != shard.PgBackRestS3CAVolumeName {
				continue
			}
			containers = append(containers, c.Name)
			if !m.ReadOnly || m.SubPath != "" ||
				m.MountPath != path.Dir(multigresv1alpha1.S3CAFilePath) {
				readOnlyAtPath = false
			}
		}
	}
	return containers, readOnlyAtPath
}

func caVolume(pod *corev1.Pod) *corev1.Volume {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == shard.PgBackRestS3CAVolumeName {
			return &pod.Spec.Volumes[i]
		}
	}
	return nil
}

// A cluster's S3 block says how the endpoint's TLS certificate is verified:
// with a CA bundle from a Secret or a ConfigMap, not at all, or with the
// system trust store. The setting has to reach the pgBackRest sidecar, which
// is the only container that talks to S3, and the database's topology record
// has to say the same, because multipooler compares the two and refuses a
// sidecar that differs. The bundle is mounted read-only into the sidecar and
// into no other container.
func TestSidecarS3TLSMatchesTopology(t *testing.T) {
	for name, tc := range map[string]struct {
		s3   func(*multigresv1alpha1.S3BackupConfig)
		want s3TLS
		// source is the projection the CA volume must have.
		source *corev1.VolumeProjection
	}{
		"system trust store": {s3: func(*multigresv1alpha1.S3BackupConfig) {}},
		"verifyTLS true is the default": {
			s3: func(c *multigresv1alpha1.S3BackupConfig) { c.VerifyTLS = ptr.To(true) },
		},
		"a CA bundle in a Secret": {
			s3: func(c *multigresv1alpha1.S3BackupConfig) {
				c.CA = &multigresv1alpha1.S3CAReference{
					Secret: &multigresv1alpha1.S3CAKeyReference{Name: "minio-ca"},
				}
			},
			want: s3TLS{CAFile: multigresv1alpha1.S3CAFilePath},
			source: &corev1.VolumeProjection{Secret: &corev1.SecretProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "minio-ca"},
				Items:                []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
			}},
		},
		// A self-signed server certificate is its own CA, and is usually
		// published under the key it was issued with.
		"a self-signed certificate in a ConfigMap, under its own key": {
			s3: func(c *multigresv1alpha1.S3BackupConfig) {
				c.CA = &multigresv1alpha1.S3CAReference{
					ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "minio-cert", Key: "tls.crt"},
				}
			},
			want: s3TLS{CAFile: multigresv1alpha1.S3CAFilePath},
			source: &corev1.VolumeProjection{ConfigMap: &corev1.ConfigMapProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "minio-cert"},
				Items:                []corev1.KeyToPath{{Key: "tls.crt", Path: "ca.crt"}},
			}},
		},
		"verification off": {
			s3:   func(c *multigresv1alpha1.S3BackupConfig) { c.VerifyTLS = ptr.To(false) },
			want: s3TLS{Skip: true},
		},
	} {
		for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
			t.Run(name+"/mode "+mode, func(t *testing.T) {
				p := provision(t, sidecarRepositoryCluster("uid-a",
					func(c *multigresv1alpha1.MultigresCluster) {
						c.Spec.Mode = mode
						c.Spec.Backup.S3.Endpoint = "https://minio.internal:9000"
						tc.s3(c.Spec.Backup.S3)
					}))
				require.NoError(t, p.podErr)

				assert.Equal(t, tc.want, sidecarS3TLS(t, p.pod), "the sidecar's arguments")
				if mode != multigresv1alpha1.ModeLite {
					assert.Equal(t, tc.want, recordedS3TLS(t, p),
						"the topology record, which multipooler compares with the sidecar")
				}

				containers, readOnly := caMounts(p.pod)
				volume := caVolume(p.pod)
				if tc.source == nil {
					assert.Empty(t, containers)
					assert.Nil(t, volume)
					return
				}
				assert.Equal(t, []string{shard.PgBackRestSidecarContainerName}, containers,
					"only the sidecar reads the CA bundle")
				assert.True(t, readOnly, "mounted read-only, whole, at the CA file's directory")
				require.NotNil(t, volume)
				require.NotNil(t, volume.Projected)
				assert.Equal(t, []corev1.VolumeProjection{*tc.source}, volume.Projected.Sources)
			})
		}
	}
}

// Naming another object or key for the CA changes the pod template, so the
// pods are replaced and start with the new bundle. Naming the same one does
// not. A bundle that changes inside the same object is not a template
// change: the volume is mounted whole, so the kubelet replaces the file.
func TestSidecarS3CAReferenceIsPartOfThePodTemplate(t *testing.T) {
	hash := func(ca *multigresv1alpha1.S3CAReference) string {
		p := provision(t, sidecarRepositoryCluster("uid-a",
			func(c *multigresv1alpha1.MultigresCluster) { c.Spec.Backup.S3.CA = ca }))
		require.NoError(t, p.podErr)
		require.NotEmpty(t, p.pod.Annotations[metadata.AnnotationSpecHash])
		return p.pod.Annotations[metadata.AnnotationSpecHash]
	}
	secret := func(name, key string) *multigresv1alpha1.S3CAReference {
		return &multigresv1alpha1.S3CAReference{
			Secret: &multigresv1alpha1.S3CAKeyReference{Name: name, Key: key},
		}
	}
	base := hash(secret("ca", ""))
	assert.Equal(t, base, hash(secret("ca", "")))
	assert.Equal(t, base, hash(secret("ca", "ca.crt")), "the default key, written out")
	assert.NotEqual(t, base, hash(nil))
	assert.NotEqual(t, base, hash(secret("other-ca", "")))
	assert.NotEqual(t, base, hash(secret("ca", "bundle.pem")))
	assert.NotEqual(t, base, hash(&multigresv1alpha1.S3CAReference{
		ConfigMap: &multigresv1alpha1.S3CAKeyReference{Name: "ca"},
	}))
}

// A location that does not verify its endpoint is reported on the cluster
// for as long as it is configured that way, and not otherwise.
func TestS3TLSConditionFollowsTheSpec(t *testing.T) {
	cluster := sidecarRepositoryCluster("uid-a")
	cluster.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{Name: "postgres", Default: true}}
	syncS3TLSCondition(cluster)
	require.Nil(t, meta.FindStatusCondition(
		cluster.Status.Conditions, multigresv1alpha1.ConditionS3TLSVerified))

	cluster.Spec.Backup.S3.VerifyTLS = ptr.To(false)
	syncS3TLSCondition(cluster)
	cond := meta.FindStatusCondition(
		cluster.Status.Conditions, multigresv1alpha1.ConditionS3TLSVerified)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, multigresv1alpha1.ReasonS3TLSVerificationDisabled, cond.Reason)
	assert.Contains(t, cond.Message, `database "postgres"`)

	cluster.Spec.Backup.S3.VerifyTLS = ptr.To(true)
	syncS3TLSCondition(cluster)
	require.Nil(t, meta.FindStatusCondition(
		cluster.Status.Conditions, multigresv1alpha1.ConditionS3TLSVerified))
}
