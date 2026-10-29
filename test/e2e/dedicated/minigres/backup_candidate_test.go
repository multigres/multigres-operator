//go:build e2e && minigres_candidate

package minigres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	multiadmin "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// This explicit build tag keeps the unmerged upstream backup candidate separate
// from qualification of the promoted runtime, which does not support it yet.
func TestMinigresBackupAndRestore(t *testing.T) {
	require.NotEmpty(
		t,
		os.Getenv("E2E_MULTIPOOLER_IMAGE"),
		"set an explicit backup-capable runtime candidate",
	)
	require.Equal(
		t,
		os.Getenv("E2E_MULTIPOOLER_IMAGE"),
		os.Getenv("E2E_MULTIADMIN_IMAGE"),
		"Minigres and multiadmin must use the same candidate",
	)
	require.NotEmpty(t, os.Getenv("E2E_POSTGRES_IMAGE"), "set the matching pgctld candidate")
	cluster.EnsureTopoTLSIssuer(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	require.NoError(t, err)
	cr := framework.MustLoadCluster("test/e2e/dedicated/minigres/testdata/cluster.yaml", ns)
	cr.Spec.Multiadmin.Spec.Replicas = ptr.To(int32(1))
	cr.Spec.GlobalTopoServer.External.Endpoints = []multigresv1alpha1.EndpointUrl{
		multigresv1alpha1.EndpointUrl("https://external-etcd." + ns + ".svc:2379"),
	}
	require.NoError(t, framework.LoadImages(t.Context(), cluster.Name, []string{
		string(cr.Spec.Images.Multipooler),
		string(cr.Spec.Images.Postgres),
		string(cr.Spec.Images.Multiadmin),
	}))
	t.Logf(
		"backup candidate: minigres=%s multiadmin=%s pgctld=%s",
		cr.Spec.Images.Multipooler,
		cr.Spec.Images.Multiadmin,
		cr.Spec.Images.Postgres,
	)
	// kubectl receives only this test cluster's kubeconfig and namespace.
	applyManifest(t, ns, strings.ReplaceAll(etcdManifest, "NAMESPACE", ns))
	require.NoError(t, c.Create(t.Context(), cr))
	pod := waitPod(t, ns, "lite", "")
	sqlEventually(
		t,
		ns,
		pod.Name,
		"CREATE TABLE minigres_e2e (id integer PRIMARY KEY, payload text NOT NULL)",
		"CREATE TABLE",
	)
	sqlMust(
		t,
		ns,
		pod.Name,
		"INSERT INTO minigres_e2e VALUES (1, 'restore me'), (2, 'same repository')",
		"INSERT 0 2",
	)
	data := dataPVC(t, ns, pod)
	backupPVC := podClaim(t, pod, "backup-data")

	// Two instances share etcd and the same logical database/shard names, but
	// must have disjoint topology roots and separate filesystem repositories.
	other := framework.MustLoadCluster("test/e2e/dedicated/minigres/testdata/cluster.yaml", ns)
	other.Name = "isolated"
	other.Spec.GlobalTopoServer.External.Endpoints = cr.Spec.GlobalTopoServer.External.Endpoints
	other.Spec.Multiadmin.Spec.Replicas = ptr.To(int32(0))
	require.NoError(t, c.Create(t.Context(), other))
	otherPod := waitPod(t, ns, "isolated", "")
	require.NotEqual(t, backupPVC, podClaim(t, otherPod, "backup-data"))
	require.NotEqual(t, resolvedRoot(t, c, ns, "lite"), resolvedRoot(t, c, ns, "isolated"))
	t.Log("two Lite instances have disjoint topology roots and backup PVCs")

	adminPod := waitComponentPod(t, ns, "lite", "multiadmin", "")
	api, closeAdmin := backupClient(t, ns, adminPod.Name)
	// A request for an absent shard must fail before reaching a backup repository.
	_, err = api.GetBackups(
		t.Context(),
		&multiadmin.GetBackupsRequest{Database: "postgres", TableGroup: "default", Shard: "0-80"},
	)
	require.Error(t, err, "must not return default/0-inf backups for a different shard")
	firstJob, firstBackup := takeBackup(t, api)
	require.NotEmpty(t, firstJob)
	sqlMust(t, ns, pod.Name, "INSERT INTO minigres_e2e VALUES (3, 'latest backup')", "INSERT 0 1")
	want := fingerprint(t, ns, pod.Name)
	jobID, latestBackup := takeBackup(t, api)
	require.NotEqual(t, firstBackup, latestBackup)
	require.Contains(t, backupIDs(t, api), firstBackup)
	require.Contains(t, backupIDs(t, api), latestBackup)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	verified, err := api.VerifyBackups(
		ctx,
		&multiadmin.VerifyBackupsRequest{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
	)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, verified)
	t.Logf(
		"normal backups completed without ForcePrimary: first=%s latest=%s; verify passed",
		firstBackup,
		latestBackup,
	)

	// Drop the in-memory tracker and force job lookup through the static pooler.
	closeAdmin()
	require.NoError(
		t,
		cluster.Clientset.CoreV1().
			Pods(ns).
			Delete(t.Context(), adminPod.Name, metav1.DeleteOptions{}),
	)
	adminPod = waitComponentPod(t, ns, "lite", "multiadmin", adminPod.UID)
	api, closeAdmin = backupClient(t, ns, adminPod.Name)
	defer closeAdmin()
	status, err := api.GetBackupJobStatus(
		t.Context(),
		&multiadmin.GetBackupJobStatusRequest{
			JobId:      jobID,
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
		},
	)
	require.NoError(t, err)
	require.Equal(t, multiadmin.JobStatus_JOB_STATUS_COMPLETED, status.Status)
	require.Equal(t, latestBackup, status.BackupId)
	t.Log("backup job status recovered after multiadmin replacement")
	ctx, cancel = context.WithTimeout(t.Context(), 3*time.Minute)
	expired, err := api.ExpireBackups(ctx, &multiadmin.ExpireBackupsRequest{
		Database:   "postgres",
		TableGroup: "default",
		Shard:      "0-inf",
		Overrides: map[string]string{
			"repo1-retention-full":      "1",
			"repo1-retention-full-type": "count",
		},
	})
	cancel()
	require.NoError(t, err)
	require.Contains(t, expired.ExpiredBackupIds, firstBackup)
	ids := backupIDs(t, api)
	require.NotContains(t, ids, firstBackup)
	require.Contains(t, ids, latestBackup)
	t.Log("expiration removed the older backup and retained the latest")

	eraseTestPostgresData(t, ns, pod, data, string(cr.Spec.Images.Postgres))
	replacement := waitPod(t, ns, "lite", pod.UID)
	sqlEventually(t, ns, replacement.Name, fingerprintQuery, want)
	restoredData := dataPVC(t, ns, replacement)
	require.Equal(t, data.UID, restoredData.UID)
	require.Equal(t, data.Spec.VolumeName, restoredData.Spec.VolumeName)
	require.Equal(t, backupPVC, podClaim(t, replacement, "backup-data"))
	// The second instance must remain untouched by expiration and restore.
	require.Equal(t, otherPod.UID, getPod(t, ns, otherPod.Name).UID)
	require.True(t, ready(getPod(t, ns, otherPod.Name)))
	sqlMust(
		t,
		ns,
		replacement.Name,
		"INSERT INTO minigres_e2e VALUES (4, 'writable after restore')",
		"INSERT 0 1",
	)
	t.Logf(
		"restore recovered fingerprint=%s from backup=%s into original PVC=%s; new writes succeeded",
		want,
		latestBackup,
		data.UID,
	)
}

func takeBackup(t *testing.T, api multiadmin.MultiadminServiceClient) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	response, err := api.Backup(
		ctx,
		&multiadmin.BackupRequest{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
			Type:       "full",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, response.JobId)
	var backupID string
	eventually(t, "backup job completion", func() (bool, error) {
		status, err := api.GetBackupJobStatus(
			ctx,
			&multiadmin.GetBackupJobStatusRequest{
				JobId:      response.JobId,
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "0-inf",
			},
		)
		if err != nil {
			return false, err
		}
		require.NotEqual(
			t,
			multiadmin.JobStatus_JOB_STATUS_FAILED,
			status.Status,
			status.ErrorMessage,
		)
		backupID = status.BackupId
		return status.Status == multiadmin.JobStatus_JOB_STATUS_COMPLETED, nil
	})
	require.NotEmpty(t, backupID)
	return response.JobId, backupID
}

func backupIDs(t *testing.T, api multiadmin.MultiadminServiceClient) []string {
	t.Helper()
	response, err := api.GetBackups(
		t.Context(),
		&multiadmin.GetBackupsRequest{
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
			Limit:      100,
		},
	)
	require.NoError(t, err)
	ids := make([]string, 0, len(response.Backups))
	for _, backup := range response.Backups {
		ids = append(ids, backup.BackupId)
	}
	return ids
}

func backupClient(t *testing.T, ns, pod string) (multiadmin.MultiadminServiceClient, func()) {
	t.Helper()
	address, stop := forward(t, ns, pod, 18070)
	secret, err := cluster.Clientset.CoreV1().
		Secrets(ns).
		Get(t.Context(), "multigres-operator.lite."+ns+".multigres.internal", metav1.GetOptions{})
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(secret.Data["ca.crt"]))
	cert, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
	require.NoError(t, err)
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12,
			ServerName: "multiadmin.lite." + ns + ".multigres.internal",
		})),
	)
	require.NoError(t, err)
	cleanup := func() { _ = conn.Close(); stop() }
	t.Cleanup(cleanup)
	return multiadmin.NewMultiadminServiceClient(conn), cleanup
}

func waitComponentPod(t *testing.T, ns, name, container string, previous types.UID) *corev1.Pod {
	t.Helper()
	var found *corev1.Pod
	eventually(t, "ready "+container+" pod", func() (bool, error) {
		pods, err := cluster.Clientset.CoreV1().
			Pods(ns).
			List(t.Context(), metav1.ListOptions{LabelSelector: "multigres.com/cluster=" + name})
		if err != nil {
			return false, err
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.UID == previous || !ready(p) {
				continue
			}
			for _, c := range p.Spec.Containers {
				if c.Name == container {
					found = p
					return true, nil
				}
			}
		}
		return false, nil
	})
	return found
}

func resolvedRoot(t *testing.T, c client.Client, ns, name string) string {
	t.Helper()
	shards := &multigresv1alpha1.ShardList{}
	require.NoError(
		t,
		c.List(
			t.Context(),
			shards,
			client.InNamespace(ns),
			client.MatchingLabels{"multigres.com/cluster": name},
		),
	)
	require.Len(t, shards.Items, 1)
	return shards.Items[0].Spec.GlobalTopoServer.RootPath
}

func podClaim(t *testing.T, pod *corev1.Pod, volume string) string {
	t.Helper()
	for _, v := range pod.Spec.Volumes {
		if v.Name == volume && v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	t.Fatalf("pod %s has no PVC for %s", pod.Name, volume)
	return ""
}

func eraseTestPostgresData(
	t *testing.T,
	ns string,
	original *corev1.Pod,
	pvc *corev1.PersistentVolumeClaim,
	image string,
) {
	t.Helper()
	require.Equal(
		t,
		"e2e-minigres",
		cluster.Name,
		"destructive restore is restricted to the dedicated test cluster",
	)
	require.True(t, strings.HasPrefix(ns, "e2e-ns-"))
	require.Equal(t, ns, original.Namespace)
	require.Equal(t, pvc.UID, dataPVC(t, ns, original).UID)
	// Stop reconciliation before deleting the pod so pgctld cannot race the erase.
	_, err := kubectl(
		"multigres-operator",
		"scale",
		"deployment/multigres-operator-controller-manager",
		"--replicas=0",
	)
	require.NoError(t, err)
	resume := func() {
		_, err := kubectl(
			"multigres-operator",
			"scale",
			"deployment/multigres-operator-controller-manager",
			"--replicas=1",
		)
		require.NoError(t, err)
	}
	t.Cleanup(resume)
	_, err = kubectl(
		"multigres-operator",
		"wait",
		"--for=delete",
		"pod",
		"-l",
		"control-plane=controller-manager",
		"--timeout=90s",
	)
	require.NoError(t, err)
	require.NoError(
		t,
		cluster.Clientset.CoreV1().
			Pods(ns).
			Delete(t.Context(), original.Name, metav1.DeleteOptions{}),
	)
	eventually(t, "original Lite pod termination", func() (bool, error) {
		_, err := cluster.Clientset.CoreV1().
			Pods(ns).
			Get(t.Context(), original.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), errUnlessNotFound(err)
	})
	helper := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "erase-postgres-data", Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: original.Spec.SecurityContext.DeepCopy(),
			Containers: []corev1.Container{
				{
					Name:  "erase",
					Image: image,
					Command: []string{
						"/bin/sh",
						"-ec",
						"test -s /data/pg_data/PG_VERSION; rm -rf /data/pg_data; test ! -e /data/pg_data",
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
		},
	}
	_, err = cluster.Clientset.CoreV1().Pods(ns).Create(t.Context(), helper, metav1.CreateOptions{})
	require.NoError(t, err)
	eventually(t, "test PGDATA removal", func() (bool, error) {
		p, err := cluster.Clientset.CoreV1().
			Pods(ns).
			Get(t.Context(), helper.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		require.NotEqual(
			t,
			corev1.PodFailed,
			p.Status.Phase,
			fmt.Sprintf("erase helper failed: %v", p.Status.ContainerStatuses),
		)
		return p.Status.Phase == corev1.PodSucceeded, nil
	})
	require.NoError(
		t,
		cluster.Clientset.CoreV1().
			Pods(ns).
			Delete(t.Context(), helper.Name, metav1.DeleteOptions{}),
	)
	eventually(t, "erase helper termination", func() (bool, error) {
		_, err := cluster.Clientset.CoreV1().
			Pods(ns).
			Get(t.Context(), helper.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err), errUnlessNotFound(err)
	})
	t.Log(
		"test PGDATA removed while Minigres and pgctld were stopped; backup PVC was never mounted by erase helper",
	)
	resume()
}

func errUnlessNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
