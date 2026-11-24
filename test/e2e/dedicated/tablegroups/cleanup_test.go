//go:build e2e

package tablegroups_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func (c lifecycleCheck) drop(t *testing.T, id int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.gateway.psql(ctx, "SET statement_timeout = '8min'", "DROP TABLEGROUP lifecycle")
		done <- err
	}()

	// DROP waits for catalog completion. Observe cleanup concurrently so a
	// blocked statement can be diagnosed down to the resource still present.
	var remaining error
	err := wait.PollUntilContextCancel(ctx, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		remaining = c.dropChecks(ctx, id)
		select {
		case err := <-done:
			if err != nil {
				return false, fmt.Errorf("DROP TABLEGROUP lifecycle: %w", err)
			}
			done = nil
		default:
		}
		return done == nil && remaining == nil, nil
	})
	require.NoErrorf(t, err, "drop catalog ID %d; last cleanup checks: %v", id, remaining)
	t.Logf("drop catalog ID %d: catalog row, pods, pooler registrations, provision claim and shard initialization claims removed", id)
}

func (c lifecycleCheck) dropChecks(ctx context.Context, id int64) error {
	var pending []error
	sqlCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	catalog, err := c.gateway.psql(sqlCtx, catalogSQL)
	cancel()
	if err != nil {
		pending = append(pending, fmt.Errorf("read dropped catalog entry: %w", err))
	} else if catalog != "" {
		pending = append(pending, fmt.Errorf("catalog entry remains (ID|state): %s", catalog))
	}

	podCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	var pods corev1.PodList
	err = c.client.List(podCtx, &pods, client.InNamespace(c.namespace), c.labels())
	cancel()
	if err != nil {
		pending = append(pending, fmt.Errorf("list old pods: %w", err))
	} else if len(pods.Items) > 0 {
		names := make([]string, 0, len(pods.Items))
		for _, pod := range pods.Items {
			names = append(names, pod.Name)
		}
		pending = append(pending, fmt.Errorf("old pods remain: %v", names))
	}

	poolerCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	poolers, err := c.poolers(poolerCtx)
	cancel()
	if err != nil {
		pending = append(pending, err)
	}
	for _, pooler := range poolers {
		ref := pooler.GetShardRef()
		if ref.GetDatabase() == database && (ref.GetTableGroup() == tablegroup || pooler.GetTablegroupId() == id) {
			pending = append(pending, fmt.Errorf("old pooler registration remains: %s (%s, ID %d)",
				pooler.GetId().GetName(), ref.GetShard(), pooler.GetTablegroupId()))
		}
	}

	topoCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	keys, err := c.claimKeys(topoCtx)
	cancel()
	if err != nil {
		pending = append(pending, err)
	}
	for _, key := range c.expectedClaims(id) {
		if _, ok := keys[key]; ok {
			pending = append(pending, fmt.Errorf("topology claim remains: %s", key))
		}
	}
	return errors.Join(pending...)
}

func (c lifecycleCheck) poolers(ctx context.Context) ([]*clustermetadatapb.Multipooler, error) {
	data, err := cluster.Clientset.CoreV1().Services(c.namespace).
		ProxyGet("http", c.name+"-multiadmin", "http", "api/v1/poolers", map[string]string{"cells": c.cell}).DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("read Multiadmin pooler registrations for cell %s: %w", c.cell, err)
	}
	var resp multiadminpb.GetPoolersResponse
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("decode Multiadmin pooler registrations: %w", err)
	}
	for _, pooler := range resp.GetPoolers() {
		ref := pooler.GetShardRef()
		if ref.GetDatabase() == database && ref.GetTableGroup() == "default" {
			return resp.GetPoolers(), nil
		}
	}
	return nil, fmt.Errorf("Multiadmin response for cell %s lacks the default pooler registrations", c.cell)
}

func (c lifecycleCheck) topoName() string {
	return c.name + "-global-topo"
}

func (c lifecycleCheck) databaseRoot() string {
	return path.Join(c.topoRoot, topoclient.DatabasesPath, database)
}

func (c lifecycleCheck) expectedClaims(id int64) []string {
	keys := []string{path.Join(c.databaseRoot(), topoclient.TableGroupClaimsDir, strconv.FormatInt(id, 10))}
	for _, shard := range ranges {
		keys = append(keys, path.Join(c.databaseRoot(), tablegroup, shard, topoclient.ShardInitClaimFile))
	}
	return keys
}

func (c lifecycleCheck) claimKeys(ctx context.Context) (map[string][]byte, error) {
	out, err := kubectl(ctx, c.namespace, "exec", "statefulset/"+c.topoName(), "-c", "etcd", "--",
		"etcdctl", "get", c.databaseRoot()+"/", "--prefix", "--write-out=json")
	if err != nil {
		return nil, fmt.Errorf("read global topology claims: %w", err)
	}
	var result struct {
		Header struct{ Revision int64 }
		KVs    []struct{ Key, Value []byte }
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		return nil, fmt.Errorf("decode global topology response: %w", err)
	}
	if result.Header.Revision <= 0 {
		return nil, errors.New("global topology response has no revision")
	}
	keys := make(map[string][]byte)
	for _, entry := range result.KVs {
		keys[string(entry.Key)] = entry.Value
	}
	return keys, nil
}

func (c lifecycleCheck) requireClaims(t *testing.T, id int64) {
	t.Helper()
	waitForCheck(t, fmt.Sprintf("record claims held by catalog ID %d before drop", id), func(ctx context.Context) error {
		keys, err := c.claimKeys(ctx)
		if err != nil {
			return err
		}
		expected := c.expectedClaims(id)
		for _, key := range expected {
			if _, ok := keys[key]; !ok {
				return fmt.Errorf("claim %s missing from global topology", key)
			}
		}
		var claim clustermetadatapb.TableGroupProvisionClaim
		if err := proto.Unmarshal(keys[expected[0]], &claim); err != nil {
			return fmt.Errorf("decode provision claim %s: %w", expected[0], err)
		}
		owner := fmt.Sprintf("multigres-operator/%s/%s", c.namespace, c.name)
		if claim.GetProvisioner() != owner || claim.GetTablegroupId() != id {
			return fmt.Errorf("expected provision claim for %s and ID %d, got %s", owner, id, &claim)
		}
		return nil
	})
}

type storageBinding struct {
	uid    types.UID
	volume string
}

func (c lifecycleCheck) waitForStorage(t *testing.T, id int64) map[string]storageBinding {
	t.Helper()
	labels := c.labels()
	labels[metadata.LabelMultigresPool] = "default"
	var bindings map[string]storageBinding
	waitForCheck(t, fmt.Sprintf("bound pool storage for catalog ID %d", id), func(ctx context.Context) error {
		var pods corev1.PodList
		if err := c.client.List(ctx, &pods, client.InNamespace(c.namespace), labels); err != nil {
			return err
		}
		replicas := make(map[string]int)
		bindings = make(map[string]storageBinding)
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				return fmt.Errorf("pool pod %s is terminating", pod.Name)
			}
			replicas[pod.Labels[metadata.LabelMultigresShard]]++
			hasDataVolume := false
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim == nil {
					continue
				}
				if volume.Name == "pgdata" {
					hasDataVolume = true
				}
				claim := volume.PersistentVolumeClaim.ClaimName
				binding, err := c.storage(ctx, claim)
				if err != nil {
					return fmt.Errorf("pool pod %s: %w", pod.Name, err)
				}
				bindings[claim] = binding
			}
			if !hasDataVolume {
				return fmt.Errorf("pool pod %s has no PVC-backed PostgreSQL data volume", pod.Name)
			}
		}
		for _, shard := range ranges {
			// Each dynamic shard inherits the base fixture's three default pool replicas.
			if replicas[shard] != 3 {
				return fmt.Errorf("shard %s: expected three pool pods, observed %d", shard, replicas[shard])
			}
		}
		return nil
	})
	t.Logf("catalog ID %d pool PVCs: %v", id, bindings)
	return bindings
}

func (c lifecycleCheck) storage(ctx context.Context, name string) (storageBinding, error) {
	var claim corev1.PersistentVolumeClaim
	if err := c.client.Get(ctx, client.ObjectKey{Namespace: c.namespace, Name: name}, &claim); err != nil {
		return storageBinding{}, err
	}
	if claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName == "" || claim.DeletionTimestamp != nil {
		return storageBinding{}, fmt.Errorf("PVC %s is not bound to a PV or is terminating", name)
	}
	return storageBinding{claim.UID, claim.Spec.VolumeName}, nil
}

func (c lifecycleCheck) requireRetainedStorage(t *testing.T, expected map[string]storageBinding) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()
	for claim, before := range expected {
		after, err := c.storage(ctx, claim)
		require.NoError(t, err, "retained PVC %s", claim)
		require.Equal(t, before, after, "retained PVC %s changed identity or PV", claim)
	}
}
