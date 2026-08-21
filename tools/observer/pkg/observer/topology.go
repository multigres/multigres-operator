package observer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/topology"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

func (o *Observer) checkTopology(ctx context.Context) {
	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err != nil {
		o.logger.Error("failed to list MultigresClusters for topology checks", "error", err)
		return
	}

	topoData := make([]map[string]any, 0, len(clusters.Items))
	for i := range clusters.Items {
		cluster := &clusters.Items[i]

		etcdAddr := o.findEtcdAddress(ctx, cluster)
		if etcdAddr == "" {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "topology",
				Component: "cluster/" + cluster.Name,
				Message:   "topology validation skipped: etcd unreachable",
			})
			topoData = append(topoData, map[string]any{
				"cluster": cluster.Name, "etcdReachable": false,
			})
			continue
		}

		rootPath, err := o.globalTopologyRoot(ctx, cluster)
		if err != nil {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "topology",
				Component: "cluster/" + cluster.Name,
				Message:   "topology validation skipped: root resolution failed: " + err.Error(),
			})
			continue
		}

		o.checkCellRegistration(ctx, cluster, etcdAddr, rootPath)
		o.checkPoolerRegistration(ctx, cluster, etcdAddr, rootPath)
		topoData = append(topoData, map[string]any{
			"cluster": cluster.Name, "etcdReachable": true, "rootPath": rootPath,
		})
	}

	if o.probes != nil {
		o.probes.Set("topology", map[string]any{"clusters": topoData})
	}
}

func (o *Observer) globalTopologyRoot(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (string, error) {
	roots, err := topology.NewRoots(cluster.Annotations, cluster.Namespace, cluster.Name)
	if err != nil {
		return "", err
	}
	rootPath := roots.Global()

	var templateName multigresv1alpha1.TemplateRef
	if spec := cluster.Spec.GlobalTopoServer; spec != nil {
		if spec.External != nil {
			if spec.External.RootPath != "" {
				return spec.External.RootPath, nil
			}
			return rootPath, nil
		}
		templateName = spec.TemplateRef
	}
	if templateName == "" {
		templateName = cluster.Spec.TemplateDefaults.CoreTemplate
	}

	// Match resolver.ResolveCoreTemplate: an empty reference uses the optional
	// system "default" template, while an explicitly named missing template is
	// an error.
	implicitDefault := templateName == "" || templateName == "default"
	if templateName == "" {
		templateName = "default"
	}
	template := &multigresv1alpha1.CoreTemplate{}
	err = o.client.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      string(templateName),
	}, template)
	switch {
	case err == nil:
		if template.Spec.GlobalTopoServer != nil &&
			template.Spec.GlobalTopoServer.Etcd != nil &&
			template.Spec.GlobalTopoServer.Etcd.RootPath != "" {
			rootPath = template.Spec.GlobalTopoServer.Etcd.RootPath
		}
	case apierrors.IsNotFound(err) && implicitDefault:
		// The default template is optional; canonical defaults apply when absent.
	case err != nil:
		return "", fmt.Errorf("resolving CoreTemplate %q: %w", templateName, err)
	}

	if spec := cluster.Spec.GlobalTopoServer; spec != nil &&
		spec.Etcd != nil && spec.Etcd.RootPath != "" {
		rootPath = spec.Etcd.RootPath
	}
	return rootPath, nil
}

func (o *Observer) findEtcdAddress(ctx context.Context, cluster *multigresv1alpha1.MultigresCluster) string {
	// Check external etcd endpoints first.
	if cluster.Spec.GlobalTopoServer != nil &&
		cluster.Spec.GlobalTopoServer.External != nil &&
		len(cluster.Spec.GlobalTopoServer.External.Endpoints) > 0 {
		for _, ep := range cluster.Spec.GlobalTopoServer.External.Endpoints {
			addr := strings.TrimRight(string(ep), "/")
			if o.checkEtcdHealth(addr) {
				return addr
			}
		}
		return ""
	}

	// Fall through to managed etcd service lookup.
	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy:     common.ManagedByMultigres,
			common.LabelAppComponent:     common.ComponentGlobalTopo,
			common.LabelMultigresCluster: cluster.Name,
		})...,
	); err != nil || len(svcs.Items) == 0 {
		return ""
	}

	svc := &svcs.Items[0]
	addr := fmt.Sprintf("http://%s.%s.svc:%d", svc.Name, svc.Namespace, common.PortEtcdClient)
	if o.checkEtcdHealth(addr) {
		return addr
	}
	return ""
}

// checkEtcdHealth verifies etcd is reachable at the given address.
func (o *Observer) checkEtcdHealth(addr string) bool {
	resp, err := o.httpClient.Get(addr + "/health")
	if err != nil {
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	return resp.StatusCode == http.StatusOK
}

// etcdRangeResponse is the minimal JSON response from the etcd v3 gRPC-gateway range API.
type etcdRangeResponse struct {
	Kvs []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"kvs"`
	Count string `json:"count"`
}

// etcdRange queries etcd via the v3 gRPC-gateway REST API for keys with the given prefix.
func (o *Observer) etcdRange(
	ctx context.Context,
	etcdAddr, prefix string,
) (*etcdRangeResponse, error) {
	keyBytes := []byte(prefix)
	rangeEnd := make([]byte, len(keyBytes))
	copy(rangeEnd, keyBytes)
	rangeEnd[len(rangeEnd)-1]++

	body, err := json.Marshal(map[string]string{
		"key":       base64.StdEncoding.EncodeToString(keyBytes),
		"range_end": base64.StdEncoding.EncodeToString(rangeEnd),
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		etcdAddr+"/v3/kv/range",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("etcd range request failed: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("etcd range returned HTTP %d", resp.StatusCode)
	}

	var result etcdRangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding etcd response: %w", err)
	}
	return &result, nil
}

func (o *Observer) checkCellRegistration(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	etcdAddr, rootPath string,
) {
	var cells multigresv1alpha1.CellList
	if err := o.client.List(ctx, &cells,
		o.listOpts(client.MatchingLabels{common.LabelMultigresCluster: cluster.Name})...,
	); err != nil {
		return
	}

	// Multigres topology keys: <rootPath>/cells/<cell-name>/Cell
	prefix := strings.TrimRight(rootPath, "/") + "/cells/"
	result, err := o.etcdRange(ctx, etcdAddr, prefix)
	if err != nil {
		o.logger.Debug("failed to query etcd for cells", "error", err)
		return
	}

	registeredCells := make(map[string]bool)
	for _, kv := range result.Kvs {
		key, err := base64.StdEncoding.DecodeString(kv.Key)
		if err != nil {
			continue
		}
		// Key format: <prefix><cell-name>/Cell
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			suffix := strings.TrimPrefix(keyStr, prefix)
			parts := strings.Split(suffix, "/")
			if len(parts) >= 2 && parts[1] == "Cell" {
				registeredCells[parts[0]] = true
			}
		}
	}

	// Check that every Cell CRD is registered.
	// Etcd uses the topology cell name (e.g. "zone-a") from the multigres.com/cell label,
	// not the K8s CRD name (e.g. "m-zone-a-b2706ce8").
	for i := range cells.Items {
		cell := &cells.Items[i]
		if cell.DeletionTimestamp != nil {
			continue
		}
		cellName := cell.Labels[common.LabelMultigresCell]
		if cellName == "" {
			cellName = cell.Name
		}
		if !registeredCells[cellName] {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityError,
				Check:     "topology",
				Component: fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name),
				Message: fmt.Sprintf(
					"Cell %s (topology name: %s) exists as CRD but not registered in etcd",
					cell.Name,
					cellName,
				),
			})
		}
		delete(registeredCells, cellName)
	}

	// Check for orphaned etcd entries.
	for name := range registeredCells {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "topology",
			Component: fmt.Sprintf("cell/%s/%s", o.namespace, name),
			Message:   fmt.Sprintf("Cell %s registered in etcd but no corresponding CRD", name),
		})
	}
}

func (o *Observer) checkPoolerRegistration(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	etcdAddr, rootPath string,
) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy:     common.ManagedByMultigres,
			common.LabelAppComponent:     common.ComponentPool,
			common.LabelMultigresCluster: cluster.Name,
		})...,
	); err != nil {
		return
	}

	// Multigres topology keys: <rootPath>/poolers/<service-id>/Pooler
	prefix := strings.TrimRight(rootPath, "/") + "/poolers/"
	result, err := o.etcdRange(ctx, etcdAddr, prefix)
	if err != nil {
		o.logger.Debug("failed to query etcd for poolers", "error", err)
		return
	}

	registeredPoolers := make(map[string]bool)
	for _, kv := range result.Kvs {
		key, err := base64.StdEncoding.DecodeString(kv.Key)
		if err != nil {
			continue
		}
		// Key format: <prefix><service-id>/Pooler
		// Service ID format: multipooler-<cell>-p-<hash>
		keyStr := string(key)
		if strings.HasPrefix(keyStr, prefix) {
			suffix := strings.TrimPrefix(keyStr, prefix)
			parts := strings.Split(suffix, "/")
			if len(parts) >= 2 && parts[1] == "Pooler" {
				serviceID := parts[0]
				registeredPoolers[serviceID] = true
			}
		}
	}

	// Build a map of service ID → pod for each pool pod.
	// The service ID is extracted from the multipooler container's --service-id flag.
	podByServiceID := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if sid := extractServiceID(pod); sid != "" {
			podByServiceID[sid] = pod
		}
	}

	// Match registered pooler service IDs to pods.
	// Etcd keys contain the full service ID (e.g. "multipooler-z1-p-a1b2c3d4");
	// pods carry the short service ID (e.g. "p-a1b2c3d4") in --service-id.
	matchedPods := make(map[string]bool, len(pods.Items))
	for etcdID := range registeredPoolers {
		for sid, pod := range podByServiceID {
			if strings.Contains(etcdID, sid) {
				matchedPods[pod.Name] = true
				delete(registeredPoolers, etcdID)
				break
			}
		}
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		drainState := pod.Annotations[common.AnnotationDrainState]
		registered := matchedPods[pod.Name]

		// Pods in ready-for-deletion should NOT be registered.
		if drainState == common.DrainStateReadyForDeletion {
			if registered {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "topology",
					Component: componentForPod(pod),
					Message: fmt.Sprintf(
						"Pod %s is in ready-for-deletion but still registered in etcd",
						pod.Name,
					),
				})
			}
			continue
		}

		// Running pods without drain state should be registered.
		if pod.Status.Phase == corev1.PodRunning && drainState == "" &&
			pod.DeletionTimestamp == nil {
			if !registered {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "topology",
					Component: componentForPod(pod),
					Message:   fmt.Sprintf("Running pool pod %s not registered in etcd", pod.Name),
				})
			}
		}
	}

	// Remaining entries are orphaned pooler registrations.
	for name := range registeredPoolers {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "topology",
			Component: "pooler/" + name,
			Message:   fmt.Sprintf("Pooler %s registered in etcd but no corresponding pod", name),
		})
	}
}

// extractServiceID reads the --service-id flag value from a pod's multipooler
// container args. Returns empty string if not found.
func extractServiceID(pod *corev1.Pod) string {
	const prefix = "--service-id="
	for _, cs := range [][]corev1.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		for i := range cs {
			if cs[i].Name != "multipooler" {
				continue
			}
			for _, arg := range cs[i].Args {
				if strings.HasPrefix(arg, prefix) {
					return strings.TrimPrefix(arg, prefix)
				}
			}
		}
	}
	return ""
}
