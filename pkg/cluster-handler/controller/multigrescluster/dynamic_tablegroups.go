package multigrescluster

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Dynamic tablegroups: CREATE TABLEGROUP through a gateway records intent in
// the sharding catalog, and the operator provisions to match. See
// docs/sharding/tablegroup-operations.md in multigres.
//
// Everything here is level-triggered from the latest successful read of
// multiadmin's desired list and the cluster's own TableGroups and Shards;
// nothing is kept in memory between reconciles.
//
//   - Provisioning applies a TableGroup labelled LabelDynamicTableGroup and
//     annotated with the catalog id, shaped from the database's default
//     tablegroup. The TableGroup controller creates its Shards, and the Shard
//     controller passes the id to poolers.
//   - Every dynamic TableGroup is kept in the active set, so the prune path in
//     reconcileDatabases never removes one, even when the catalog no longer
//     lists it. Removing provisioned tablegroups needs the topology cleanup a
//     drop requires (pooler records and shard initialization claims), which
//     this operator does not run.
//   - A failed read adds nothing.

// dynamicTableGroupPollInterval is how often an opted-in cluster re-reads the
// desired list when nothing else requeues it.
const dynamicTableGroupPollInterval = 10 * time.Second

// multiadminHTTPTimeout bounds one desired-list read.
const multiadminHTTPTimeout = 10 * time.Second

// resourceNamePattern is the CRD pattern for TableGroupName and ShardName;
// the catalog's own rules are stricter.
var resourceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// maxDynamicShards is the TableGroup CRD's limit on spec.shards (MaxItems). A
// longer layout would be rejected by the API server on every apply.
const maxDynamicShards = 32

// dynamicTableGroupsEnabled reports whether the operator acts on the catalog
// for cluster. Lite clusters run one minigres pod and cannot host more shards.
func dynamicTableGroupsEnabled(cluster *multigresv1alpha1.MultigresCluster) bool {
	return multigresv1alpha1.DynamicTableGroupsEnabled(cluster) &&
		cluster.Spec.Mode != multigresv1alpha1.ModeLite
}

// dynamicTemplate is a database's default tablegroup, the shape every dynamic
// tablegroup of that database is provisioned with.
type dynamicTemplate struct {
	cfg   multigresv1alpha1.TableGroupConfig
	shard multigresv1alpha1.ShardResolvedSpec
}

// listDesiredTableGroups reads the database's desired tablegroups, through the
// test override when set.
func (r *MultigresClusterReconciler) listDesiredTableGroups(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	database string,
) ([]multiadmin.TableGroup, error) {
	if r.ListDesiredTableGroups != nil {
		return r.ListDesiredTableGroups(ctx, cluster, database)
	}
	// Multiadmin's HTTP listener is plain HTTP and the operator never starts
	// it with --enable-auth, so no token is sent.
	// One connection per read: reads are one per database every 10 seconds,
	// too few for a pooled transport to matter.
	transport := &http.Transport{DisableKeepAlives: true}
	if r.DialContext != nil {
		transport.DialContext = r.DialContext
	}
	httpClient := &http.Client{Timeout: multiadminHTTPTimeout, Transport: transport}
	baseURL := fmt.Sprintf("http://%s-multiadmin.%s.svc:18000", cluster.Name, cluster.Namespace)
	return multiadmin.ListDesiredTableGroups(ctx, httpClient, baseURL, database)
}

// reconcileDynamicTableGroups applies the desired dynamic TableGroups and marks
// in active every TableGroup that must not be pruned. existing is the
// cluster's TableGroups as listed before this reconcile applied anything.
func (r *MultigresClusterReconciler) reconcileDynamicTableGroups(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	existing []multigresv1alpha1.TableGroup,
	templates map[multigresv1alpha1.DatabaseName]dynamicTemplate,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	active map[string]bool,
) error {
	byName := map[string]*multigresv1alpha1.TableGroup{}
	dynamicByDB := map[multigresv1alpha1.DatabaseName][]*multigresv1alpha1.TableGroup{}
	for i := range existing {
		tg := &existing[i]
		byName[tg.Name] = tg
		if tg.Labels[multigresv1alpha1.LabelDynamicTableGroup] == "true" {
			dynamicByDB[tg.Spec.DatabaseName] = append(dynamicByDB[tg.Spec.DatabaseName], tg)
		}
	}
	keep := func(db multigresv1alpha1.DatabaseName) {
		for _, tg := range dynamicByDB[db] {
			active[tg.Name] = true
		}
	}

	// Provisioned dynamic tablegroups are never removed; see the file comment.
	// That also covers turning the opt-in off, which freezes what was
	// provisioned, and a database leaving the spec.
	for db := range dynamicByDB {
		keep(db)
	}

	if !dynamicTableGroupsEnabled(cluster) {
		meta.RemoveStatusCondition(&cluster.Status.Conditions,
			multigresv1alpha1.ConditionDynamicTableGroups)
		meta.RemoveStatusCondition(&cluster.Status.Conditions,
			multigresv1alpha1.ConditionDynamicTableGroupConflict)
		return nil
	}

	l := log.FromContext(ctx)
	var problems, conflicts []string
	readFailed := false

	for _, db := range cluster.Spec.Databases {
		tmpl, ok := templates[db.Name]
		if !ok {
			problems = append(problems,
				fmt.Sprintf("%s: no default tablegroup to provision from", db.Name))
			continue
		}

		desired, err := r.listDesiredTableGroups(ctx, cluster, string(db.Name))
		if err != nil {
			l.Info("Desired tablegroups unknown; provisioning nothing new",
				"database", db.Name, "error", err.Error())
			readFailed = true
			problems = append(problems, fmt.Sprintf("%s: %v", db.Name, err))
			continue
		}

		specNames := map[multigresv1alpha1.TableGroupName]bool{}
		for _, tg := range db.TableGroups {
			specNames[tg.Name] = true
		}

		listed := map[string]bool{}
		for _, want := range desired {
			if listed[want.Name] {
				problems = append(
					problems,
					fmt.Sprintf("%s/%s: listed more than once", db.Name, want.Name),
				)
				continue
			}
			listed[want.Name] = true
			if specNames[multigresv1alpha1.TableGroupName(want.Name)] {
				conflicts = append(conflicts, fmt.Sprintf("%s/%s", db.Name, want.Name))
				continue
			}
			if msg := validateDesiredTableGroup(want); msg != "" {
				problems = append(problems, fmt.Sprintf("%s/%s: %s", db.Name, want.Name, msg))
				continue
			}

			desiredTG, err := buildDynamicTableGroup(
				cluster,
				db,
				tmpl,
				want,
				globalTopoRef,
				r.Scheme,
			)
			if err != nil {
				return err
			}

			wait, err := r.priorIncarnationPresent(
				ctx,
				cluster,
				byName[desiredTG.Name],
				db.Name,
				want,
			)
			if err != nil {
				return err
			}
			if wait != "" {
				problems = append(problems, fmt.Sprintf("%s/%s (id %d): %s",
					db.Name, want.Name, want.ID, wait))
				continue
			}

			active[desiredTG.Name] = true
			if err := r.applyTableGroup(ctx, cluster, desiredTG); err != nil {
				// One entry the API server rejects must not stop the rest of the
				// cluster's reconcile.
				problems = append(problems, fmt.Sprintf("%s/%s: %v", db.Name, want.Name, err))
				continue
			}
		}
	}

	setDynamicTableGroupConditions(
		cluster,
		existing,
		readFailed,
		problems,
		conflicts,
	)
	return nil
}

// validateDesiredTableGroup checks one catalog entry at the trust boundary.
// The catalog enforces stricter rules; this keeps a bad entry from reaching
// resource names.
func validateDesiredTableGroup(tg multiadmin.TableGroup) string {
	if tg.ID <= 0 {
		return fmt.Sprintf("invalid catalog id %d", tg.ID)
	}
	if len(tg.Name) > 25 || !resourceNamePattern.MatchString(tg.Name) {
		return "name is not a lowercase DNS label of at most 25 characters"
	}
	if len(tg.Shards) == 0 {
		return "no shards"
	}
	if len(tg.Shards) > maxDynamicShards {
		return fmt.Sprintf(
			"%d shards, more than the %d a TableGroup holds",
			len(tg.Shards),
			maxDynamicShards,
		)
	}
	seen := map[string]bool{}
	for _, s := range tg.Shards {
		if len(s) > 25 || !resourceNamePattern.MatchString(s) || seen[s] {
			return fmt.Sprintf("invalid or duplicate shard %q", s)
		}
		seen[s] = true
	}
	return ""
}

// buildDynamicTableGroup shapes a desired catalog tablegroup from the
// database's default tablegroup: every shard copies the default shard's
// resolved spec (cells, pools, storage, backup, multiorch), and only the
// tablegroup name, shard names and catalog id differ.
func buildDynamicTableGroup(
	cluster *multigresv1alpha1.MultigresCluster,
	db multigresv1alpha1.DatabaseConfig,
	tmpl dynamicTemplate,
	want multiadmin.TableGroup,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.TableGroup, error) {
	cfg := tmpl.cfg.DeepCopy()
	cfg.Name = multigresv1alpha1.TableGroupName(want.Name)
	cfg.Default = false
	cfg.Shards = nil

	shards := make([]multigresv1alpha1.ShardResolvedSpec, 0, len(want.Shards))
	for _, name := range want.Shards {
		s := tmpl.shard.DeepCopy()
		s.Name = name
		shards = append(shards, *s)
	}

	tg, err := BuildTableGroup(cluster, db, cfg, shards, globalTopoRef, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to build dynamic tablegroup %q: %w", want.Name, err)
	}
	tg.Labels[multigresv1alpha1.LabelDynamicTableGroup] = "true"
	if tg.Annotations == nil {
		tg.Annotations = map[string]string{}
	}
	tg.Annotations[multigresv1alpha1.AnnotationTableGroupID] = strconv.FormatInt(want.ID, 10)
	return tg, nil
}

// priorIncarnationPresent returns why want cannot be applied yet, or "" when
// it can. A different incarnation of the name (another catalog id, a
// spec-declared TableGroup being pruned, or this one already on its way out)
// must be gone completely, Shards included, before the new one is created
// under the same names.
func (r *MultigresClusterReconciler) priorIncarnationPresent(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	current *multigresv1alpha1.TableGroup,
	db multigresv1alpha1.DatabaseName,
	want multiadmin.TableGroup,
) (string, error) {
	if current != nil {
		switch {
		case current.Labels[multigresv1alpha1.LabelDynamicTableGroup] != "true":
			return "waiting for a spec-declared tablegroup of this name to be removed", nil
		case multigresv1alpha1.TableGroupIDOf(current) != want.ID:
			return fmt.Sprintf("waiting for tablegroup id %d to be removed",
				multigresv1alpha1.TableGroupIDOf(current)), nil
		case current.DeletionTimestamp != nil ||
			current.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "":
			return "waiting for an earlier removal to finish", nil
		}
		return "", nil
	}

	// No TableGroup: any Shard left under the name is a previous incarnation
	// still running its deletion finalizer.
	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		metadata.LabelMultigresCluster:    cluster.Name,
		metadata.LabelMultigresDatabase:   string(db),
		metadata.LabelMultigresTableGroup: want.Name,
	}); err != nil {
		return "", fmt.Errorf("failed to list shards of tablegroup %q: %w", want.Name, err)
	}
	if len(shards.Items) > 0 {
		return fmt.Sprintf("waiting for %d shard(s) of a previous incarnation to be removed",
			len(shards.Items)), nil
	}
	return "", nil
}

// setDynamicTableGroupConditions reports what the operator has provisioned.
// It is for people and debugging; Multigres reads only topology.
func setDynamicTableGroupConditions(
	cluster *multigresv1alpha1.MultigresCluster,
	existing []multigresv1alpha1.TableGroup,
	readFailed bool,
	problems, conflicts []string,
) {
	var provisioned []string
	for i := range existing {
		tg := &existing[i]
		if tg.Labels[multigresv1alpha1.LabelDynamicTableGroup] != "true" {
			continue
		}
		state := fmt.Sprintf("%s %d/%d shards ready", tg.Status.Phase,
			tg.Status.ReadyShards, tg.Status.TotalShards)
		provisioned = append(provisioned, fmt.Sprintf(
			"%s/%s (id %d): %s",
			tg.Spec.DatabaseName,
			tg.Spec.TableGroupName,
			multigresv1alpha1.TableGroupIDOf(tg),
			state,
		))
	}
	slices.Sort(provisioned)

	msg := "provisioned: none"
	if len(provisioned) > 0 {
		msg = "provisioned: " + strings.Join(provisioned, "; ")
	}
	if len(problems) > 0 {
		msg += ". " + strings.Join(problems, "; ")
	}

	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionDynamicTableGroups,
		Status:             metav1.ConditionTrue,
		Reason:             "Synced",
		Message:            msg,
		ObservedGeneration: cluster.Generation,
	}
	if readFailed {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DesiredStateUnknown"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, cond)

	if len(conflicts) == 0 {
		meta.RemoveStatusCondition(&cluster.Status.Conditions,
			multigresv1alpha1.ConditionDynamicTableGroupConflict)
		return
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:   multigresv1alpha1.ConditionDynamicTableGroupConflict,
		Status: metav1.ConditionTrue,
		Reason: "DeclaredInSpec",
		Message: "the catalog lists tablegroups the spec also declares; whichever was " +
			"provisioned first is kept and the other is ignored: " + strings.Join(conflicts, ", "),
		ObservedGeneration: cluster.Generation,
	})
}
