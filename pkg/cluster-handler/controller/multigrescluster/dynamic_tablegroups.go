package multigrescluster

import (
	"cmp"
	"context"
	"fmt"
	"maps"
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
// nothing is kept in memory between reconciles. CREATE TABLEGROUP changes
// nothing the operator watches, so dynamicTableGroupPoller reads the desired
// lists on its own schedule and enqueues a cluster only when the catalog asks
// for something its TableGroups and conditions do not yet show; a converged
// cluster is never reconciled because of the catalog.
//
//   - Provisioning claims the catalog id in topology, reads the list again,
//     and applies a TableGroup labelled LabelDynamicTableGroup and annotated
//     with the catalog id, shaped from the database's default tablegroup.
//     The TableGroup controller creates its Shards, and the Shard controller
//     passes the id to poolers. See dynamic_tablegroup_claims.go.
//   - A dynamic TableGroup whose id a successful read lists as dropping
//     leaves the active set, so the prune path in reconcileDatabases drains
//     and deletes it. Each Shard's deletion removes the id's pooler records,
//     and the provision claim is released once nothing carries the id. Every other dynamic TableGroup
//     of a database the spec declares stays active, even when the catalog no
//     longer lists it: absence is never a reason to remove anything.
//   - Removing the database from the spec releases its dynamic tablegroups:
//     they are drained and deleted with the database's spec-declared ones,
//     as before dynamic tablegroups existed. Its catalog goes with the
//     default tablegroup, and PruneDatabases deletes its topology record,
//     which is what the shards' poolers and any waiting CREATE depend on.
//   - A retained dynamic TableGroup is re-applied on every reconcile from its
//     own stored shape (name, shard names, catalog id) and the database's
//     current default tablegroup, whether or not the catalog is being read.
//     Images, credentials, TLS, backup and topology changes reach it like any
//     spec-declared tablegroup.
//   - A spec-declared tablegroup is never applied over a dynamic one of the
//     same name. The conflict is reported from the existing TableGroups, so
//     it is visible whether or not the catalog is being read.
//   - A failed read adds nothing and removes nothing.

// multiadminHTTPTimeout bounds one desired-list read.
const multiadminHTTPTimeout = 10 * time.Second

// resourceNamePattern is the CRD pattern for TableGroupName and ShardName;
// the catalog's own rules are stricter.
var resourceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// maxDynamicShards is the TableGroup CRD's limit on spec.shards (MaxItems). A
// longer layout would be rejected by the API server on every apply.
const maxDynamicShards = 32

// maxNameLen is the catalog's limit on tablegroup and shard names. Longer
// names from the catalog are untrusted input and are only ever quoted
// truncated to this length.
const maxNameLen = 25

// maxDynamicTableGroupsPerDatabase caps what the catalog can make the
// operator provision for one database. It matches the spec's own limit
// (MaxItems=20 on DatabaseConfig.TableGroups): anyone who can run CREATE
// TABLEGROUP could otherwise turn a loop of statements into unbounded pods
// and PVCs, with no supported cleanup. Entries past the cap, in id order, are
// reported in the condition and not provisioned.
const maxDynamicTableGroupsPerDatabase = 20

// maxConditionItems bounds each list in the DynamicTableGroups condition
// message. The CRD caps a message at 32768 bytes and a rejected status apply
// would freeze every condition of the cluster, so the lists are cut with an
// "and N more" suffix well before that.
const maxConditionItems = 20

// dynamicTableGroupsEnabled reports whether the operator acts on the catalog
// for cluster. Lite clusters run one minigres pod and cannot host more shards.
func dynamicTableGroupsEnabled(cluster *multigresv1alpha1.MultigresCluster) bool {
	return multigresv1alpha1.DynamicTableGroupsEnabled(cluster) &&
		cluster.Spec.Mode != multigresv1alpha1.ModeLite
}

// dynamicTemplate is a database's default tablegroup, the shape every dynamic
// tablegroup of that database is provisioned with.
type dynamicTemplate struct {
	db    multigresv1alpha1.DatabaseConfig
	cfg   multigresv1alpha1.TableGroupConfig
	shard multigresv1alpha1.ShardResolvedSpec
}

// isDynamic reports whether tg is a TableGroup provisioned from the catalog.
func isDynamic(tg *multigresv1alpha1.TableGroup) bool {
	return tg != nil && tg.Labels[multigresv1alpha1.LabelDynamicTableGroup] == "true"
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

// reconcileDynamicTableGroups re-applies the retained dynamic TableGroups,
// applies the ones the catalog newly lists, leaves out of active the ones it
// is dropping so the prune in reconcileDatabases removes them, and releases
// the provision claims of dropped ones with nothing left. It marks in active
// every TableGroup that must not be pruned. existing is the cluster's
// TableGroups by object name as listed before this reconcile applied
// anything.
func (r *MultigresClusterReconciler) reconcileDynamicTableGroups(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	existing map[string]*multigresv1alpha1.TableGroup,
	templates map[multigresv1alpha1.DatabaseName]dynamicTemplate,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	active map[string]bool,
) error {
	specDBs := map[multigresv1alpha1.DatabaseName]bool{}
	conflicts := map[string]bool{}
	for _, db := range cluster.Spec.Databases {
		specDBs[db.Name] = true
		for _, tg := range db.TableGroups {
			if isDynamic(existing[TableGroupName(cluster.Name, db.Name, tg.Name)]) {
				conflicts[fmt.Sprintf("%s/%s", db.Name, tg.Name)] = true
			}
		}
	}

	enabled := dynamicTableGroupsEnabled(cluster)
	l := log.FromContext(ctx)
	var problems []string
	readFailed := false

	// Read every database's list first: a provisioned tablegroup the catalog
	// is dropping must stay out of the active set below. A database whose
	// read fails has no plan, and so loses nothing.
	plans := map[multigresv1alpha1.DatabaseName]desiredPlan{}
	if enabled {
		for _, db := range cluster.Spec.Databases {
			desired, err := r.listDesiredTableGroups(ctx, cluster, string(db.Name))
			if err != nil {
				l.Info("Desired tablegroups unknown; provisioning and removing nothing",
					"database", db.Name, "error", err.Error())
				readFailed = true
				problems = append(problems, fmt.Sprintf("%s: %v", db.Name, err))
				continue
			}
			plan := planDesiredTableGroups(cluster, db, desired, existing)
			plans[db.Name] = plan
			problems = append(problems, plan.problems...)
			for _, c := range plan.conflicts {
				conflicts[c] = true
			}
		}
	}

	// Provisioned dynamic tablegroups of a declared database keep following
	// cluster-level configuration, and are removed only when a successful
	// read lists their id as dropping; see the file comment. Turning the
	// opt-in off therefore removes nothing. One whose database left the
	// spec, that is already draining, or that is being dropped is left out
	// of the active set so the prune below finishes it; a drained one the
	// catalog still lists is recreated once its Shards are gone
	// (priorIncarnationPresent).
	for _, tgName := range slices.Sorted(maps.Keys(existing)) {
		tg := existing[tgName]
		if !isDynamic(tg) || !specDBs[tg.Spec.DatabaseName] || tgDraining(tg) {
			continue
		}
		if plans[tg.Spec.DatabaseName].drops(multigresv1alpha1.TableGroupIDOf(tg)) {
			l.Info("Removing a tablegroup the catalog is dropping", "tablegroup", tg.Name,
				"tablegroupID", multigresv1alpha1.TableGroupIDOf(tg))
			continue
		}
		active[tg.Name] = true
		tmpl, ok := templates[tg.Spec.DatabaseName]
		if !ok {
			// The default tablegroup did not resolve this pass: keep the
			// TableGroup as it is rather than guess its shape.
			continue
		}
		desired, err := buildDynamicTableGroup(
			cluster,
			tmpl,
			storedShape(tg),
			globalTopoRef,
			r.Scheme,
		)
		if err != nil {
			return err
		}
		if err := r.applyTableGroup(ctx, cluster, desired, tg); err != nil {
			return err
		}
	}

	if !enabled {
		meta.RemoveStatusCondition(&cluster.Status.Conditions,
			multigresv1alpha1.ConditionDynamicTableGroups)
		setDynamicTableGroupConflictCondition(cluster, conflicts)
		return nil
	}

	claims := &claimStore{r: r, cluster: cluster, ref: globalTopoRef}
	defer claims.close()

	for _, db := range cluster.Spec.Databases {
		plan, ok := plans[db.Name]
		if !ok {
			continue
		}
		problems = append(
			problems,
			r.releaseDroppedClaims(ctx, cluster, claims, db.Name, plan.dropping, existing)...)

		tmpl, ok := templates[db.Name]
		if !ok {
			problems = append(problems,
				fmt.Sprintf("%s: no default tablegroup to provision from", db.Name))
			continue
		}

		var claimable []multiadmin.TableGroup
		built := map[int64]*multigresv1alpha1.TableGroup{}
		for _, want := range plan.pending {
			desiredTG, err := buildDynamicTableGroup(cluster, tmpl, want, globalTopoRef, r.Scheme)
			if err != nil {
				return err
			}
			// A pending entry has no TableGroup of its name and id, so a
			// current object is another incarnation to wait for; what is
			// left after the wait is a name with nothing under it yet.
			wait, err := r.priorIncarnationPresent(
				ctx,
				cluster,
				existing[desiredTG.Name],
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
			claimable = append(claimable, want)
			built[want.ID] = desiredTG
		}
		if len(claimable) == 0 {
			continue
		}

		ready, msgs, err := r.claimAndRecheck(ctx, cluster, claims, db.Name, claimable)
		problems = append(problems, msgs...)
		if err != nil {
			l.Info("Desired tablegroups unknown after claiming; provisioning nothing",
				"database", db.Name, "error", err.Error())
			readFailed = true
			continue
		}
		for _, want := range ready {
			desiredTG := built[want.ID]
			active[desiredTG.Name] = true
			if err := r.applyTableGroup(ctx, cluster, desiredTG, nil); err != nil {
				// One entry the API server rejects must not stop the rest of the
				// cluster's reconcile. Its claim stays: it is released once the
				// catalog drops the id, like any other.
				problems = append(problems, fmt.Sprintf("%s/%s: %v", db.Name, want.Name, err))
				continue
			}
		}
	}

	r.setDynamicTableGroupConditions(cluster, existing, readFailed, problems, conflicts)
	return nil
}

// desiredPlan is what one database's catalog read asks of the operator, given
// the cluster's existing TableGroups. It is pure: the reconcile acts on it and
// the poller decides from it whether a reconcile is due at all.
type desiredPlan struct {
	// pending are the valid entries under the cap that have no TableGroup of
	// their name and catalog id yet. The name may still carry another
	// incarnation (another id, a spec-declared TableGroup, one draining)
	// that the reconcile waits on and reports before creating this one.
	pending []multiadmin.TableGroup
	// dropping are the valid entries the catalog is dropping, whether or not
	// anything was ever provisioned for them.
	dropping []multiadmin.TableGroup
	// problems are entries that cannot be provisioned, with the reason.
	problems []string
	// conflicts are "db/name" entries the spec also declares.
	conflicts []string
}

// planDesiredTableGroups classifies every catalog entry of db against
// existing, the cluster's TableGroups by object name.
func planDesiredTableGroups(
	cluster *multigresv1alpha1.MultigresCluster,
	db multigresv1alpha1.DatabaseConfig,
	desired []multiadmin.TableGroup,
	existing map[string]*multigresv1alpha1.TableGroup,
) desiredPlan {
	var plan desiredPlan

	specNames := map[multigresv1alpha1.TableGroupName]bool{}
	for _, tg := range db.TableGroups {
		specNames[tg.Name] = true
	}

	// Oldest first, so the ones already provisioned keep their place under
	// the cap and a new entry is the one left out.
	desired = slices.SortedFunc(slices.Values(desired), func(a, b multiadmin.TableGroup) int {
		return cmp.Compare(a.ID, b.ID)
	})
	provisioned := 0
	for _, tg := range existing {
		if isDynamic(tg) && tg.Spec.DatabaseName == db.Name {
			provisioned++
		}
	}

	listed := map[string]bool{}
	for _, want := range desired {
		// want.Name is unvalidated here and below: quote it truncated.
		if listed[want.Name] {
			plan.problems = append(plan.problems,
				fmt.Sprintf("%s/%s: listed more than once", db.Name, shortName(want.Name)))
			continue
		}
		listed[want.Name] = true
		if msg := validateDesiredTableGroup(want); msg != "" {
			plan.problems = append(plan.problems,
				fmt.Sprintf("%s/%s: %s", db.Name, shortName(want.Name), msg))
			continue
		}
		if want.Dropping() {
			plan.dropping = append(plan.dropping, want)
			continue
		}
		if !want.Provisionable() {
			continue // a state this operator does not know: neither add nor remove
		}
		if specNames[multigresv1alpha1.TableGroupName(want.Name)] {
			plan.conflicts = append(plan.conflicts, fmt.Sprintf("%s/%s", db.Name, want.Name))
			continue
		}

		current := existing[TableGroupName(cluster.Name, db.Name, multigresv1alpha1.TableGroupName(want.Name))]
		if isDynamic(current) && multigresv1alpha1.TableGroupIDOf(current) == want.ID &&
			!tgDraining(current) {
			continue // this incarnation exists
		}
		if current == nil {
			if provisioned >= maxDynamicTableGroupsPerDatabase {
				plan.problems = append(plan.problems, fmt.Sprintf(
					"%s/%s (id %d): not provisioned, the database already has %d dynamic tablegroups",
					db.Name,
					want.Name,
					want.ID,
					maxDynamicTableGroupsPerDatabase,
				))
				continue
			}
			provisioned++
		}
		plan.pending = append(plan.pending, want)
	}
	return plan
}

// drops reports whether the catalog is dropping tablegroup id. A zero plan,
// from a read that failed or never ran, drops nothing.
func (p desiredPlan) drops(id int64) bool {
	return slices.ContainsFunc(p.dropping, func(tg multiadmin.TableGroup) bool {
		return tg.ID == id
	})
}

// storedShape is the catalog entry a retained dynamic TableGroup was
// provisioned from, read back from the object itself.
func storedShape(tg *multigresv1alpha1.TableGroup) multiadmin.TableGroup {
	shards := make([]string, 0, len(tg.Spec.Shards))
	for _, s := range tg.Spec.Shards {
		shards = append(shards, s.Name)
	}
	return multiadmin.TableGroup{
		ID:     multigresv1alpha1.TableGroupIDOf(tg),
		Name:   string(tg.Spec.TableGroupName),
		Shards: shards,
	}
}

// shortName quotes an unvalidated catalog name for a message.
func shortName(s string) string {
	if len(s) > maxNameLen {
		return s[:maxNameLen] + "..."
	}
	return s
}

// joinCapped joins items with "; ", keeping at most maxConditionItems of them.
func joinCapped(items []string) string {
	if len(items) <= maxConditionItems {
		return strings.Join(items, "; ")
	}
	return fmt.Sprintf("%s; and %d more",
		strings.Join(items[:maxConditionItems], "; "), len(items)-maxConditionItems)
}

// validateDesiredTableGroup checks one catalog entry at the trust boundary.
// The catalog enforces stricter rules; this keeps a bad entry from reaching
// resource names.
func validateDesiredTableGroup(tg multiadmin.TableGroup) string {
	if tg.ID <= 0 {
		return fmt.Sprintf("invalid catalog id %d", tg.ID)
	}
	if len(tg.Name) > maxNameLen || !resourceNamePattern.MatchString(tg.Name) {
		return fmt.Sprintf("name is not a lowercase DNS label of at most %d characters", maxNameLen)
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
		if len(s) > maxNameLen || !resourceNamePattern.MatchString(s) || seen[s] {
			return fmt.Sprintf("invalid or duplicate shard %q", shortName(s))
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

	tg, err := BuildTableGroup(cluster, tmpl.db, cfg, shards, globalTopoRef, scheme)
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
		case !isDynamic(current):
			return "waiting for a spec-declared tablegroup of this name to be removed", nil
		case multigresv1alpha1.TableGroupIDOf(current) != want.ID:
			return fmt.Sprintf("waiting for tablegroup id %d to be removed",
				multigresv1alpha1.TableGroupIDOf(current)), nil
		case tgDraining(current):
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
// It is for people and debugging; Multigres reads only topology, so a CREATE
// TABLEGROUP that cannot complete just hangs for the SQL user. This condition
// and a Warning event when it turns False are the only places the failure
// shows up. The event is recorded on the transition only, so a lasting
// problem adds no steady-state events.
func (r *MultigresClusterReconciler) setDynamicTableGroupConditions(
	cluster *multigresv1alpha1.MultigresCluster,
	existing map[string]*multigresv1alpha1.TableGroup,
	readFailed bool,
	problems []string,
	conflicts map[string]bool,
) {
	var provisioned []string
	for _, tg := range existing {
		if !isDynamic(tg) {
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
		msg = "provisioned: " + joinCapped(provisioned)
	}
	if len(problems) > 0 {
		msg += ". " + joinCapped(problems)
	}

	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionDynamicTableGroups,
		Status:             metav1.ConditionTrue,
		Reason:             "Synced",
		Message:            msg,
		ObservedGeneration: cluster.Generation,
	}
	switch {
	case readFailed:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "DesiredStateUnknown"
	case len(problems) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = "ProvisioningFailed"
	}
	// SetStatusCondition updates the stored condition in place, so read the
	// previous status before it runs.
	wasFalse := meta.IsStatusConditionFalse(cluster.Status.Conditions, cond.Type)
	meta.SetStatusCondition(&cluster.Status.Conditions, cond)
	if cond.Status == metav1.ConditionFalse && !wasFalse {
		r.Recorder.Eventf(cluster, "Warning", "DynamicTableGroupsFailed",
			"%s: %s", cond.Reason, joinCapped(problems))
	}

	setDynamicTableGroupConflictCondition(cluster, conflicts)
}

// setDynamicTableGroupConflictCondition reports names ("db/tablegroup") that
// both the spec and the catalog claim.
func setDynamicTableGroupConflictCondition(
	cluster *multigresv1alpha1.MultigresCluster,
	conflicts map[string]bool,
) {
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
			"provisioned first is kept and the other is ignored: " +
			joinCapped(slices.Sorted(maps.Keys(conflicts))),
		ObservedGeneration: cluster.Generation,
	})
}
