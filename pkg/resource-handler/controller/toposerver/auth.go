package toposerver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

const operatorEtcdUsername = "multigres-operator"

const (
	// authIdentityTimeout bounds the etcd calls for one identity, so a slow
	// identity cannot consume the time of those after it.
	authIdentityTimeout = 10 * time.Second
	// authPassTimeout bounds the whole pass, including the etcd connection.
	authPassTimeout = 5 * time.Minute
	// maxReportedFailures caps the failures named in a condition message.
	maxReportedFailures = 3
)

type etcdAuthClient interface {
	AuthStatus(context.Context) (*clientv3.AuthStatusResponse, error)
	AuthEnable(context.Context) (*clientv3.AuthEnableResponse, error)
	UserGet(context.Context, string) (*clientv3.AuthUserGetResponse, error)
	UserDelete(context.Context, string) (*clientv3.AuthUserDeleteResponse, error)
	UserAddWithOptions(
		context.Context,
		string,
		string,
		*clientv3.UserAddOptions,
	) (*clientv3.AuthUserAddResponse, error)
	UserGrantRole(context.Context, string, string) (*clientv3.AuthUserGrantRoleResponse, error)
	UserRevokeRole(context.Context, string, string) (*clientv3.AuthUserRevokeRoleResponse, error)
	RoleGet(context.Context, string) (*clientv3.AuthRoleGetResponse, error)
	RoleDelete(context.Context, string) (*clientv3.AuthRoleDeleteResponse, error)
	RoleAdd(context.Context, string) (*clientv3.AuthRoleAddResponse, error)
	RoleGrantPermission(
		context.Context,
		string,
		string,
		string,
		clientv3.PermissionType,
	) (*clientv3.AuthRoleGrantPermissionResponse, error)
	RoleRevokePermission(
		context.Context,
		string,
		string,
		string,
	) (*clientv3.AuthRoleRevokePermissionResponse, error)
}

type etcdPermission struct {
	Key      string
	RangeEnd string
	Type     authpb.Permission_Type
}

// etcdPrefixPermission grants read-write access to descendants of prefix, which
// must end in the "/" delimiter so that /clusters/a/ excludes /clusters/ab.
func etcdPrefixPermission(prefix string) etcdPermission {
	return etcdPermission{
		Key: prefix, RangeEnd: clientv3.GetPrefixRangeEnd(prefix), Type: clientv3.PermReadWrite,
	}
}

// etcdIdentityGrant is the complete set of permissions for one project
// identity. Name is both its etcd user and that user's only role.
type etcdIdentityGrant struct {
	Name        string
	Permissions []etcdPermission
	// Clusters are the clusters that required the identity. Several clusters
	// share one identity when they share a project reference.
	Clusters []client.ObjectKey
}

func (g etcdIdentityGrant) clusterNames() string {
	names := make([]string, 0, len(g.Clusters))
	for _, cluster := range g.Clusters {
		names = append(names, cluster.String())
	}
	return strings.Join(names, ",")
}

// reconcileEtcdIdentities records desired identities in status, makes etcd
// users and roles match them, removes tracked identities no longer desired, and
// enables auth. Frozen identities stay tracked and untouched in etcd. Once auth
// is active a failing identity no longer stops the others; the failures come
// back as an *identityFailuresError after bookkeeping.
func (r *TopoServerReconciler) reconcileEtcdIdentities(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	desired []etcdIdentityGrant,
	frozen []string,
	auth etcdAuthClient,
	status *clientv3.AuthStatusResponse,
) error {
	previous := slices.DeleteFunc(
		slices.Clone(ts.Status.AuthIdentities),
		func(identity string) bool { return slices.Contains(frozen, identity) },
	)
	current := make([]string, 0, len(desired))
	priorClusters := ts.Status.AuthIdentityClusters
	clusters := maps.Clone(priorClusters)
	if clusters == nil {
		clusters = map[string]string{}
	}
	for _, grant := range desired {
		current = append(current, grant.Name)
		names := grant.clusterNames()
		prior := priorClusters[grant.Name]
		if prior == "" && slices.Contains(previous, grant.Name) {
			continue
		}
		if prior != "" && prior != names {
			// A failed pass may still leave ranges belonging to earlier contributors.
			combined := append(strings.Split(prior, ","), strings.Split(names, ",")...)
			slices.Sort(combined)
			names = strings.Join(slices.Compact(combined), ",")
		}
		clusters[grant.Name] = names
	}
	slices.Sort(current)
	tracked := slices.Concat(previous, current, frozen)
	slices.Sort(tracked)
	tracked = slices.Compact(tracked)
	if err := r.saveAuthIdentities(ctx, ts, tracked, clusters); err != nil {
		return err
	}
	if auth == nil {
		var closeClient func()
		var err error
		auth, closeClient, err = r.openEtcdAuth(ctx, ts)
		if err != nil {
			return err
		}
		defer closeClient()
	}
	result, err := reconcileEtcdAuth(ctx, auth, desired, previous, authPassOptions{
		continueOnError: ts.Status.AuthEnabled,
		identityTimeout: authIdentityTimeout,
		status:          status,
		confirmRangeRemoval: func(ctx context.Context, grant etcdIdentityGrant) error {
			recorded := priorClusters[grant.Name]
			if recorded == "" && slices.Contains(previous, grant.Name) {
				return fmt.Errorf(
					"cannot confirm range removal for %q: identity has no recorded cluster",
					grant.Name,
				)
			}
			return r.confirmRangeRemoval(ctx, ts, grant, recorded)
		},
	})
	if err != nil {
		return err
	}
	remaining := slices.Concat(current, result.undeleted, frozen)
	slices.Sort(remaining)
	remaining = slices.Compact(remaining)
	for _, grant := range desired {
		if _, failed := result.failed[grant.Name]; !failed {
			clusters[grant.Name] = grant.clusterNames()
		}
	}
	for identity := range clusters {
		if !slices.Contains(remaining, identity) {
			delete(clusters, identity)
		}
	}
	if err := r.saveAuthIdentities(ctx, ts, remaining, clusters); err != nil {
		return err
	}
	if len(result.failed) > 0 {
		return &identityFailuresError{
			total:  len(desired) + len(result.undeleted),
			failed: result.failed,
		}
	}
	return nil
}

func (r *TopoServerReconciler) openEtcdAuth(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (etcdAuthClient, func(), error) {
	c, err := r.maintenanceClient(ctx, ts)
	if err != nil {
		return nil, nil, err
	}
	auth, ok := c.(etcdAuthClient)
	if !ok {
		c.Close()
		return nil, nil, fmt.Errorf("etcd client does not support authorization")
	}
	return auth, c.Close, nil
}

// identityFailuresError reports identities left unreconciled in an
// already-activated server.
type identityFailuresError struct {
	total  int
	failed map[string]error
}

func (e *identityFailuresError) Error() string {
	names := slices.Sorted(maps.Keys(e.failed))
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s: %v", name, e.failed[name]))
	}
	return fmt.Sprintf(
		"%d of %d etcd identities failed to reconcile: %s",
		len(names), e.total, cappedList(parts, "; "),
	)
}

// cappedList joins the first maxReportedFailures parts and counts the rest.
func cappedList(parts []string, separator string) string {
	shown := parts[:min(len(parts), maxReportedFailures)]
	list := strings.Join(shown, separator)
	if more := len(parts) - len(shown); more > 0 {
		list += fmt.Sprintf(" (and %d more)", more)
	}
	return list
}

func (r *TopoServerReconciler) saveAuthIdentities(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	identities []string,
	clusters map[string]string,
) error {
	if len(clusters) == 0 {
		clusters = nil
	}
	if slices.Equal(ts.Status.AuthIdentities, identities) &&
		maps.Equal(ts.Status.AuthIdentityClusters, clusters) {
		return nil
	}
	before := ts.DeepCopy()
	patch := ts.DeepCopy()
	patch.Status.AuthIdentities = slices.Clone(identities)
	patch.Status.AuthIdentityClusters = maps.Clone(clusters)
	if err := r.Status().Patch(
		ctx,
		patch,
		client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
		client.FieldOwner(ownership.ManagerTopoServer),
	); err != nil {
		return fmt.Errorf("recording managed etcd identities: %w", err)
	}
	ts.ResourceVersion = patch.ResourceVersion
	ts.Status.AuthIdentities = slices.Clone(identities)
	ts.Status.AuthIdentityClusters = maps.Clone(clusters)
	return nil
}

// authPassOptions tunes one reconcileEtcdAuth pass.
type authPassOptions struct {
	// continueOnError keeps reconciling the remaining identities after one
	// fails, which is only safe once etcd has auth enabled.
	continueOnError bool
	// identityTimeout bounds the etcd calls for each identity. Zero means none.
	identityTimeout     time.Duration
	status              *clientv3.AuthStatusResponse
	confirmRangeRemoval func(context.Context, etcdIdentityGrant) error
}

type authPassResult struct {
	// failed holds desired identities that did not reconcile.
	failed map[string]error
	// undeleted holds stale identities whose removal failed.
	undeleted []string
}

func (o authPassOptions) run(ctx context.Context, f func(context.Context) error) error {
	if o.identityTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.identityTimeout)
		defer cancel()
	}
	return f(ctx)
}

func reconcileEtcdAuth(
	ctx context.Context,
	c etcdAuthClient,
	desired []etcdIdentityGrant,
	previous []string,
	opts authPassOptions,
) (authPassResult, error) {
	result := authPassResult{failed: map[string]error{}}
	status := opts.status
	if err := opts.run(ctx, func(ctx context.Context) error {
		if status == nil {
			var err error
			if status, err = readEtcdAuthStatus(ctx, c); err != nil {
				return err
			}
		}
		if err := ensureEtcdRole(ctx, c, "root"); err != nil {
			return err
		}
		for _, username := range []string{"root", operatorEtcdUsername} {
			if err := ensureEtcdUserRole(ctx, c, username, "root", false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return result, err
	}
	tolerate := opts.continueOnError && status.Enabled
	desiredNames := make(map[string]bool, len(desired))
	for _, grant := range desired {
		desiredNames[grant.Name] = true
	}
	for _, grant := range desired {
		if err := opts.run(ctx, func(ctx context.Context) error {
			if err := ensureEtcdRole(ctx, c, grant.Name); err != nil {
				return err
			}
			if err := reconcileEtcdPermissions(
				ctx,
				c,
				grant,
				opts.confirmRangeRemoval,
			); err != nil {
				return err
			}
			return ensureEtcdUserRole(ctx, c, grant.Name, grant.Name, true)
		}); err != nil {
			if !tolerate {
				return result, err
			}
			result.failed[grant.Name] = err
			continue
		}
	}
	for _, identity := range previous {
		if identity == "" || identity == "root" || identity == operatorEtcdUsername ||
			desiredNames[identity] {
			continue
		}
		if err := opts.run(ctx, func(ctx context.Context) error {
			return deleteEtcdIdentity(ctx, c, identity)
		}); err != nil {
			if !tolerate {
				return result, err
			}
			result.undeleted = append(result.undeleted, identity)
			result.failed[identity] = err
		}
	}
	if !status.Enabled {
		if err := opts.run(ctx, func(ctx context.Context) error {
			if _, err := c.AuthEnable(ctx); err != nil {
				return fmt.Errorf("enabling etcd auth: %w", err)
			}
			return nil
		}); err != nil {
			return result, err
		}
	}
	return result, nil
}

func readEtcdAuthStatus(
	ctx context.Context,
	c etcdAuthClient,
) (*clientv3.AuthStatusResponse, error) {
	status, err := c.AuthStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading etcd auth status: %w", err)
	}
	return status, nil
}

func deleteEtcdIdentity(ctx context.Context, c etcdAuthClient, identity string) error {
	if _, err := c.UserDelete(ctx, identity); err != nil &&
		!errors.Is(err, rpctypes.ErrUserNotFound) {
		return fmt.Errorf("deleting stale etcd user %q: %w", identity, err)
	}
	if _, err := c.RoleDelete(ctx, identity); err != nil &&
		!errors.Is(err, rpctypes.ErrRoleNotFound) {
		return fmt.Errorf("deleting stale etcd role %q: %w", identity, err)
	}
	return nil
}

func ensureEtcdRole(ctx context.Context, c etcdAuthClient, role string) error {
	_, err := c.RoleGet(ctx, role)
	if err == nil {
		return nil
	}
	if !errors.Is(err, rpctypes.ErrRoleNotFound) {
		return fmt.Errorf("reading etcd role %q: %w", role, err)
	}
	_, err = c.RoleAdd(ctx, role)
	if err != nil && !errors.Is(err, rpctypes.ErrRoleAlreadyExist) {
		return fmt.Errorf("creating etcd role %q: %w", role, err)
	}
	return nil
}

func ensureEtcdUserRole(
	ctx context.Context,
	c etcdAuthClient,
	user,
	role string,
	exclusive bool,
) error {
	existing, err := c.UserGet(ctx, user)
	if errors.Is(err, rpctypes.ErrUserNotFound) {
		_, err = c.UserAddWithOptions(ctx, user, "", &clientv3.UserAddOptions{NoPassword: true})
		if err != nil && !errors.Is(err, rpctypes.ErrUserAlreadyExist) {
			return fmt.Errorf("creating etcd user %q: %w", user, err)
		}
		existing, err = c.UserGet(ctx, user)
	}
	if err != nil {
		return fmt.Errorf("reading etcd user %q: %w", user, err)
	}
	if !slices.Contains(existing.Roles, role) {
		if _, err := c.UserGrantRole(ctx, user, role); err != nil {
			return fmt.Errorf("granting etcd role %q to %q: %w", role, user, err)
		}
	}
	if exclusive {
		for _, previous := range existing.Roles {
			if previous != role {
				if _, err := c.UserRevokeRole(ctx, user, previous); err != nil {
					return fmt.Errorf("revoking etcd role %q from %q: %w", previous, user, err)
				}
			}
		}
	}
	return nil
}

func reconcileEtcdPermissions(
	ctx context.Context,
	c etcdAuthClient,
	grant etcdIdentityGrant,
	confirmRangeRemoval func(context.Context, etcdIdentityGrant) error,
) error {
	role, desired := grant.Name, grant.Permissions
	existing, err := c.RoleGet(ctx, role)
	if err != nil {
		return fmt.Errorf("reading etcd role %q permissions: %w", role, err)
	}
	obsolete := slices.DeleteFunc(
		slices.Clone(existing.Perm),
		func(permission *authpb.Permission) bool {
			return slices.ContainsFunc(desired, func(p etcdPermission) bool {
				return p.Key == string(permission.Key) && p.RangeEnd == string(permission.RangeEnd)
			})
		},
	)
	if len(obsolete) != 0 && confirmRangeRemoval != nil {
		if err := confirmRangeRemoval(ctx, grant); err != nil {
			return err
		}
	}
	for _, permission := range desired {
		found := slices.ContainsFunc(existing.Perm, func(p *authpb.Permission) bool {
			return bytes.Equal(p.Key, []byte(permission.Key)) &&
				bytes.Equal(p.RangeEnd, []byte(permission.RangeEnd)) &&
				p.PermType == permission.Type
		})
		if !found {
			if _, err := c.RoleGrantPermission(
				ctx,
				role,
				permission.Key,
				permission.RangeEnd,
				clientv3.PermissionType(permission.Type),
			); err != nil {
				return fmt.Errorf("granting etcd role %q permission: %w", role, err)
			}
		}
	}
	for _, permission := range obsolete {
		if _, err := c.RoleRevokePermission(
			ctx,
			role,
			string(permission.Key),
			string(permission.RangeEnd),
		); err != nil {
			return fmt.Errorf("revoking etcd role %q permission: %w", role, err)
		}
	}
	return nil
}
