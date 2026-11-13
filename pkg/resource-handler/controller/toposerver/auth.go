package toposerver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

const operatorEtcdUsername = "multigres-operator"

// etcdAuthClient exposes only the auth RPCs used by the reconciler. AuthDisable
// is deliberately absent: a failed reconcile must never turn authorization off.
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
}

// reconcileEtcdIdentities records desired identities in status, makes etcd
// users and roles match them, removes tracked identities no longer desired, and
// enables auth. An identity left out of desired loses access.
func (r *TopoServerReconciler) reconcileEtcdIdentities(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	desired []etcdIdentityGrant,
) error {
	previous := ts.Status.AuthIdentities
	current := make([]string, 0, len(desired))
	for _, grant := range desired {
		current = append(current, grant.Name)
	}
	slices.Sort(current)
	tracked := slices.Concat(previous, current)
	slices.Sort(tracked)
	tracked = slices.Compact(tracked)
	if err := r.saveAuthIdentities(ctx, ts, tracked); err != nil {
		return err
	}
	c, err := r.maintenanceClient(ctx, ts)
	if err != nil {
		return err
	}
	defer c.Close()
	auth, ok := c.(etcdAuthClient)
	if !ok {
		return fmt.Errorf("etcd client does not support authorization")
	}
	if err := reconcileEtcdAuth(ctx, auth, desired, previous); err != nil {
		return err
	}
	return r.saveAuthIdentities(ctx, ts, current)
}

func (r *TopoServerReconciler) saveAuthIdentities(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	identities []string,
) error {
	if slices.Equal(ts.Status.AuthIdentities, identities) {
		return nil
	}
	before := ts.DeepCopy()
	patch := ts.DeepCopy()
	patch.Status.AuthIdentities = slices.Clone(identities)
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
	return nil
}

func reconcileEtcdAuth(
	ctx context.Context,
	c etcdAuthClient,
	desired []etcdIdentityGrant,
	previous []string,
) error {
	status, err := c.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("reading etcd auth status: %w", err)
	}
	if err := ensureEtcdRole(ctx, c, "root"); err != nil {
		return err
	}
	for _, username := range []string{"root", operatorEtcdUsername} {
		if err := ensureEtcdUserRole(ctx, c, username, "root", false); err != nil {
			return err
		}
	}
	desiredNames := make(map[string]bool, len(desired))
	for _, grant := range desired {
		desiredNames[grant.Name] = true
		if err := ensureEtcdRole(ctx, c, grant.Name); err != nil {
			return err
		}
		if err := reconcileEtcdPermissions(ctx, c, grant.Name, grant.Permissions); err != nil {
			return err
		}
		if err := ensureEtcdUserRole(ctx, c, grant.Name, grant.Name, true); err != nil {
			return err
		}
	}
	for _, identity := range previous {
		if identity == "" || identity == "root" || identity == operatorEtcdUsername ||
			desiredNames[identity] {
			continue
		}
		if _, err := c.UserDelete(ctx, identity); err != nil &&
			!errors.Is(err, rpctypes.ErrUserNotFound) {
			return fmt.Errorf("deleting stale etcd user %q: %w", identity, err)
		}
		if _, err := c.RoleDelete(ctx, identity); err != nil &&
			!errors.Is(err, rpctypes.ErrRoleNotFound) {
			return fmt.Errorf("deleting stale etcd role %q: %w", identity, err)
		}
	}
	if !status.Enabled {
		if _, err := c.AuthEnable(ctx); err != nil {
			return fmt.Errorf("enabling etcd auth: %w", err)
		}
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
	role string,
	desired []etcdPermission,
) error {
	existing, err := c.RoleGet(ctx, role)
	if err != nil {
		return fmt.Errorf("reading etcd role %q permissions: %w", role, err)
	}
	retained := make([]*authpb.Permission, 0, len(existing.Perm))
	for _, permission := range existing.Perm {
		desiredPermission, found := findEtcdPermission(desired, permission)
		if found && desiredPermission.Type != permission.PermType {
			if _, err := c.RoleRevokePermission(
				ctx,
				role,
				string(permission.Key),
				string(permission.RangeEnd),
			); err != nil {
				return fmt.Errorf(
					"revoking etcd role %q permission before type change: %w",
					role,
					err,
				)
			}
			continue
		}
		retained = append(retained, permission)
	}
	for _, permission := range desired {
		found := slices.ContainsFunc(retained, func(p *authpb.Permission) bool {
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
	for _, permission := range retained {
		found := slices.ContainsFunc(desired, func(p etcdPermission) bool {
			return p.Key == string(permission.Key) && p.RangeEnd == string(permission.RangeEnd)
		})
		if !found {
			if _, err := c.RoleRevokePermission(
				ctx,
				role,
				string(permission.Key),
				string(permission.RangeEnd),
			); err != nil {
				return fmt.Errorf("revoking etcd role %q permission: %w", role, err)
			}
		}
	}
	return nil
}

func findEtcdPermission(
	desired []etcdPermission,
	existing *authpb.Permission,
) (etcdPermission, bool) {
	for _, permission := range desired {
		if permission.Key == string(existing.Key) &&
			permission.RangeEnd == string(existing.RangeEnd) {
			return permission, true
		}
	}
	return etcdPermission{}, false
}
