package toposerver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type fakeEtcdAuth struct {
	enabled        bool
	users          map[string][]string
	roles          map[string][]*authpb.Permission
	mutations      []string
	failGrant      bool
	failRoleDelete bool
}

func (f *fakeEtcdAuth) UserDelete(
	_ context.Context,
	name string,
) (*clientv3.AuthUserDeleteResponse, error) {
	if _, ok := f.users[name]; !ok {
		return nil, rpctypes.ErrUserNotFound
	}
	f.mutations = append(f.mutations, "delete-user:"+name)
	delete(f.users, name)
	return &clientv3.AuthUserDeleteResponse{}, nil
}

func (f *fakeEtcdAuth) RoleDelete(
	_ context.Context,
	name string,
) (*clientv3.AuthRoleDeleteResponse, error) {
	if f.failRoleDelete {
		return nil, errors.New("role deletion interrupted")
	}
	if _, ok := f.roles[name]; !ok {
		return nil, rpctypes.ErrRoleNotFound
	}
	f.mutations = append(f.mutations, "delete-role:"+name)
	delete(f.roles, name)
	return &clientv3.AuthRoleDeleteResponse{}, nil
}

func newFakeEtcdAuth() *fakeEtcdAuth {
	return &fakeEtcdAuth{users: map[string][]string{}, roles: map[string][]*authpb.Permission{}}
}

func (f *fakeEtcdAuth) AuthStatus(context.Context) (*clientv3.AuthStatusResponse, error) {
	return &clientv3.AuthStatusResponse{Enabled: f.enabled}, nil
}

func (f *fakeEtcdAuth) AuthEnable(context.Context) (*clientv3.AuthEnableResponse, error) {
	if !slices.Contains(f.users["root"], "root") ||
		!slices.Contains(f.users[operatorEtcdUsername], "root") {
		return nil, errors.New("missing administration identity")
	}
	f.mutations = append(f.mutations, "enable")
	f.enabled = true
	return &clientv3.AuthEnableResponse{}, nil
}

func (f *fakeEtcdAuth) UserGet(
	_ context.Context,
	user string,
) (*clientv3.AuthUserGetResponse, error) {
	roles, ok := f.users[user]
	if !ok {
		return nil, rpctypes.ErrUserNotFound
	}
	return &clientv3.AuthUserGetResponse{Roles: slices.Clone(roles)}, nil
}

func (f *fakeEtcdAuth) UserAddWithOptions(
	_ context.Context,
	user,
	password string,
	options *clientv3.UserAddOptions,
) (*clientv3.AuthUserAddResponse, error) {
	if password != "" || !options.NoPassword {
		return nil, errors.New("expected certificate-only user")
	}
	f.mutations = append(f.mutations, "user:"+user)
	f.users[user] = nil
	return &clientv3.AuthUserAddResponse{}, nil
}

func (f *fakeEtcdAuth) UserGrantRole(
	_ context.Context,
	user,
	role string,
) (*clientv3.AuthUserGrantRoleResponse, error) {
	f.mutations = append(f.mutations, "user-role:"+user+":"+role)
	f.users[user] = append(f.users[user], role)
	return &clientv3.AuthUserGrantRoleResponse{}, nil
}

func (f *fakeEtcdAuth) UserRevokeRole(
	_ context.Context,
	user,
	role string,
) (*clientv3.AuthUserRevokeRoleResponse, error) {
	f.mutations = append(f.mutations, "revoke-role:"+user+":"+role)
	f.users[user] = slices.DeleteFunc(f.users[user], func(s string) bool { return s == role })
	return &clientv3.AuthUserRevokeRoleResponse{}, nil
}

func (f *fakeEtcdAuth) RoleGet(
	_ context.Context,
	role string,
) (*clientv3.AuthRoleGetResponse, error) {
	permissions, ok := f.roles[role]
	if !ok {
		return nil, rpctypes.ErrRoleNotFound
	}
	return &clientv3.AuthRoleGetResponse{Perm: slices.Clone(permissions)}, nil
}

func (f *fakeEtcdAuth) RoleAdd(
	_ context.Context,
	role string,
) (*clientv3.AuthRoleAddResponse, error) {
	f.mutations = append(f.mutations, "role:"+role)
	f.roles[role] = nil
	return &clientv3.AuthRoleAddResponse{}, nil
}

func (f *fakeEtcdAuth) RoleGrantPermission(
	_ context.Context,
	role,
	key,
	end string,
	permission clientv3.PermissionType,
) (*clientv3.AuthRoleGrantPermissionResponse, error) {
	if f.failGrant {
		return nil, errors.New("grant failed")
	}
	f.mutations = append(f.mutations, "permission:"+role+":"+key)
	f.roles[role] = slices.DeleteFunc(f.roles[role], func(p *authpb.Permission) bool {
		return string(p.Key) == key && string(p.RangeEnd) == end
	})
	f.roles[role] = append(f.roles[role], &authpb.Permission{
		Key: []byte(key), RangeEnd: []byte(end), PermType: authpb.Permission_Type(permission),
	})
	return &clientv3.AuthRoleGrantPermissionResponse{}, nil
}

func (f *fakeEtcdAuth) RoleRevokePermission(
	_ context.Context,
	role,
	key,
	end string,
) (*clientv3.AuthRoleRevokePermissionResponse, error) {
	f.mutations = append(f.mutations, "revoke-permission:"+role+":"+key)
	f.roles[role] = slices.DeleteFunc(f.roles[role], func(p *authpb.Permission) bool {
		return string(p.Key) == key && string(p.RangeEnd) == end
	})
	return &clientv3.AuthRoleRevokePermissionResponse{}, nil
}

func TestEtcdPrefixPermissionExcludesSiblingRoots(t *testing.T) {
	p := etcdPrefixPermission("/clusters/a/")
	for key, want := range map[string]bool{
		"/clusters/a/global/key":  true,
		"/clusters/ab/global/key": false,
		"/clusters/a-other/key":   false,
	} {
		if got := key >= p.Key && key < p.RangeEnd; got != want {
			t.Errorf("permission includes %q = %t, want %t", key, got, want)
		}
	}
}

func TestReconcileEtcdAuthBootstrapsThenIsIdempotent(t *testing.T) {
	c := newFakeEtcdAuth()
	p := etcdPrefixPermission("/clusters/a/")
	desired := []etcdIdentityGrant{{
		Name:        "cluster-a",
		Permissions: []etcdPermission{p},
	}}
	if err := reconcileEtcdAuth(t.Context(), c, desired, nil); err != nil {
		t.Fatal(err)
	}
	if !c.enabled || c.mutations[len(c.mutations)-1] != "enable" {
		t.Fatalf("auth must be enabled last: %v", c.mutations)
	}
	if diff := cmp.Diff([]string{"cluster-a"}, c.users["cluster-a"]); diff != "" {
		t.Fatal(diff)
	}
	if len(c.roles["cluster-a"]) != 1 {
		t.Fatal("desired permissions missing before auth enabled")
	}
	c.mutations = nil
	if err := reconcileEtcdAuth(t.Context(), c, desired, nil); err != nil {
		t.Fatal(err)
	}
	if len(c.mutations) != 0 {
		t.Fatalf("unchanged auth reconcile mutated state: %v", c.mutations)
	}
}

func TestReconcileEtcdAuthFailureNeverEnablesOrDisables(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := map[bool]string{false: "bootstrap", true: "already enabled"}[enabled]
		t.Run(name, func(t *testing.T) {
			c := newFakeEtcdAuth()
			c.enabled, c.failGrant = enabled, true
			p := etcdPrefixPermission("/clusters/a/")
			if err := reconcileEtcdAuth(t.Context(), c, []etcdIdentityGrant{{
				Name: "a", Permissions: []etcdPermission{p},
			}}, nil); err == nil {
				t.Fatal("expected permission failure")
			}
			if c.enabled != enabled || slices.Contains(c.mutations, "enable") {
				t.Fatalf("failure changed auth state: %v", c.mutations)
			}
		})
	}
}

func TestReconcileEtcdAuthReplacesStaleGrants(t *testing.T) {
	c := newFakeEtcdAuth()
	c.enabled = true
	c.users["a"] = []string{"previous", "root"}
	c.roles["a"] = []*authpb.Permission{{
		Key: []byte("/old/"), RangeEnd: []byte("/old0"), PermType: authpb.READWRITE,
	}}
	p := etcdPrefixPermission("/clusters/a/")
	if err := reconcileEtcdAuth(t.Context(), c, []etcdIdentityGrant{{
		Name: "a", Permissions: []etcdPermission{p},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"a"}, c.users["a"]); diff != "" {
		t.Fatal(diff)
	}
	if len(c.roles["a"]) != 1 || string(c.roles["a"][0].Key) != p.Key {
		t.Fatalf("stale permission retained: %v", c.roles["a"])
	}
	if slices.Contains(c.mutations, "enable") {
		t.Fatal("already-enabled auth was enabled again")
	}
}

func TestReconcileEtcdPermissionsUpgradesType(t *testing.T) {
	c := newFakeEtcdAuth()
	c.roles["a"] = []*authpb.Permission{{
		Key: []byte("/clusters/a/"), RangeEnd: []byte("/clusters/a0"), PermType: authpb.READ,
	}}
	desired := []etcdPermission{{
		Key: "/clusters/a/", RangeEnd: "/clusters/a0", Type: authpb.READWRITE,
	}}

	if err := reconcileEtcdPermissions(t.Context(), c, "a", desired); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{
		"revoke-permission:a:/clusters/a/",
		"permission:a:/clusters/a/",
	}, c.mutations); diff != "" {
		t.Fatalf("permission type change mutations (-want +got):\n%s", diff)
	}
	if got := c.roles["a"]; len(got) != 1 || got[0].PermType != authpb.READWRITE {
		t.Fatalf("permission was not upgraded: %+v", got)
	}

	c.mutations = nil
	if err := reconcileEtcdPermissions(t.Context(), c, "a", desired); err != nil {
		t.Fatal(err)
	}
	if len(c.mutations) != 0 {
		t.Fatalf("unchanged permissions mutated state: %v", c.mutations)
	}
}

func TestReconcileEtcdAuthNeverPrunesAdministrators(t *testing.T) {
	c := newFakeEtcdAuth()
	c.enabled = true
	if err := reconcileEtcdAuth(
		t.Context(), c, nil, []string{"root", operatorEtcdUsername},
	); err != nil {
		t.Fatal(err)
	}
	for _, user := range []string{"root", operatorEtcdUsername} {
		if _, ok := c.users[user]; !ok {
			t.Errorf("administrator user %q was removed", user)
		}
	}
	if _, ok := c.roles["root"]; !ok {
		t.Error("root role was removed")
	}
	for _, mutation := range c.mutations {
		if strings.HasPrefix(mutation, "delete-") {
			t.Errorf("unexpected mutation %q", mutation)
		}
	}
}
