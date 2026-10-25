package testutil

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/ownership"
)

// RewriteAsChildWriter renames every operator ("multigres-" prefixed) field
// manager on the object at key to ownership.ManagerChildWriter, so it looks
// last written by an operator that applied it under that name, which is the
// state every child object of an upgraded cluster is in. The operator's own
// writes are kept; only the names change. Nothing is renamed when the
// object already carries only that name.
//
// A test that drives the controller afterwards proves a rename of the child
// writer cannot have happened: under any other name the old entry would stay
// a co-owner and the controller could no longer prune what it stops sending.
func RewriteAsChildWriter(t testing.TB, c client.Client, obj client.Object) {
	t.Helper()
	key := client.ObjectKeyFromObject(obj)
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := rewriteOnce(t.Context(), c, key, obj)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rewrite managers on %s to %s: %v", key, ownership.ManagerChildWriter, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func rewriteOnce(
	ctx context.Context,
	c client.Client,
	key client.ObjectKey,
	obj client.Object,
) error {
	if err := c.Get(ctx, key, obj); err != nil {
		return err
	}
	var from []string
	for _, m := range managedfields.Managers(obj) {
		if strings.HasPrefix(m, "multigres-") && m != ownership.ManagerChildWriter {
			from = append(from, m)
		}
	}
	for _, m := range from {
		err := managedfields.RenameManager(ctx, c, obj, m, ownership.ManagerChildWriter)
		if err != nil && !errors.Is(err, managedfields.ErrNoMatch) {
			return err
		}
	}
	return nil
}

// OperatorManagers returns the sorted "multigres-" manager names on obj. The
// test client and the apiserver write under other names.
func OperatorManagers(obj client.Object) []string {
	return slices.DeleteFunc(managedfields.Managers(obj), func(m string) bool {
		return !strings.HasPrefix(m, "multigres-")
	})
}
