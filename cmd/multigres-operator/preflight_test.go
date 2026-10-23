package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestPreflight(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ns := func(name string, labels map[string]string) client.Object {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	}
	lease := func(namespace, name string) client.Object {
		return &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		}
	}
	disabled := map[string]string{"multigres.com/reconcile": "disabled"}
	// A deployed operator installed outside the default namespace, which a
	// lookup keyed on a fixed namespace misses.
	custom := lease("mgo", leaderElectionID)
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
		"", errors.New("rbac"),
	)

	tests := map[string]struct {
		objs          []client.Object
		laptopNS      string
		outOfCluster  bool
		listErr       error
		getErr        error
		wantErrSubstr string
	}{
		"in cluster without --namespace checks nothing": {
			objs:    []client.Object{custom},
			listErr: forbidden,
		},
		"out of cluster on a bare cluster is allowed": {
			outOfCluster: true,
		},
		"out of cluster ignores leases with other names": {
			objs:         []client.Object{lease("kube-node-lease", "node-1")},
			outOfCluster: true,
		},
		"out of cluster beside an operator in the default namespace is refused": {
			objs:          []client.Object{lease("multigres-operator", leaderElectionID)},
			outOfCluster:  true,
			wantErrSubstr: "namespace(s) multigres-operator,",
		},
		"out of cluster beside an operator in another namespace is refused": {
			objs:          []client.Object{custom},
			outOfCluster:  true,
			wantErrSubstr: "namespace(s) mgo,",
		},
		"out of cluster when leases cannot be listed is refused": {
			outOfCluster:  true,
			listErr:       forbidden,
			wantErrSubstr: "cannot list leases",
		},
		"a claimed namespace labelled disabled passes beside a deployed operator": {
			objs:         []client.Object{custom, ns("a", disabled)},
			laptopNS:     "a",
			outOfCluster: true,
		},
		"claiming the deployed operator's own namespace is refused, wherever it is": {
			objs:          []client.Object{custom, ns("mgo", disabled)},
			laptopNS:      "mgo",
			outOfCluster:  true,
			wantErrSubstr: "deployed operator's own",
		},
		"claiming the deployed operator's own namespace is refused in cluster too": {
			objs:          []client.Object{custom, ns("mgo", disabled)},
			laptopNS:      "mgo",
			wantErrSubstr: "deployed operator's own",
		},
		"a claimed namespace when leases cannot be listed is refused": {
			objs:          []client.Object{ns("a", disabled)},
			laptopNS:      "a",
			listErr:       forbidden,
			wantErrSubstr: "cannot list leases",
		},
		"an unlabelled namespace is refused": {
			objs:          []client.Object{ns("a", nil)},
			laptopNS:      "a",
			wantErrSubstr: "not labelled",
		},
		"a malformed label value is refused": {
			objs: []client.Object{
				ns("a", map[string]string{"multigres.com/reconcile": "Disabled"}),
			},
			laptopNS:      "a",
			wantErrSubstr: "not labelled",
		},
		"a missing namespace is refused": {
			laptopNS:      "a",
			wantErrSubstr: "verifying",
		},
		"a namespace read error is refused": {
			laptopNS:      "a",
			getErr:        errors.New("connection refused"),
			wantErrSubstr: "connection refused",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// The API server supports a metadata.name field selector on every
			// resource; the fake mimics field selectors only through indexes.
			b := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objs...).
				WithIndex(&coordinationv1.Lease{}, "metadata.name", func(o client.Object) []string {
					return []string{o.GetName()}
				}).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(
						ctx context.Context, c client.WithWatch, key client.ObjectKey,
						obj client.Object, opts ...client.GetOption,
					) error {
						if tc.getErr != nil {
							return tc.getErr
						}
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(
						ctx context.Context, c client.WithWatch, list client.ObjectList,
						opts ...client.ListOption,
					) error {
						if tc.listErr != nil {
							return tc.listErr
						}
						return c.List(ctx, list, opts...)
					},
				})
			err := preflight(t.Context(), b.Build(), tc.laptopNS, tc.outOfCluster)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("preflight() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("preflight() = %v, want an error mentioning %q", err, tc.wantErrSubstr)
			}
		})
	}
}
