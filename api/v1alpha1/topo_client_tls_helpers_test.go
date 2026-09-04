/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import "testing"

func TestTopoClientTLSConfigured(t *testing.T) {
	tests := map[string]struct {
		ref  GlobalTopoServerRef
		want bool
	}{
		"both secrets set": {
			ref:  GlobalTopoServerRef{CASecret: "ca", ClientCertSecret: "client"},
			want: true,
		},
		"neither set":     {ref: GlobalTopoServerRef{}, want: false},
		"only ca set":     {ref: GlobalTopoServerRef{CASecret: "ca"}, want: false},
		"only client set": {ref: GlobalTopoServerRef{ClientCertSecret: "client"}, want: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := TopoClientTLSConfigured(tc.ref); got != tc.want {
				t.Errorf("TopoClientTLSConfigured() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A managed topology names one Secret for both the keypair and the CA; an
// external topology may split them. The projection has to read the keypair from
// ClientCertSecret and the CA from CASecret in either case.
func TestBuildTopoClientTLSVolume_ProjectsFromBothSecrets(t *testing.T) {
	ref := GlobalTopoServerRef{CASecret: "the-ca", ClientCertSecret: "the-client"}
	vol := BuildTopoClientTLSVolume(ref)

	if vol.Name != TopoClientTLSVolumeName {
		t.Errorf("volume name = %q, want %q", vol.Name, TopoClientTLSVolumeName)
	}
	if vol.Projected == nil {
		t.Fatal("expected a projected volume source")
	}
	sources := vol.Projected.Sources
	if len(sources) != 2 {
		t.Fatalf("got %d projection sources, want 2", len(sources))
	}

	keypair := sources[0].Secret
	if keypair == nil || keypair.Name != "the-client" {
		t.Fatalf("keypair source = %+v, want secret the-client", keypair)
	}
	wantKeypairKeys := map[string]string{"tls.crt": "tls.crt", "tls.key": "tls.key"}
	gotKeypairKeys := map[string]string{}
	for _, item := range keypair.Items {
		gotKeypairKeys[item.Key] = item.Path
	}
	for k, v := range wantKeypairKeys {
		if gotKeypairKeys[k] != v {
			t.Errorf("keypair projects %q to %q, want %q", k, gotKeypairKeys[k], v)
		}
	}

	ca := sources[1].Secret
	if ca == nil || ca.Name != "the-ca" {
		t.Fatalf("ca source = %+v, want secret the-ca", ca)
	}
	if len(ca.Items) != 1 || ca.Items[0].Key != "ca.crt" || ca.Items[0].Path != "ca.crt" {
		t.Errorf("ca projection = %+v, want ca.crt to ca.crt", ca.Items)
	}
}

func TestTopoClientTLSArgs(t *testing.T) {
	args := TopoClientTLSArgs()
	want := []string{
		"--topo-etcd-tls-cert", TopoClientTLSCertFile,
		"--topo-etcd-tls-key", TopoClientTLSKeyFile,
		"--topo-etcd-tls-ca", TopoClientTLSCAFile,
	}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %q, want %q", i, args[i], want[i])
		}
	}
}
