package multiadmin

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func serve(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/databases/postgres/tablegroups" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestListDesiredTableGroups(t *testing.T) {
	commerce := []TableGroup{
		{ID: 7, Name: "commerce", Shards: []string{"0-80", "80-inf"}},
	}
	tests := map[string]struct {
		status  int
		body    string
		want    []TableGroup
		wantErr bool
	}{
		"protojson lowerCamel, int64 as string": {
			status: 200,
			body:   `{"tableGroups":[{"tablegroupId":"7","name":"commerce","state":"TABLE_GROUP_STATE_CREATING","shards":["0-80","80-inf"]}]}`,
			want:   commerce,
		},
		"int64 as a number": {
			status: 200,
			body:   `{"tableGroups":[{"tablegroupId":7,"name":"commerce","state":"TABLE_GROUP_STATE_CREATING","shards":["0-80","80-inf"]}]}`,
			want:   commerce,
		},
		"unknown fields are ignored": {
			status: 200,
			body:   `{"tableGroups":[{"tablegroupId":"7","name":"commerce","state":"TABLE_GROUP_STATE_CREATING","shards":["0-80","80-inf"],"future":1}],"next":"x"}`,
			want:   commerce,
		},
		"empty object is an empty list": {status: 200, body: `{}`, want: []TableGroup{}},
		"empty array is an empty list": {
			status: 200,
			body:   `{"tableGroups":[]}`,
			want:   []TableGroup{},
		},
		"404 is an error": {status: 404, body: `{"code":"not_found"}`, wantErr: true},
		"503 is an error": {
			status:  503,
			body:    `{"code":"unavailable"}`,
			wantErr: true,
		},
		"200 with junk is an error": {status: 200, body: `<html>`, wantErr: true},
		"200 with empty body is an error": {
			status: 200, body: ``, wantErr: true,
		},
		"missing id is an error": {
			status:  200,
			body:    `{"tableGroups":[{"name":"commerce","shards":["0-inf"]}]}`,
			wantErr: true,
		},
		"non-numeric id is an error": {
			status:  200,
			body:    `{"tableGroups":[{"tablegroupId":"seven","name":"commerce"}]}`,
			wantErr: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := ListDesiredTableGroups(
				t.Context(),
				http.DefaultClient,
				serve(t, tc.status, tc.body),
				"postgres",
			)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", got)
				}
				if got != nil {
					t.Fatalf("an error must come with no list, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestListDesiredTableGroups_Unreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	if _, err := ListDesiredTableGroups(
		t.Context(),
		http.DefaultClient,
		srv.URL,
		"postgres",
	); err == nil {
		t.Fatal("want error from an unreachable multiadmin")
	}
}
