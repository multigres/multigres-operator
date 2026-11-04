// Package multiadmin reads from multiadmin's REST API.
package multiadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// maxResponseBytes bounds a response body; a desired-tablegroups list is a few
// hundred bytes per tablegroup.
const maxResponseBytes = 4 << 20

// TableGroup is one desired tablegroup from the sharding catalog.
type TableGroup struct {
	// ID is the catalog id (multigres.tablegroup.tablegroup_id).
	ID int64
	// Name is the tablegroup name.
	Name string
	// Shards is the shard layout, e.g. ["0-80", "80-inf"].
	Shards []string
}

// wireTableGroup is one entry of multiadmin's protojson response, which uses
// lowerCamel field names and encodes int64 as a JSON string; a number is
// accepted too.
type wireTableGroup struct {
	TablegroupID json.Number `json:"tablegroupId"`
	Name         string      `json:"name"`
	Shards       []string    `json:"shards"`
}

type wireResponse struct {
	TableGroups []wireTableGroup `json:"tableGroups"`
}

// ListDesiredTableGroups calls GET /api/v1/databases/{database}/tablegroups
// on the multiadmin at baseURL (e.g. http://c-multiadmin.ns.svc:18000).
//
// Any error means the desired state is unknown. Callers must not treat it as
// an empty list: a failed read adds nothing and removes nothing. Only a 200
// with a body that parses is a successful read; an empty list is then real.
func ListDesiredTableGroups(
	ctx context.Context,
	httpClient *http.Client,
	baseURL, database string,
) ([]TableGroup, error) {
	u := baseURL + "/api/v1/databases/" + url.PathEscape(database) + "/tablegroups"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing desired tablegroups of %q: %w", database, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("reading desired tablegroups of %q: %w", database, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing desired tablegroups of %q: HTTP %d: %s",
			database, resp.StatusCode, truncate(body, 512))
	}

	var wire wireResponse
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("decoding desired tablegroups of %q: %w", database, err)
	}
	out := make([]TableGroup, 0, len(wire.TableGroups))
	for _, w := range wire.TableGroups {
		id, err := w.TablegroupID.Int64()
		if err != nil {
			return nil, fmt.Errorf(
				"decoding desired tablegroups of %q: tablegroup %q has id %q: %w",
				database,
				w.Name,
				w.TablegroupID,
				err,
			)
		}
		out = append(out, TableGroup{ID: id, Name: w.Name, Shards: w.Shards})
	}
	return out, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
