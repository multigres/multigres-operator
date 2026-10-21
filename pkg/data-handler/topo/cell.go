package topo

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

func cellMetadataFromTopoRefs(
	cellName string,
	localTopo *multigresv1alpha1.LocalTopoServerSpec,
	globalTopo multigresv1alpha1.GlobalTopoServerRef,
	managedTopoAddress string,
	defaultCellRoot string,
) *clustermetadata.Cell {
	if localTopo != nil && localTopo.External != nil && len(localTopo.External.Endpoints) > 0 {
		addresses := make([]string, 0, len(localTopo.External.Endpoints))
		for _, endpoint := range localTopo.External.Endpoints {
			addresses = append(addresses, string(endpoint))
		}
		rootPath := localTopo.External.RootPath
		if rootPath == "" {
			rootPath = defaultCellRoot
		}
		return &clustermetadata.Cell{
			Name:            cellName,
			ServerAddresses: addresses,
			Root:            rootPath,
		}
	}
	if localTopo != nil && localTopo.Etcd != nil && managedTopoAddress != "" {
		rootPath := localTopo.Etcd.RootPath
		if rootPath == "" {
			rootPath = defaultCellRoot
		}
		return &clustermetadata.Cell{
			Name:            cellName,
			ServerAddresses: []string{managedTopoAddress},
			Root:            rootPath,
		}
	}

	return &clustermetadata.Cell{
		Name:            cellName,
		ServerAddresses: []string{globalTopo.Address},
		Root:            globalTopo.RootPath,
	}
}

func createOrUpdateCell(
	ctx context.Context,
	store topoclient.Store,
	cellName string,
	cellMetadata *clustermetadata.Cell,
) (bool, error) {
	if err := store.CreateCell(ctx, cellName, cellMetadata); err != nil {
		var topoErr topoclient.TopoError
		if errors.As(err, &topoErr) && topoErr.Code == topoclient.NodeExists {
			if err := store.UpdateCellFields(
				ctx,
				cellName,
				func(existing *clustermetadata.Cell) error {
					if existing.Name == cellMetadata.Name && existing.Root == cellMetadata.Root &&
						existing.Metadata == cellMetadata.Metadata && slices.Equal(existing.ServerAddresses, cellMetadata.ServerAddresses) {
						return &topoclient.TopoError{Code: topoclient.NoUpdateNeeded}
					}
					existing.Name = cellMetadata.Name
					existing.ServerAddresses = cellMetadata.ServerAddresses
					existing.Root = cellMetadata.Root
					existing.Metadata = cellMetadata.Metadata
					return nil
				},
			); err != nil {
				return false, fmt.Errorf("updating existing cell %s in topology: %w", cellName, err)
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to create cell in topology: %w", err)
	}
	return true, nil
}

// ManagedLocalTopoServerName returns the operator-managed local TopoServer name
// for a Cell resource name.
func ManagedLocalTopoServerName(cellResourceName string) string {
	return name.JoinWithConstraints(name.StatefulSetConstraints, cellResourceName, "topo")
}

// ManagedLocalTopoServerAddress returns the in-cluster client Service address
// for a managed local TopoServer.
func ManagedLocalTopoServerAddress(cellResourceName, namespace string) string {
	return fmt.Sprintf("%s.%s.svc:2379", ManagedLocalTopoServerName(cellResourceName), namespace)
}
