package topo

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// RegisterCell registers the cell metadata in the global topology.
func RegisterCell(
	ctx context.Context,
	store topoclient.Store,
	recorder record.EventRecorder,
	cell *multigresv1alpha1.Cell,
) error {
	logger := log.FromContext(ctx)

	cellName := string(cell.Spec.Name)

	cellMetadata := cellMetadataFromTopoRefs(
		cellName, cell.Spec.TopoServer, cell.Spec.GlobalTopoServer,
	)

	created, err := createOrUpdateCell(ctx, store, cellName, cellMetadata)
	if err != nil {
		recorder.Eventf(
			cell,
			"Warning",
			"RegistrationFailed",
			"Failed to register cell in topology: %v",
			err,
		)
		return err
	}

	if !created {
		logger.V(1).Info("Updated existing cell in topology", "cellName", cellName)
		return nil
	}

	logger.Info("Cell metadata stored in topology", "cellName", cellName)
	recorder.Eventf(
		cell,
		"Normal",
		"CellRegistered",
		"Registered cell '%s' in topology",
		cellName,
	)
	return nil
}

func cellMetadataFromTopoRefs(
	cellName string,
	localTopo *multigresv1alpha1.LocalTopoServerSpec,
	globalTopo multigresv1alpha1.GlobalTopoServerRef,
) *clustermetadata.Cell {
	if localTopo != nil && localTopo.External != nil && len(localTopo.External.Endpoints) > 0 {
		addresses := make([]string, 0, len(localTopo.External.Endpoints))
		for _, endpoint := range localTopo.External.Endpoints {
			addresses = append(addresses, string(endpoint))
		}
		return &clustermetadata.Cell{
			Name:            cellName,
			ServerAddresses: addresses,
			Root:            localTopo.External.RootPath,
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
					existing.Name = cellMetadata.Name
					existing.ServerAddresses = cellMetadata.ServerAddresses
					existing.Root = cellMetadata.Root
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

// UnregisterCell removes the cell metadata from the global topology.
func UnregisterCell(
	ctx context.Context,
	store topoclient.Store,
	recorder record.EventRecorder,
	cell *multigresv1alpha1.Cell,
) error {
	logger := log.FromContext(ctx)

	cellName := string(cell.Spec.Name)

	if err := store.DeleteCell(ctx, cellName, false); err != nil {
		var topoErr topoclient.TopoError
		if errors.As(err, &topoErr) && topoErr.Code == topoclient.NoNode {
			logger.V(1).Info("Cell does not exist in topology, skipping deletion")
			return nil
		}
		if !IsTopoUnavailable(err) {
			recorder.Eventf(
				cell,
				"Warning",
				"UnregistrationFailed",
				"Failed to remove cell from topology: %v",
				err,
			)
		}
		return fmt.Errorf("failed to delete cell from topology: %w", err)
	}

	logger.Info("Cell metadata removed from topology", "cellName", cellName)
	recorder.Eventf(
		cell,
		"Normal",
		"CellUnregistered",
		"Removed cell '%s' from topology",
		cellName,
	)
	return nil
}
