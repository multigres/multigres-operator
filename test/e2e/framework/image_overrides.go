//go:build e2e

package framework

import (
	"os"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

const (
	postgresImageEnv      = "E2E_POSTGRES_IMAGE"
	multiadminImageEnv    = "E2E_MULTIADMIN_IMAGE"
	multiadminWebImageEnv = "E2E_MULTIADMIN_WEB_IMAGE"
	multiorchImageEnv     = "E2E_MULTIORCH_IMAGE"
	multipoolerImageEnv   = "E2E_MULTIPOOLER_IMAGE"
	multigatewayImageEnv  = "E2E_MULTIGATEWAY_IMAGE"
)

type imageOverrides struct {
	postgres      string
	multiadmin    string
	multiadminWeb string
	multiorch     string
	multipooler   string
	multigateway  string
}

func imageOverridesFromEnv() imageOverrides {
	return imageOverrides{
		postgres:      os.Getenv(postgresImageEnv),
		multiadmin:    os.Getenv(multiadminImageEnv),
		multiadminWeb: os.Getenv(multiadminWebImageEnv),
		multiorch:     os.Getenv(multiorchImageEnv),
		multipooler:   os.Getenv(multipoolerImageEnv),
		multigateway:  os.Getenv(multigatewayImageEnv),
	}
}

func applyImageOverrides(cluster *multigresv1alpha1.MultigresCluster) {
	overrides := imageOverridesFromEnv()
	if overrides.postgres != "" {
		cluster.Spec.Images.Postgres = multigresv1alpha1.ImageRef(overrides.postgres)
	}
	if overrides.multiadmin != "" {
		cluster.Spec.Images.Multiadmin = multigresv1alpha1.ImageRef(overrides.multiadmin)
	}
	if overrides.multiadminWeb != "" {
		cluster.Spec.Images.MultiadminWeb = multigresv1alpha1.ImageRef(overrides.multiadminWeb)
	}
	if overrides.multiorch != "" {
		cluster.Spec.Images.Multiorch = multigresv1alpha1.ImageRef(overrides.multiorch)
	}
	if overrides.multipooler != "" {
		cluster.Spec.Images.Multipooler = multigresv1alpha1.ImageRef(overrides.multipooler)
	}
	if overrides.multigateway != "" {
		cluster.Spec.Images.Multigateway = multigresv1alpha1.ImageRef(overrides.multigateway)
	}
}

// runtimeImages returns exactly the runtime images needed by an e2e cluster.
// Defaults are retained for components without an override, and duplicate
// references are removed before loading them into Kind.
func runtimeImages() []string {
	overrides := imageOverridesFromEnv()
	images := []string{testutil.MultigresImages[3]} // etcd is not overridable.

	if overrides.postgres == "" {
		images = append(images, testutil.MultigresImages[1])
	} else {
		images = append(images, overrides.postgres)
	}
	if overrides.multiadminWeb == "" {
		images = append(images, testutil.MultigresImages[2])
	} else {
		images = append(images, overrides.multiadminWeb)
	}

	goComponentOverrides := []string{
		overrides.multiadmin,
		overrides.multiorch,
		overrides.multipooler,
		overrides.multigateway,
	}
	for _, image := range goComponentOverrides {
		if image == "" {
			images = append(images, testutil.MultigresImages[0])
		} else {
			images = append(images, image)
		}
	}

	seen := make(map[string]struct{}, len(images))
	unique := make([]string, 0, len(images))
	for _, image := range images {
		if _, ok := seen[image]; ok {
			continue
		}
		seen[image] = struct{}{}
		unique = append(unique, image)
	}
	return unique
}
