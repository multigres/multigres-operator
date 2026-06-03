//go:build e2e

package framework

import (
	"os"
	"strings"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// Data-plane image override env vars.
//
// When set, the e2e harness deploys (and preloads into Kind) the given image
// instead of the operator's compiled-in default from
// api/v1alpha1/image_defaults.go. This lets CI validate a freshly built
// data-plane image (e.g. a SHA-tagged Multigres build) end to end without
// editing the defaults. Unset vars fall back to the operator defaults.
const (
	// EnvMultigresImage overrides the "multigres" mono-image shared by the
	// multigateway, multiorch, multipooler, and multiadmin components.
	EnvMultigresImage = "E2E_MULTIGRES_IMAGE"
	// EnvPgctldImage overrides the pgctld image used by postgres pools.
	EnvPgctldImage = "E2E_PGCTLD_IMAGE"
	// EnvMultiAdminWebImage overrides the multiadmin-web image.
	EnvMultiAdminWebImage = "E2E_MULTIADMINWEB_IMAGE"
)

func envImage(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

// ApplyImageOverrides pins spec.Images to any override images present in the
// environment, so the operator deploys the overridden image instead of its
// compiled-in default. It only touches components whose override env var is
// set; everything else keeps the operator default. No-op when no overrides are
// present.
func ApplyImageOverrides(spec *multigresv1alpha1.MultigresClusterSpec) {
	if v := envImage(EnvMultigresImage); v != "" {
		ref := multigresv1alpha1.ImageRef(v)
		spec.Images.MultiGateway = ref
		spec.Images.MultiOrch = ref
		spec.Images.MultiPooler = ref
		spec.Images.MultiAdmin = ref
	}
	if v := envImage(EnvPgctldImage); v != "" {
		spec.Images.Postgres = multigresv1alpha1.ImageRef(v)
	}
	if v := envImage(EnvMultiAdminWebImage); v != "" {
		spec.Images.MultiAdminWeb = multigresv1alpha1.ImageRef(v)
	}
}

// OverrideImageList returns the set of override images to preload into Kind, in
// addition to the defaults. Empty when no overrides are set. Deduplicated (the
// multigres mono-image is referenced by four components).
func OverrideImageList() []string {
	var out []string
	seen := map[string]bool{}
	add := func(img string) {
		if img == "" || seen[img] {
			return
		}
		seen[img] = true
		out = append(out, img)
	}
	add(envImage(EnvMultigresImage))
	add(envImage(EnvPgctldImage))
	add(envImage(EnvMultiAdminWebImage))
	return out
}
