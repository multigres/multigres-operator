// Package images resolves the container images the operator deploys for each
// Multigres component.
//
// Component images are operator configuration, not user intent: they change
// with every release, so they are never written into MultigresCluster specs.
// The operator resolves unset spec.images fields at reconcile time from the
// default set built here — compiled-in defaults, each overridable through a
// deployment-level environment variable. Explicit spec.images values always
// win.
package images

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sort"
	"strings"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// UpdateStrategy controls when clusters pick up a changed default image set.
type UpdateStrategy string

const (
	// UpdateImmediate applies the operator's current default image set on
	// every reconcile. Changing operator configuration rolls all clusters.
	UpdateImmediate UpdateStrategy = "immediate"

	// UpdateLazy keeps each cluster on the default image set it is already
	// running until spec.imageUpdatePolicy.acknowledgedRevision names the new
	// set's revision.
	UpdateLazy UpdateStrategy = "lazy"
)

// Environment variables that override the compiled-in default image for a
// single component on the operator deployment.
const (
	EnvPostgresImage      = "MULTIGRES_IMAGE_POSTGRES"
	EnvMultiadminImage    = "MULTIGRES_IMAGE_MULTIADMIN"
	EnvMultiadminWebImage = "MULTIGRES_IMAGE_MULTIADMIN_WEB"
	EnvMultiorchImage     = "MULTIGRES_IMAGE_MULTIORCH"
	EnvMultipoolerImage   = "MULTIGRES_IMAGE_MULTIPOOLER"
	EnvMultigatewayImage  = "MULTIGRES_IMAGE_MULTIGATEWAY"
)

// Config is the operator's image configuration, resolved once at startup.
type Config struct {
	// Defaults is the default image set: compiled-in values with any
	// environment overrides applied.
	Defaults multigresv1alpha1.ComponentImages

	// Strategy controls when clusters adopt a changed default set.
	Strategy UpdateStrategy
}

// CompiledDefaults returns the image set compiled into this operator build.
func CompiledDefaults() multigresv1alpha1.ComponentImages {
	return multigresv1alpha1.ComponentImages{
		Postgres:      multigresv1alpha1.DefaultPostgresImage,
		Multiadmin:    multigresv1alpha1.DefaultMultiadminImage,
		MultiadminWeb: multigresv1alpha1.DefaultMultiadminWebImage,
		Multiorch:     multigresv1alpha1.DefaultMultiorchImage,
		Multipooler:   multigresv1alpha1.DefaultMultipoolerImage,
		Multigateway:  multigresv1alpha1.DefaultMultigatewayImage,
	}
}

// DefaultsFromEnv returns the compiled defaults with environment overrides
// applied, plus the overrides that were active, for startup logging.
func DefaultsFromEnv() (multigresv1alpha1.ComponentImages, map[string]string) {
	set := CompiledDefaults()
	overrides := map[string]string{}

	apply := func(env string, dst *multigresv1alpha1.ImageRef) {
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			*dst = multigresv1alpha1.ImageRef(v)
			overrides[env] = v
		}
	}
	apply(EnvPostgresImage, &set.Postgres)
	apply(EnvMultiadminImage, &set.Multiadmin)
	apply(EnvMultiadminWebImage, &set.MultiadminWeb)
	apply(EnvMultiorchImage, &set.Multiorch)
	apply(EnvMultipoolerImage, &set.Multipooler)
	apply(EnvMultigatewayImage, &set.Multigateway)

	return set, overrides
}

// Revision returns a short content hash identifying an image set. It is the
// value spec.imageUpdatePolicy.acknowledgedRevision must carry for a lazy
// cluster to adopt the set.
//
// This is an internal convenience for compact comparison, logging, and
// display (a printer column, an event, a status field) — the same role
// Deployment's pod-template-hash plays for ReplicaSets. It is not a contract
// external systems should reimplement: anything comparing image sets across
// a process boundary (a version manifest checking a live cluster for drift,
// for example) should compare the actual images in status.images.effective,
// not recompute this hash. String comparison needs no shared algorithm and
// works the same in any language; a hash does, and won't.
//
// The algorithm may change between operator versions without notice.
func Revision(set multigresv1alpha1.ComponentImages) string {
	lines := []string{
		"multiadmin=" + string(set.Multiadmin),
		"multiadminweb=" + string(set.MultiadminWeb),
		"multigateway=" + string(set.Multigateway),
		"multiorch=" + string(set.Multiorch),
		"multipooler=" + string(set.Multipooler),
		"postgres=" + string(set.Postgres),
	}
	sort.Strings(lines)
	h := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(h[:])[:12]
}

// FromSpec returns only the component image fields from a cluster image spec.
// Pull policy and pull secrets do not participate in image-set resolution.
func FromSpec(spec multigresv1alpha1.ClusterImages) multigresv1alpha1.ComponentImages {
	return multigresv1alpha1.ComponentImages{
		Postgres:      spec.Postgres,
		Multiadmin:    spec.Multiadmin,
		MultiadminWeb: spec.MultiadminWeb,
		Multiorch:     spec.Multiorch,
		Multipooler:   spec.Multipooler,
		Multigateway:  spec.Multigateway,
		PgBackRestd:   spec.PgBackRestd,
	}
}

// IsComplete reports whether every component image in the set is non-empty.
// PgBackRestd is not required: it is never part of a default or recorded set,
// only derived per cluster (see DerivePgBackRestd).
// A recorded applied set is only trustworthy when complete: a partial set
// would resolve some components from the record and silently drop the rest
// to compiled-in fallbacks.
func IsComplete(set multigresv1alpha1.ComponentImages) bool {
	return set.Postgres != "" &&
		set.Multiadmin != "" &&
		set.MultiadminWeb != "" &&
		set.Multiorch != "" &&
		set.Multipooler != "" &&
		set.Multigateway != ""
}

// Complete fills unset fields of spec in-memory from the given default set.
// Explicit values are left untouched. Nothing is persisted.
func Complete(spec *multigresv1alpha1.ClusterImages, defaults multigresv1alpha1.ComponentImages) {
	if spec.Postgres == "" {
		spec.Postgres = defaults.Postgres
	}
	if spec.Multiadmin == "" {
		spec.Multiadmin = defaults.Multiadmin
	}
	if spec.MultiadminWeb == "" {
		spec.MultiadminWeb = defaults.MultiadminWeb
	}
	if spec.Multiorch == "" {
		spec.Multiorch = defaults.Multiorch
	}
	if spec.Multipooler == "" {
		spec.Multipooler = defaults.Multipooler
	}
	if spec.Multigateway == "" {
		spec.Multigateway = defaults.Multigateway
	}
}

// DerivePgBackRestd sets an unset pgbackrestd image to the Multipooler image
// of the same resolved set. The sidecar and multipooler talk over a versioned
// connect API and an argv allowlist, so they must be the same multigres build:
// it follows multipooler through every resolution (pinned, lazy record,
// default) and never moves on its own. An explicit value is left untouched.
// Call it after Complete.
func DerivePgBackRestd(spec *multigresv1alpha1.ClusterImages) {
	if spec.PgBackRestd == "" {
		spec.PgBackRestd = spec.Multipooler
	}
}
