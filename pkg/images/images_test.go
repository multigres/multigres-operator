package images

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestDefaultsFromEnv(t *testing.T) {
	t.Run("no overrides returns compiled defaults", func(t *testing.T) {
		c := assert.NewCollecting(t)
		set, overrides := DefaultsFromEnv()
		c.Eq(CompiledDefaults(), set, "expected compiled defaults, got")
		c.Empty(overrides, "expected no overrides, got")
	})

	t.Run("env override replaces single component", func(t *testing.T) {
		t.Setenv(EnvPostgresImage, "custom/pgctld:v9")
		t.Setenv(EnvMultigatewayImage, "  custom/gateway:v9  ")
		c := assert.NewCollecting(t)

		set, overrides := DefaultsFromEnv()
		c.Eq("custom/pgctld:v9", set.Postgres, "postgres override not applied")
		c.Eq("custom/gateway:v9", set.Multigateway, "multigateway override not trimmed/applied")
		c.Eq(
			CompiledDefaults().Multiadmin,
			set.Multiadmin,
			"multiadmin should keep compiled default, got",
		)
		c.Len(overrides, 2, "expected 2 active overrides, got")
	})
}

func TestRevision(t *testing.T) {
	c := assert.NewCollecting(t)
	base := CompiledDefaults()
	rev := Revision(base)
	c.Require().Len(rev, 12, "expected 12-char revision, got")
	c.Eq(rev, Revision(base), "revision is not deterministic")

	changed := base
	changed.Postgres = "other/pgctld:v1"
	c.NotEq(rev, Revision(changed), "revision did not change when an image changed")
}

func TestIsComplete(t *testing.T) {
	c := assert.NewCollecting(t)
	c.True(IsComplete(CompiledDefaults()), "compiled defaults must always form a complete set")
	partial := CompiledDefaults()
	partial.Multipooler = ""
	c.False(IsComplete(partial), "a set with an empty component must not be complete")
	c.False(IsComplete(multigresv1alpha1.ComponentImages{}), "the zero set must not be complete")
}

func TestComplete(t *testing.T) {
	defaults := CompiledDefaults()

	t.Run("fills unset fields", func(t *testing.T) {
		spec := multigresv1alpha1.ClusterImages{}
		Complete(&spec, defaults)
		assert.NewCollecting(t).
			False(spec.Postgres != defaults.Postgres || spec.Multigateway != defaults.Multigateway, "unset fields not filled: %+v", spec)
	})

	t.Run("explicit values win", func(t *testing.T) {
		c := assert.NewCollecting(t)
		spec := multigresv1alpha1.ClusterImages{Postgres: "pinned/pgctld:v1"}
		Complete(&spec, defaults)
		c.Eq("pinned/pgctld:v1", spec.Postgres, "explicit value overwritten")
		c.Eq(defaults.Multiorch, spec.Multiorch, "unset field not filled")
	})
}
