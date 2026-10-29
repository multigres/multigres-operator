package images

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestDefaultsFromEnv(t *testing.T) {
	t.Run("no overrides returns compiled defaults", func(t *testing.T) {
		set, overrides := DefaultsFromEnv()
		if set != CompiledDefaults() {
			t.Errorf("expected compiled defaults, got %+v", set)
		}
		if len(overrides) != 0 {
			t.Errorf("expected no overrides, got %v", overrides)
		}
	})

	t.Run("env override replaces single component", func(t *testing.T) {
		t.Setenv(EnvPostgresImage, "custom/pgctld:v9")
		t.Setenv(EnvMultigatewayImage, "  custom/gateway:v9  ")

		set, overrides := DefaultsFromEnv()
		if set.Postgres != "custom/pgctld:v9" {
			t.Errorf("postgres override not applied: %s", set.Postgres)
		}
		if set.Multigateway != "custom/gateway:v9" {
			t.Errorf("multigateway override not trimmed/applied: %s", set.Multigateway)
		}
		if set.Multiadmin != CompiledDefaults().Multiadmin {
			t.Errorf("multiadmin should keep compiled default, got %s", set.Multiadmin)
		}
		if len(overrides) != 2 {
			t.Errorf("expected 2 active overrides, got %v", overrides)
		}
	})
}

func TestPgBackRestdImage(t *testing.T) {
	if CompiledDefaults().PgBackRestd != "" {
		t.Errorf("default sets must not carry pgbackrestd, got %s", CompiledDefaults().PgBackRestd)
	}
	t.Setenv("MULTIGRES_IMAGE_PGBACKRESTD", "custom/multigres:v9")
	if set, overrides := DefaultsFromEnv(); set.PgBackRestd != "" || len(overrides) != 0 {
		t.Errorf(
			"an environment variable must not set an independent pgbackrestd default: %v",
			overrides,
		)
	}

	if FromSpec(multigresv1alpha1.ClusterImages{PgBackRestd: "x"}).PgBackRestd != "x" {
		t.Error("FromSpec drops pgbackrestd")
	}

	// Revision identifies the default set, which never carries pgbackrestd, so
	// existing acknowledgedRevision values stay valid.
	changed := CompiledDefaults()
	changed.PgBackRestd = "other/multigres:v1"
	if Revision(changed) != Revision(CompiledDefaults()) {
		t.Error("revision must ignore pgbackrestd")
	}

	spec := multigresv1alpha1.ClusterImages{}
	Complete(&spec, CompiledDefaults())
	if spec.PgBackRestd != "" {
		t.Errorf("Complete must not fill pgbackrestd: %q", spec.PgBackRestd)
	}
	DerivePgBackRestd(&spec)
	if spec.PgBackRestd != spec.Multipooler || spec.Multipooler == "" {
		t.Errorf("DerivePgBackRestd = %q, want multipooler %q", spec.PgBackRestd, spec.Multipooler)
	}

	explicit := multigresv1alpha1.ClusterImages{Multipooler: "m", PgBackRestd: "x"}
	DerivePgBackRestd(&explicit)
	if explicit.PgBackRestd != "x" {
		t.Errorf("explicit pgbackrestd overwritten: %q", explicit.PgBackRestd)
	}
}

func TestRevision(t *testing.T) {
	base := CompiledDefaults()
	rev := Revision(base)
	if len(rev) != 12 {
		t.Fatalf("expected 12-char revision, got %q", rev)
	}
	if Revision(base) != rev {
		t.Error("revision is not deterministic")
	}

	changed := base
	changed.Postgres = "other/pgctld:v1"
	if Revision(changed) == rev {
		t.Error("revision did not change when an image changed")
	}
}

func TestIsComplete(t *testing.T) {
	if !IsComplete(CompiledDefaults()) {
		t.Error("compiled defaults must always form a complete set")
	}
	partial := CompiledDefaults()
	partial.Multipooler = ""
	if IsComplete(partial) {
		t.Error("a set with an empty component must not be complete")
	}
	if IsComplete(multigresv1alpha1.ComponentImages{}) {
		t.Error("the zero set must not be complete")
	}
}

func TestComplete(t *testing.T) {
	defaults := CompiledDefaults()

	t.Run("fills unset fields", func(t *testing.T) {
		spec := multigresv1alpha1.ClusterImages{}
		Complete(&spec, defaults)
		if spec.Postgres != defaults.Postgres || spec.Multigateway != defaults.Multigateway {
			t.Errorf("unset fields not filled: %+v", spec)
		}
	})

	t.Run("explicit values win", func(t *testing.T) {
		spec := multigresv1alpha1.ClusterImages{Postgres: "pinned/pgctld:v1"}
		Complete(&spec, defaults)
		if spec.Postgres != "pinned/pgctld:v1" {
			t.Errorf("explicit value overwritten: %s", spec.Postgres)
		}
		if spec.Multiorch != defaults.Multiorch {
			t.Errorf("unset field not filled: %s", spec.Multiorch)
		}
	})
}
