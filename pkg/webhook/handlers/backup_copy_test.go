package handlers

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// admissionChain runs a write through the defaulting and then the validating
// webhook handler, as the API server does, and returns the object the API
// server would store.
type admissionChain struct {
	defaulter *MultigresClusterDefaulter
	validator *MultigresClusterValidator
}

func newAdmissionChain(t *testing.T) admissionChain {
	t.Helper()
	scheme := setupScheme()
	_ = storagev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "standard",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: "k8s.io/fake",
		},
	).Build()
	return admissionChain{
		defaulter: NewMultigresClusterDefaulter(resolver.NewResolver(c, "default")),
		validator: NewMultigresClusterValidator(c),
	}
}

func (a admissionChain) create(
	t *testing.T,
	obj *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.MultigresCluster, error) {
	t.Helper()
	stored := obj.DeepCopy()
	if err := a.defaulter.Default(t.Context(), stored); err != nil {
		return nil, err
	}
	_, err := a.validator.ValidateCreate(t.Context(), stored)
	return stored, err
}

func (a admissionChain) update(
	t *testing.T,
	old, obj *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.MultigresCluster, error) {
	t.Helper()
	stored, _, err := a.updateWithWarnings(t, old, obj)
	return stored, err
}

func (a admissionChain) updateWithWarnings(
	t *testing.T,
	old, obj *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.MultigresCluster, admission.Warnings, error) {
	t.Helper()
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	ctx := admission.NewContextWithRequest(t.Context(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Update,
			OldObject: runtime.RawExtension{Raw: raw},
		},
	})
	stored := obj.DeepCopy()
	if err := a.defaulter.Default(ctx, stored); err != nil {
		return nil, nil, err
	}
	warnings, err := a.validator.ValidateUpdate(context.WithoutCancel(ctx), old, stored)
	return stored, warnings, err
}

// backupCopyCluster is a cluster that uses no ShardTemplate, with an S3
// location in spec.backup and whatever the user wrote for the shard.
func backupCopyCluster(
	mode string,
	shardBlock *multigresv1alpha1.BackupConfig,
) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Mode: mode,
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket: "b1", Region: "us-east-1", Endpoint: "https://s3.example",
					KeyPrefix: "prod", CredentialsSecret: "keys",
				},
				PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "tls-1"},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "postgres", Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "default", Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf", Backup: shardBlock}},
				}},
			}},
		},
	}
}

func shardBlockOf(c *multigresv1alpha1.MultigresCluster) *multigresv1alpha1.BackupConfig {
	return c.Spec.Databases[0].TableGroups[0].Shards[0].Backup
}

// locationEdits are the edits of the cluster-level location that a copy of
// its earlier values in a shard block would contradict.
var locationEdits = map[string]func(*multigresv1alpha1.BackupConfig){
	"bucket":             func(b *multigresv1alpha1.BackupConfig) { b.S3.Bucket = "b2" },
	"region":             func(b *multigresv1alpha1.BackupConfig) { b.S3.Region = "eu-west-1" },
	"endpoint":           func(b *multigresv1alpha1.BackupConfig) { b.S3.Endpoint = "https://other.example" },
	"TLS secret":         func(b *multigresv1alpha1.BackupConfig) { b.PgBackRestTLS.SecretName = "tls-2" },
	"TLS secret removed": func(b *multigresv1alpha1.BackupConfig) { b.PgBackRestTLS = nil },
}

// Defaulting does not write a shard's merged backup config into the shard's
// block, so a cluster whose user set the location once, at cluster level, can
// edit it there: nothing below pins the old values.
func TestBackupLocationEdit_ClusterThatNeverSetAShardBlock(t *testing.T) {
	t.Parallel()
	for name, edit := range locationEdits {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := newAdmissionChain(t)
			stored, err := a.create(t, backupCopyCluster("", nil))
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if got := shardBlockOf(stored); got != nil {
				t.Fatalf("defaulting wrote a shard backup block the user did not: %+v", got)
			}
			if stored.Spec.Databases[0].TableGroups[0].Shards[0].Spec == nil {
				t.Fatal("the fixture must take the path that materializes the shard spec")
			}

			edited := stored.DeepCopy()
			edit(edited.Spec.Backup)
			after, err := a.update(t, stored, edited)
			if err != nil {
				t.Fatalf("the edit must be admitted: %v", err)
			}
			if got := shardBlockOf(after); got != nil {
				t.Fatalf("the update wrote a shard backup block: %+v", got)
			}
		})
	}
}

// A stored cluster can carry the merged config in its shard block, written
// there by defaulting when the cluster was created. That block repeats the
// location's old values, so after an edit of the location it would be refused
// as an override naming a block nobody wrote. Defaulting removes it in the
// same update, and the shard inherits the edited location.
func TestBackupLocationEdit_ClusterWithADefaultedCopy(t *testing.T) {
	t.Parallel()
	for name, edit := range locationEdits {
		for _, withPrefix := range []bool{true, false} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				a := newAdmissionChain(t)
				stored, err := a.create(t, backupCopyCluster("", nil))
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				// The copy as defaulting wrote it. One made before the
				// cluster's key prefix was allocated lacks the prefix.
				copied := resolver.InheritedShardBackup(stored.Spec.Backup)
				if !withPrefix {
					copied.S3.KeyPrefix = ""
				}
				stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = copied
				if _, err := a.update(t, stored, stored); err != nil {
					t.Fatalf("the stored cluster with its copy must be valid: %v", err)
				}

				edited := stored.DeepCopy()
				edit(edited.Spec.Backup)
				after, err := a.update(t, stored, edited)
				if err != nil {
					t.Fatalf("the edit must be admitted: %v", err)
				}
				if got := shardBlockOf(after); got != nil {
					t.Fatalf("the stale copy was kept: %+v", got)
				}

				// With no admission request there is no stored object to
				// compare with, and the validator alone refuses the copy
				// and says where it may have come from.
				_, err = a.validator.ValidateCreate(t.Context(), edited)
				if err == nil || !strings.Contains(err.Error(), "written by defaulting") {
					t.Fatalf("want a refusal that explains the copy, got %v", err)
				}
			})
		}
	}
}

// A copy that still agrees with the location is left where it is: removing
// it changes nothing, and a stored object is not rewritten without a reason.
func TestBackupLocationEdit_CopyThatStillAgreesIsKept(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	stored, err := a.create(t, backupCopyCluster("", nil))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = resolver.InheritedShardBackup(
		stored.Spec.Backup,
	)
	edited := stored.DeepCopy()
	edited.Spec.Backup.S3.CredentialsSecret = "other-keys"
	after, err := a.update(t, stored, edited)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if shardBlockOf(after) == nil {
		t.Fatal("a copy that contradicts nothing was removed")
	}
}

// A shard block that set something of its own in the stored object is an
// override somebody wrote. Defaulting leaves it, and it is refused when it
// is introduced and when the location is edited under it.
func TestBackupLocationEdit_UserOverrideIsStillRefused(t *testing.T) {
	t.Parallel()
	own := &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "own-bucket", Region: "us-east-1"},
	}
	a := newAdmissionChain(t)

	if _, err := a.create(t, backupCopyCluster("", own)); err == nil ||
		!strings.Contains(err.Error(), `s3.bucket to "own-bucket"`) {
		t.Fatalf("create with an override: want refusal, got %v", err)
	}

	stored, err := a.create(t, backupCopyCluster("", nil))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	introduced := stored.DeepCopy()
	introduced.Spec.Databases[0].TableGroups[0].Shards[0].Backup = own.DeepCopy()
	after, err := a.update(t, stored, introduced)
	if err == nil || !strings.Contains(err.Error(), `s3.bucket to "own-bucket"`) {
		t.Fatalf("introducing an override: want refusal, got %v", err)
	}
	if shardBlockOf(after) == nil {
		t.Fatal("defaulting removed an override the user wrote")
	}

	// A stored cluster that already has the override, with the location
	// edited under it: the block is not a copy of the old location.
	withOverride := stored.DeepCopy()
	withOverride.Spec.Databases[0].TableGroups[0].Shards[0].Backup = own.DeepCopy()
	edited := withOverride.DeepCopy()
	edited.Spec.Backup.S3.Bucket = "b2"
	after, err = a.update(t, withOverride, edited)
	if err == nil || !strings.Contains(err.Error(), "own-bucket") {
		t.Fatalf("editing under an override: want refusal, got %v", err)
	}
	if got := shardBlockOf(after); got == nil || got.S3.Bucket != "own-bucket" {
		t.Fatalf("defaulting changed the user's override: %+v", got)
	}
}

// Defaulting removes a block only when the update leaves it as it was. A
// block the user adds or changes in the same update that edits the location
// is what they are asking for, even when it says what the location said
// before, and it is validation that answers it.
func TestBackupLocationEdit_BlockWrittenInTheSameUpdateIsKept(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	bare, err := a.create(t, backupCopyCluster("", nil))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	oldValues := resolver.InheritedShardBackup(bare.Spec.Backup)

	// Added: the stored cluster has no shard block.
	added := bare.DeepCopy()
	added.Spec.Backup.S3.Bucket = "b2"
	added.Spec.Databases[0].TableGroups[0].Shards[0].Backup = oldValues.DeepCopy()
	after, err := a.update(t, bare, added)
	if err == nil || !strings.Contains(err.Error(), `s3.bucket to "b1"`) {
		t.Fatalf("adding a block with the old bucket: want refusal, got %v", err)
	}
	if got := shardBlockOf(after); got == nil || got.S3.Bucket != "b1" {
		t.Fatalf("defaulting removed a block written in this update: %+v", got)
	}

	// Edited: the stored cluster has a copy, and the update changes it.
	withCopy := bare.DeepCopy()
	withCopy.Spec.Databases[0].TableGroups[0].Shards[0].Backup = oldValues.DeepCopy()
	edited := withCopy.DeepCopy()
	edited.Spec.Backup.S3.Bucket = "b2"
	shardBlockOf(edited).S3.CredentialsSecret = "shard-keys"
	after, err = a.update(t, withCopy, edited)
	if err == nil || !strings.Contains(err.Error(), `s3.bucket to "b1"`) {
		t.Fatalf("editing the block with the location: want refusal, got %v", err)
	}
	if got := shardBlockOf(after); got == nil || got.S3.CredentialsSecret != "shard-keys" {
		t.Fatalf("defaulting removed a block edited in this update: %+v", got)
	}
}

// What a shard inherits includes its tablegroup's block. A shard block that
// repeats the merge of all three levels is a copy, and is recognised as one
// although it differs from the database's location in what the tablegroup
// set.
func TestBackupLocationEdit_CopyBelowATableGroupBlock(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	stored, err := a.create(t, backupCopyCluster("", nil))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	tg := &stored.Spec.Databases[0].TableGroups[0]
	tg.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3:   &multigresv1alpha1.S3BackupConfig{CredentialsSecret: "tablegroup-keys"},
	}
	tg.Shards[0].Backup = resolver.InheritedShardBackup(
		multigresv1alpha1.MergeBackupConfig(tg.Backup, stored.Spec.Backup),
	)
	if got := tg.Shards[0].Backup.S3.CredentialsSecret; got != "tablegroup-keys" {
		t.Fatalf("the fixture's copy must carry the tablegroup's value, has %q", got)
	}
	if _, err := a.update(t, stored, stored); err != nil {
		t.Fatalf("the stored cluster must be valid: %v", err)
	}
	edited := stored.DeepCopy()
	edited.Spec.Backup.S3.Region = "eu-west-1"
	after, err := a.update(t, stored, edited)
	if err != nil {
		t.Fatalf("the edit must be admitted: %v", err)
	}
	if got := shardBlockOf(after); got != nil {
		t.Fatalf("the stale copy was kept: %+v", got)
	}
}

// In a Lite cluster the shard's block is the location the sidecar serves. A
// stored copy there would keep the repository at the old values after the
// cluster-level location is edited, with nothing reporting that the edit had
// no effect, so it is removed and the edit takes effect. Any edit counts:
// there is no database-level sidecar location whose settings limit the rule.
func TestBackupLocationEdit_LiteCopyFollowsTheEdit(t *testing.T) {
	t.Parallel()
	edits := map[string]func(*multigresv1alpha1.BackupConfig){
		"credentials": func(b *multigresv1alpha1.BackupConfig) { b.S3.CredentialsSecret = "other" },
	}
	for name, edit := range locationEdits {
		edits[name] = edit
	}
	for name, edit := range edits {
		for _, withPrefix := range []bool{true, false} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				a := newAdmissionChain(t)
				stored, err := a.create(t, backupCopyCluster(multigresv1alpha1.ModeLite, nil))
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				copied := resolver.InheritedShardBackup(stored.Spec.Backup)
				if !withPrefix {
					copied.S3.KeyPrefix = ""
				}
				stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = copied

				unchanged, err := a.update(t, stored, stored)
				if err != nil {
					t.Fatalf("update: %v", err)
				}
				if shardBlockOf(unchanged) == nil {
					t.Fatal("an update that edits nothing removed the block")
				}

				edited := stored.DeepCopy()
				edit(edited.Spec.Backup)
				after, err := a.update(t, stored, edited)
				if err != nil {
					t.Fatalf("update: %v", err)
				}
				if got := shardBlockOf(after); got != nil {
					t.Fatalf("the Lite shard still carries the old location: %+v", got)
				}
			})
		}
	}
}

// A Lite shard block that set something of its own is the location someone
// chose for that shard. It stays, and wins over the cluster level, whatever
// is edited above it.
func TestBackupLocationEdit_LiteShardLocationIsKept(t *testing.T) {
	t.Parallel()
	own := &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "own-bucket", Region: "us-east-1", CredentialsSecret: "keys",
		},
	}
	a := newAdmissionChain(t)
	stored, err := a.create(t, backupCopyCluster(multigresv1alpha1.ModeLite, own))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	edited := stored.DeepCopy()
	edited.Spec.Backup.S3.Bucket = "b2"
	after, err := a.update(t, stored, edited)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := shardBlockOf(after); got == nil || got.S3.Bucket != "own-bucket" {
		t.Fatalf("the Lite shard's own location was changed: %+v", got)
	}
}

// sidecarLocationType is the type of the location the shard's pgBackRest
// sidecar serves in c: the database's merged backup config in a distributed
// cluster, the shard's in a Lite one.
func sidecarLocationType(c *multigresv1alpha1.MultigresCluster) multigresv1alpha1.BackupType {
	db := c.Spec.Databases[0]
	database := multigresv1alpha1.MergeBackupConfig(db.Backup, c.Spec.Backup)
	shard := multigresv1alpha1.MergeBackupConfig(
		db.TableGroups[0].Shards[0].Backup,
		multigresv1alpha1.MergeBackupConfig(db.TableGroups[0].Backup, database),
	)
	if multigresv1alpha1.SidecarBackupLocation(c.Spec.Mode, database, shard).ResolvesToS3() {
		return multigresv1alpha1.BackupTypeS3
	}
	return multigresv1alpha1.BackupTypeFilesystem
}

// storedWithCopy is a stored cluster with S3 in spec.backup, or in the
// database's backup below a spec.backup that defaulting filled with the
// filesystem default, and in the shard's block the copy of the merged config
// that earlier operator versions wrote there.
func storedWithCopy(
	t *testing.T, a admissionChain, mode string, s3AtDatabase bool,
) *multigresv1alpha1.MultigresCluster {
	t.Helper()
	submitted := backupCopyCluster(mode, nil)
	if s3AtDatabase {
		submitted.Spec.Databases[0].Backup, submitted.Spec.Backup = submitted.Spec.Backup, nil
	}
	stored, err := a.create(t, submitted)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if stored.Spec.Backup == nil ||
		(s3AtDatabase && stored.Spec.Backup.Type != multigresv1alpha1.BackupTypeFilesystem) {
		t.Fatalf("defaulting left spec.backup as %+v", stored.Spec.Backup)
	}
	stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = resolver.InheritedShardBackup(
		multigresv1alpha1.MergeBackupConfig(stored.Spec.Databases[0].Backup, stored.Spec.Backup),
	)
	if _, err := a.update(t, stored, stored); err != nil {
		t.Fatalf("the stored cluster with its copy must be valid: %v", err)
	}
	return stored
}

func explicitFilesystem() *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
	}
}

// requireBothWaysOut fails unless err is the refusal of a distributed cluster
// left with S3 only in a shard block, naming both ways to resolve it.
func requireBothWaysOut(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("a distributed cluster was admitted with S3 left only in a shard block")
	}
	for _, want := range []string{
		"resolves to the local filesystem",
		"delete the S3 shard backup block",
		"write type: filesystem at a level above it that did not say filesystem before",
		"spec.backup says it by default",
		"configure S3 in spec.backup or the database's backup",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not say %q: %v", want, err)
		}
	}
}

// Taking the S3 block away above a shard block that still says S3 states no
// location, so the shard block is not removed for it: that would move the
// backups to the cluster's own volume on the strength of a deletion. A Lite
// shard keeps its block and stays on S3. A distributed cluster, whose shard
// block selects nothing, is refused with both ways out. The same holds when
// the S3 block was the database's and the filesystem default that defaulting
// stored in spec.backup shows through once it is gone.
//
// Writing type filesystem above the shard block does state a location. The
// shard block the update leaves alone is removed and the sidecar's location
// becomes the filesystem, in both modes, and the same for a block someone
// wrote to set a bucket of the shard's own.
func TestBackupLocationEdit_S3TakenAwayAboveAShardBlock(t *testing.T) {
	t.Parallel()
	const s3, filesystem = multigresv1alpha1.BackupTypeS3, multigresv1alpha1.BackupTypeFilesystem
	type level = func(c *multigresv1alpha1.MultigresCluster) **multigresv1alpha1.BackupConfig
	levels := map[string]level{
		"spec.backup": func(c *multigresv1alpha1.MultigresCluster) **multigresv1alpha1.BackupConfig {
			return &c.Spec.Backup
		},
		"database": func(c *multigresv1alpha1.MultigresCluster) **multigresv1alpha1.BackupConfig {
			return &c.Spec.Databases[0].Backup
		},
		"tablegroup": func(c *multigresv1alpha1.MultigresCluster) **multigresv1alpha1.BackupConfig {
			return &c.Spec.Databases[0].TableGroups[0].Backup
		},
	}
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		lite := mode == multigresv1alpha1.ModeLite
		for _, s3Level := range []string{"spec.backup", "database"} {
			t.Run("mode "+mode+"/S3 at "+s3Level+" removed", func(t *testing.T) {
				t.Parallel()
				a := newAdmissionChain(t)
				stored := storedWithCopy(t, a, mode, s3Level == "database")
				// What is sent for the other levels is what is stored there,
				// the filesystem default in spec.backup included.
				edited := stored.DeepCopy()
				*levels[s3Level](edited) = nil
				after, err := a.update(t, stored, edited)
				if !lite {
					requireBothWaysOut(t, err)
					if shardBlockOf(after) == nil {
						t.Fatal("the S3 shard block was removed for a deleted block above it")
					}

					// The update was refused, so the stored object is
					// unchanged. Saying filesystem where S3 was is one way out.
					*levels[s3Level](edited) = explicitFilesystem()
					after, err = a.update(t, stored, edited)
					if err != nil {
						t.Fatalf("type filesystem above the copy must be admitted: %v", err)
					}
					if shardBlockOf(after) != nil || sidecarLocationType(after) != filesystem {
						t.Fatalf(
							"the copy was kept below type filesystem: %+v",
							shardBlockOf(after),
						)
					}
					// Deleting the block by hand is the other.
					edited = stored.DeepCopy()
					*levels[s3Level](edited) = nil
					edited.Spec.Databases[0].TableGroups[0].Shards[0].Backup = nil
					after, err = a.update(t, stored, edited)
					if err != nil || sidecarLocationType(after) != filesystem {
						t.Fatalf("deleting the shard block by hand must be admitted: %v", err)
					}
					// And putting S3 back, which is the stored object again.
					after, err = a.update(t, stored, stored.DeepCopy())
					if err != nil || shardBlockOf(after) == nil ||
						sidecarLocationType(after) != s3 {
						t.Fatalf(
							"restoring S3 above the copy: %v, block %+v",
							err,
							shardBlockOf(after),
						)
					}
					return
				}
				if err != nil {
					t.Fatalf("a Lite cluster keeps its shard location: %v", err)
				}
				if shardBlockOf(after) == nil || sidecarLocationType(after) != s3 {
					t.Fatalf("the Lite shard left S3 for a deleted block above it: %+v",
						shardBlockOf(after))
				}
				kept := after

				// The stored spec.backup is now the filesystem default that
				// defaulting wrote. Sent back in a later update, as stored or
				// with another size, it is not a location someone states.
				if kept.Spec.Backup == nil || kept.Spec.Backup.Type != filesystem {
					t.Fatalf("defaulting did not fill spec.backup: %+v", kept.Spec.Backup)
				}
				for name, edit := range map[string]func(*multigresv1alpha1.BackupConfig){
					"sent back as stored": func(*multigresv1alpha1.BackupConfig) {},
					"with another size": func(b *multigresv1alpha1.BackupConfig) {
						b.Filesystem.Storage.Size = "20Gi"
					},
				} {
					sentBack := kept.DeepCopy()
					edit(sentBack.Spec.Backup)
					again, err := a.update(t, kept, sentBack)
					if err != nil || shardBlockOf(again) == nil ||
						sidecarLocationType(again) != s3 {
						t.Fatalf("spec.backup %s moved the shard off S3: %v, block %+v",
							name, err, shardBlockOf(again))
					}
				}

				// S3 put back above it: the block stays, and so does S3.
				restored := kept.DeepCopy()
				*levels[s3Level](restored) = (*levels[s3Level](stored)).DeepCopy()
				again, err := a.update(t, kept, restored)
				if err != nil || shardBlockOf(again) == nil || sidecarLocationType(again) != s3 {
					t.Fatalf("restoring S3 above the kept block: %v, block %+v",
						err, shardBlockOf(again))
				}

				// Type filesystem written in a later update at a level that
				// did not say it before is a location the user states: the
				// block goes.
				for name, at := range levels {
					if name == "spec.backup" {
						continue
					}
					explicit := kept.DeepCopy()
					*at(explicit) = explicitFilesystem()
					again, err = a.update(t, kept, explicit)
					if err != nil {
						t.Fatalf("type filesystem at %s must be admitted: %v", name, err)
					}
					if shardBlockOf(again) != nil || sidecarLocationType(again) != filesystem {
						t.Fatalf("the block was kept below type filesystem at %s: %+v",
							name, shardBlockOf(again))
					}

					// Once stored there it is not news: a shard block someone
					// then adds is the shard's own location and stays.
					standing := again.DeepCopy()
					standing.Spec.Databases[0].TableGroups[0].Shards[0].Backup = shardBlockOf(
						kept,
					).DeepCopy()
					unrelated := standing.DeepCopy()
					(*at(unrelated)).Filesystem.Storage.Size = "20Gi"
					again, err = a.update(t, standing, unrelated)
					if err != nil || shardBlockOf(again) == nil ||
						sidecarLocationType(again) != s3 {
						t.Fatalf(
							"a shard location below a standing filesystem block at %s: %v, block %+v",
							name,
							err,
							shardBlockOf(again),
						)
					}
				}
			})
		}
		for name, at := range levels {
			t.Run("mode "+mode+"/type filesystem at "+name, func(t *testing.T) {
				t.Parallel()
				for blockName, block := range map[string]func(
					*multigresv1alpha1.MultigresCluster,
				) *multigresv1alpha1.BackupConfig{
					"a copy": func(c *multigresv1alpha1.MultigresCluster) *multigresv1alpha1.BackupConfig {
						return resolver.InheritedShardBackup(c.Spec.Backup)
					},
					"a block with a bucket of its own": func(
						*multigresv1alpha1.MultigresCluster,
					) *multigresv1alpha1.BackupConfig {
						return &multigresv1alpha1.BackupConfig{
							Type: multigresv1alpha1.BackupTypeS3,
							S3: &multigresv1alpha1.S3BackupConfig{
								Bucket: "own-bucket", Region: "us-east-1", CredentialsSecret: "keys",
							},
						}
					},
				} {
					if !lite && blockName != "a copy" {
						// A distributed cluster does not store such a block.
						continue
					}
					a := newAdmissionChain(t)
					stored, err := a.create(t, backupCopyCluster(mode, nil))
					if err != nil {
						t.Fatalf("create: %v", err)
					}
					stored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = block(stored)
					if _, err := a.update(t, stored, stored); err != nil {
						t.Fatalf("%s: the stored cluster must be valid: %v", blockName, err)
					}
					edited := stored.DeepCopy()
					if name != "spec.backup" {
						// S3 is taken away above, and the level says filesystem.
						edited.Spec.Backup = nil
					}
					*at(edited) = explicitFilesystem()
					after, err := a.update(t, stored, edited)
					if err != nil {
						t.Fatalf(
							"%s: type filesystem at %s must be admitted: %v",
							blockName,
							name,
							err,
						)
					}
					if shardBlockOf(after) != nil || sidecarLocationType(after) != filesystem {
						t.Fatalf("%s: the S3 shard block outlived type filesystem at %s: %+v",
							blockName, name, shardBlockOf(after))
					}

					// A block the same update writes is what the user asks for.
					edited.Spec.Databases[0].TableGroups[0].Shards[0].Backup.S3.Region = "eu-west-1"
					after, err = a.update(t, stored, edited)
					if lite {
						if err != nil || sidecarLocationType(after) != s3 {
							t.Fatalf(
								"%s: a Lite shard block edited in the update: %v",
								blockName,
								err,
							)
						}
					} else if err == nil || shardBlockOf(after) == nil {
						t.Fatalf("%s: a shard block edited in the update was removed or admitted",
							blockName)
					}
				}
			})
		}
	}
}

// A stored spec.backup says filesystem by default, so writing type filesystem
// there in the update that deletes the database's S3 block is not something
// the webhook can tell from sending the stored block back: the shard block
// stays. The refusal a distributed cluster then gets has to say so, or it
// would send the user back into the same refusal.
func TestBackupLocationEdit_FilesystemAtSpecBackupOverTheStoredDefault(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		a := newAdmissionChain(t)
		stored := storedWithCopy(t, a, mode, true)
		edited := stored.DeepCopy()
		edited.Spec.Databases[0].Backup = nil
		edited.Spec.Backup = explicitFilesystem()
		after, err := a.update(t, stored, edited)
		if mode == multigresv1alpha1.ModeLite {
			if err != nil || shardBlockOf(after) == nil ||
				sidecarLocationType(after) != multigresv1alpha1.BackupTypeS3 {
				t.Fatalf("Lite: %v, block %+v", err, shardBlockOf(after))
			}
			continue
		}
		requireBothWaysOut(t, err)

		// What the message says works: type filesystem where the S3 block was.
		edited.Spec.Databases[0].Backup = explicitFilesystem()
		after, err = a.update(t, stored, edited)
		if err != nil || shardBlockOf(after) != nil {
			t.Fatalf("type filesystem at database level: %v, block %+v", err, shardBlockOf(after))
		}
	}
}

// The ways out of a shard block left on S3 are named only when a shard block
// is involved. A distributed cluster whose database block selects the
// filesystem below an S3 spec.backup has no shard block to delete.
func TestBackupLocationEdit_RefusalWithoutAShardBlockNamesNone(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	cluster := backupCopyCluster("", nil)
	cluster.Spec.Databases[0].Backup = explicitFilesystem()
	_, err := a.create(t, cluster)
	if err == nil || !strings.Contains(err.Error(), "resolves to the local filesystem") {
		t.Fatalf("want a filesystem fallback refusal, got %v", err)
	}
	if strings.Contains(err.Error(), "shard backup block, or write") {
		t.Fatalf("the refusal names a shard block the cluster does not have: %v", err)
	}
}

// Once the S3 location above a Lite shard's copy is deleted, the copy is the
// only statement of the shard's location and counts as the shard's own block
// from then on: S3 put back above it with another bucket does not reach the
// shard. Without the deletion in between, the same bucket edit removes the
// copy and the shard follows.
func TestBackupLocationEdit_LiteCopyKeptThroughARemovalIsTheShardsOwn(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	stored := storedWithCopy(t, a, multigresv1alpha1.ModeLite, false)
	bucketOf := func(c *multigresv1alpha1.MultigresCluster) string {
		shard := multigresv1alpha1.MergeBackupConfig(shardBlockOf(c), c.Spec.Backup)
		if shard == nil || shard.S3 == nil {
			return ""
		}
		return shard.S3.Bucket
	}

	other := stored.DeepCopy()
	other.Spec.Backup.S3.Bucket = "b2"
	after, err := a.update(t, stored, other)
	if err != nil || bucketOf(after) != "b2" {
		t.Fatalf("a bucket edit above the copy: %v, bucket %q", err, bucketOf(after))
	}

	removed := stored.DeepCopy()
	removed.Spec.Backup = nil
	kept, err := a.update(t, stored, removed)
	if err != nil || bucketOf(kept) != "b1" {
		t.Fatalf("S3 removed above the copy: %v, bucket %q", err, bucketOf(kept))
	}
	restored := kept.DeepCopy()
	restored.Spec.Backup = other.Spec.Backup.DeepCopy()
	after, err = a.update(t, kept, restored)
	if err != nil || shardBlockOf(after) == nil || bucketOf(after) != "b1" {
		t.Fatalf("S3 restored with another bucket: %v, bucket %q", err, bucketOf(after))
	}

	// Deleting the block is what makes the shard follow again.
	restored.Spec.Databases[0].TableGroups[0].Shards[0].Backup = nil
	after, err = a.update(t, kept, restored)
	if err != nil || bucketOf(after) != "b2" {
		t.Fatalf("shard block deleted: %v, bucket %q", err, bucketOf(after))
	}
}

// An update that moves the backup location above a Lite shard whose own S3
// block stays is admitted and does not move that shard. The update says so
// in a warning, once: a later write that changes nothing above the shard
// does not repeat it.
func TestBackupLocationEdit_LiteShardLeftOnItsOwnLocationIsWarnedAbout(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)
	stored := storedWithCopy(t, a, multigresv1alpha1.ModeLite, false)
	const stillOn = `shard postgres/default/0-inf still backs up to its own S3 location ` +
		`(bucket "b1", prefix "prod"), while the levels above it now resolve to `
	const follow = `; delete the shard's backup block to make it follow them`
	requireWarnings := func(got admission.Warnings, err error, want ...string) {
		t.Helper()
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("warnings:\n got %q\nwant %q", got, want)
		}
	}

	_, warnings, err := a.updateWithWarnings(t, stored, stored)
	requireWarnings(warnings, err)

	removed := stored.DeepCopy()
	removed.Spec.Backup = nil
	kept, warnings, err := a.updateWithWarnings(t, stored, removed)
	requireWarnings(warnings, err, stillOn+"the filesystem"+follow)
	if block := shardBlockOf(kept); block == nil || block.S3.Bucket != "b1" {
		t.Fatalf("the shard block after S3 was removed above it: %+v", block)
	}

	labelled := kept.DeepCopy()
	labelled.Labels = map[string]string{"team": "a"}
	_, warnings, err = a.updateWithWarnings(t, kept, labelled)
	requireWarnings(warnings, err)

	restored := kept.DeepCopy()
	restored.Spec.Backup = stored.Spec.Backup.DeepCopy()
	restored.Spec.Backup.S3.Bucket = "b2"
	after, warnings, err := a.updateWithWarnings(t, kept, restored)
	requireWarnings(warnings, err, stillOn+`S3 bucket "b2", prefix "prod"`+follow)
	if block := shardBlockOf(after); block == nil || block.S3.Bucket != "b1" {
		t.Fatalf("the shard block after S3 came back above it: %+v", block)
	}

	// Another prefix above the shard is another location.
	prefixed := after.DeepCopy()
	prefixed.Spec.Backup.S3.KeyPrefix = "staging"
	_, warnings, err = a.updateWithWarnings(t, after, prefixed)
	requireWarnings(warnings, err, stillOn+`S3 bucket "b2", prefix "staging"`+follow)

	// An edit above the shard that leaves bucket and prefix alone names
	// what it changed.
	elsewhere := after.DeepCopy()
	elsewhere.Spec.Backup.S3.Bucket, elsewhere.Spec.Backup.S3.KeyPrefix = "b1", "prod"
	elsewhere.Spec.Backup.S3.Endpoint = "https://other.example"
	elsewhere.Spec.Backup.S3.Region = "eu-west-1"
	_, warnings, err = a.updateWithWarnings(t, after, elsewhere)
	requireWarnings(warnings, err,
		`shard postgres/default/0-inf still backs up to its own S3 location (bucket "b1", `+
			`prefix "prod", endpoint "https://s3.example", region "us-east-1"), while the `+
			`levels above it now resolve to S3 bucket "b1", prefix "prod", endpoint `+
			`"https://other.example", region "eu-west-1"`+follow)
	regional := after.DeepCopy()
	regional.Spec.Backup.S3.Bucket, regional.Spec.Backup.S3.Region = "b1", "eu-west-1"
	_, warnings, err = a.updateWithWarnings(t, after, regional)
	requireWarnings(warnings, err,
		`shard postgres/default/0-inf still backs up to its own S3 location (bucket "b1", `+
			`prefix "prod", region "us-east-1"), while the levels above it now resolve to `+
			`S3 bucket "b1", prefix "prod", region "eu-west-1"`+follow)

	// A block edited in the same update is what its author asked for.
	rewritten := prefixed.DeepCopy()
	rewritten.Spec.Databases[0].TableGroups[0].Shards[0].Backup.S3.Bucket = "b3"
	_, warnings, err = a.updateWithWarnings(t, after, rewritten)
	requireWarnings(warnings, err)

	// Moving the levels above the shard to where the shard already is
	// leaves nothing to report, and neither does deleting the block.
	back := after.DeepCopy()
	back.Spec.Backup.S3.Bucket = "b1"
	_, warnings, err = a.updateWithWarnings(t, after, back)
	requireWarnings(warnings, err)
	followed := after.DeepCopy()
	followed.Spec.Databases[0].TableGroups[0].Shards[0].Backup = nil
	_, warnings, err = a.updateWithWarnings(t, after, followed)
	requireWarnings(warnings, err)
}

// A copy that follows the edit above it, and a distributed cluster, whose
// shard block the validator refuses instead, get no warning.
func TestBackupLocationEdit_NoWarningWhereTheShardFollowsOrIsRefused(t *testing.T) {
	t.Parallel()
	a := newAdmissionChain(t)

	lite := storedWithCopy(t, a, multigresv1alpha1.ModeLite, false)
	edited := lite.DeepCopy()
	edited.Spec.Backup.S3.Bucket = "b2"
	after, warnings, err := a.updateWithWarnings(t, lite, edited)
	if err != nil || len(warnings) != 0 || shardBlockOf(after) != nil {
		t.Fatalf("a copy that follows: %v, warnings %q, block %+v",
			err, warnings, shardBlockOf(after))
	}

	distributed := storedWithCopy(t, a, "", false)
	_, warnings, err = a.updateWithWarnings(t, distributed, distributed)
	if err != nil || len(warnings) != 0 {
		t.Fatalf("distributed, unchanged: %v, warnings %q", err, warnings)
	}
	removed := distributed.DeepCopy()
	removed.Spec.Backup = nil
	_, warnings, err = a.updateWithWarnings(t, distributed, removed)
	if err == nil || len(warnings) != 0 {
		t.Fatalf("distributed, S3 removed above the block: %v, warnings %q", err, warnings)
	}
	if got := liteShardBackupWarnings(distributed, removed); got != nil {
		t.Fatalf("distributed clusters are refused, not warned: %q", got)
	}
}

// Warnings come in the same order for the same update, whatever order the
// spec lists its shards in.
func TestLiteShardBackupWarnings_AreSorted(t *testing.T) {
	t.Parallel()
	own := func(bucket string) *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: bucket, Region: "us-east-1"},
		}
	}
	old := backupCopyCluster(multigresv1alpha1.ModeLite, nil)
	old.Spec.Databases[0].TableGroups[0].Shards = []multigresv1alpha1.ShardConfig{
		{Name: "b", Backup: own("own-b")},
		{Name: "a", Backup: own("own-a")},
		{Name: "c"},
		// A block that does not resolve to S3 has no S3 location to name.
		{Name: "d", Backup: explicitFilesystem()},
	}
	updated := old.DeepCopy()
	updated.Spec.Backup.S3.Bucket = "b2"
	got := liteShardBackupWarnings(old, updated)
	if len(got) != 2 || !strings.HasPrefix(got[0], "shard postgres/default/a still backs up") ||
		!strings.Contains(got[0], `bucket "own-a"`) ||
		!strings.HasPrefix(got[1], "shard postgres/default/b still backs up") {
		t.Fatalf("warnings: %q", got)
	}
}
