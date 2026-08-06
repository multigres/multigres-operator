package shard

// RenderPostgresConfig re-exports the unexported renderPostgresConfig to
// black-box (shard_test) tests so they can reproduce the rendered-config hash
// the controller stamps on pods. Production code uses renderEffectiveConfig /
// renderPostgresConfig directly; this handle exists only in the test binary.
var RenderPostgresConfig = renderPostgresConfig
