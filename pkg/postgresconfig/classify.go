package postgresconfig

import "strings"

// restartContexts are the pg_settings "context" values whose parameters only
// take effect after a full PostgreSQL restart. A configuration reload (SIGHUP)
// does not apply them:
//
//   - postmaster: settable only at server start (e.g. shared_buffers,
//     max_connections, wal_level).
//   - internal: compiled-in / read-only (e.g. block_size); never reloadable.
//
// Every other context (sighup, superuser, user, backend, superuser-backend) is
// applied by a reload without a restart.
var restartContexts = map[string]bool{
	"postmaster": true,
	"internal":   true,
}

// RequiresRestart reports whether changing the given parameter requires a
// PostgreSQL restart to take effect, as opposed to a configuration reload
// (SIGHUP). This is the operator's static basis for the reload-vs-restart
// split: parameters that require a restart drive pod recreation, the rest can
// be applied in place by reloading the running server.
//
// Unknown parameters and namespaced (extension) parameters such as "cron.*" or
// "auto_explain.*" are not in the built-in catalog and default to restart. This
// is the conservative choice: reloading a parameter that actually needed a
// restart would silently fail to apply it, whereas needlessly restarting for a
// reload-safe one is merely more disruptive, not incorrect.
func RequiresRestart(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	// Namespaced extension parameters are custom placeholders absent from the
	// built-in catalog; classify conservatively as restart.
	if strings.Contains(lower, ".") {
		return true
	}
	entry, ok := catalog[lower]
	if !ok || entry.context == "" {
		return true
	}
	return restartContexts[entry.context]
}
