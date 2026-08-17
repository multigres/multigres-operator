package postgresconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// ConfigSplit is a rendered postgresql.conf partitioned by whether each setting
// requires a restart, computed in a single pass.
//
//   - RestartHash covers the effective settings that only take effect after a
//     PostgreSQL restart (postmaster/internal context). It is the only part that
//     feeds the pod spec-hash, so a change confined to reload-safe settings leaves
//     the spec-hash untouched and is applied by reloading the running server.
//   - ReloadHash covers the reload-safe settings (applied in place via SIGHUP).
//   - ReloadSettings is that same reload-safe partition as a name->value map,
//     ready to pass to the multipooler ReloadConfig RPC as expected_settings.
type ConfigSplit struct {
	RestartHash    string
	ReloadHash     string
	ReloadSettings map[string]string
}

// StampAndSplit parses a rendered postgresql.conf into its effective settings (last
// assignment wins, matching how PostgreSQL applies the file) and partitions them
// by RequiresRestart in a single pass, returning both partition hashes and the
// reload-safe settings map — so the restart/reload classification and the parse
// happen exactly once.
//
// It then stamps the config-version marker (see ReloadMarkerGUC): its value is the
// reload-hash, and it is added to BOTH the returned reload-safe settings (sent to
// the pooler as expected_settings) and the returned postgresql.conf (so
// pg_file_settings carries it). This is what makes a reload verifiable even when a
// change only REMOVES a reload-safe setting. Because the marker is stamped AFTER
// the reload-hash is computed, it never feeds back into any hash: the hashes and
// the pod annotations stay a pure function of the real settings. StampAndSplit
// therefore returns the marker-stamped config alongside the split — feed that
// returned string into the ConfigMap.
//
// Parsing to effective settings first is what makes the split robust: a value
// that appears in several layers (template, PostgresConfigRef body, inline map)
// is counted once at its final value, and a purely cosmetic edit (reordering,
// comment, whitespace) that does not change any effective value moves neither
// hash (and so leaves the marker untouched too).
//
// Values are unquoted (undoing the renderer's quote()) so ReloadSettings matches
// PostgreSQL's raw pg_file_settings.setting token when used as expected_settings
// (e.g. '32MB' -> 32MB). TODO: once PostgresConfigRef is removed, build
// expected_settings directly from the (already-unquoted) inline spec.postgresConfig
// map instead — smaller payload, no round-trip; correct only once the ref layer no
// longer contributes reload-safe settings the inline map would omit.
func StampAndSplit(rendered string) (string, ConfigSplit) {
	settings := parseEffectiveConfig(rendered)

	restart := make(map[string]string, len(settings))
	reload := make(map[string]string, len(settings)+1)
	for k, v := range settings {
		v = unquoteValue(v)
		if RequiresRestart(k) {
			restart[k] = v
		} else {
			reload[k] = v
		}
	}

	// Stamp the config-version marker after hashing the real reload-safe settings,
	// so it never feeds back into the reload-hash. It goes into both the expected
	// settings and the file.
	reloadHash := hashSettings(reload)
	reload[ReloadMarkerGUC] = reloadHash
	rendered += reloadMarkerLine(reloadHash)

	return rendered, ConfigSplit{
		RestartHash:    hashSettings(restart),
		ReloadHash:     reloadHash,
		ReloadSettings: reload,
	}
}

// unquoteValue strips the single quotes the renderer wraps values in (undoing
// quote()), so the value matches the raw token PostgreSQL reports in
// pg_file_settings (e.g. '32MB' -> 32MB). A doubled ” inside the quotes is
// PostgreSQL's escape for a literal quote. Unquoted tokens (numbers rendered by
// the template) are returned unchanged.
func unquoteValue(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return strings.ReplaceAll(v[1:len(v)-1], "''", "'")
	}
	return v
}

// parseEffectiveConfig reduces a rendered postgresql.conf to its effective
// key->value settings. Comments and blank lines are ignored, inline "# ..."
// trailers are stripped, and parameter names are lower-cased (PostgreSQL treats
// them case-insensitively) so later assignments overwrite earlier ones by
// canonical key.
func parseEffectiveConfig(rendered string) map[string]string {
	m := make(map[string]string)
	for line := range strings.SplitSeq(rendered, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		m[key] = strings.TrimSpace(stripInlineComment(val))
	}
	return m
}

// stripInlineComment removes a trailing "# ..." comment from a postgresql.conf
// value, leaving a '#' that appears inside a single-quoted string intact. A
// doubled single quote (”) inside a quoted value is PostgreSQL's escape for a
// literal quote and does not end the string.
func stripInlineComment(s string) string {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if inQuote && i+1 < len(s) && s[i+1] == '\'' {
				i++ // escaped '' — stays inside the string
				continue
			}
			inQuote = !inQuote
		case '#':
			if !inQuote {
				return s[:i]
			}
		}
	}
	return s
}

// hashSettings produces a deterministic SHA-256 over a settings map by hashing
// its entries in sorted key order. An empty map yields the SHA-256 of the empty
// input, a stable sentinel meaning "no settings in this partition".
func hashSettings(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		// hash.Hash.Write never returns an error per its interface contract.
		_, _ = fmt.Fprintf(h, "%s=%s\n", k, m[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
