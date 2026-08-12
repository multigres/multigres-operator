package postgresconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// ConfigHashes are the two content hashes the operator derives from a rendered
// postgresql.conf. RestartHash covers the effective settings that only take
// effect after a PostgreSQL restart; ReloadHash covers the settings a reload
// (SIGHUP) applies in place. Splitting them lets a reload-only change avoid pod
// recreation: only RestartHash feeds the pod spec-hash, so a change confined to
// reload-safe settings leaves the spec-hash untouched and is applied by
// reloading the running server instead.
type ConfigHashes struct {
	RestartHash string
	ReloadHash  string
}

// SplitHashes parses a rendered postgresql.conf into its effective settings
// (last assignment wins, matching how PostgreSQL applies the file) and
// partitions them by RequiresRestart into two independent SHA-256 hashes.
//
// Parsing to effective settings first is what makes the split robust: a value
// that appears in several layers (template, PostgresConfigRef body, inline map)
// is counted once at its final value, and a purely cosmetic edit (reordering,
// comment, whitespace) that does not change any effective value moves neither
// hash.
func SplitHashes(rendered string) ConfigHashes {
	settings := parseEffectiveConfig(rendered)

	restart := make(map[string]string, len(settings))
	reload := make(map[string]string, len(settings))
	for k, v := range settings {
		if RequiresRestart(k) {
			restart[k] = v
		} else {
			reload[k] = v
		}
	}
	return ConfigHashes{
		RestartHash: hashSettings(restart),
		ReloadHash:  hashSettings(reload),
	}
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
		fmt.Fprintf(h, "%s=%s\n", k, m[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
