---
name: triage_logs_with_stern
description: Tail and triage recurring errors across many pooler/postgres pods at once using stern. Use this when the user reports scattered or fleet-wide errors, wants to find the dominant recurring log pattern behind a symptom, asks to debug pooler or postgres ERROR logs, or mentions stern.
---

# Triage Logs with Stern Skill

**Goal:** Tail logs across many pool pods at once, strip the variable parts of each error line, and rank by frequency to find the one recurring pattern that explains the symptom — instead of reading pod-by-pod.

> This complements `diagnose_with_observer`'s `references/log-tracing.md`, which traces a *known* failure through a single call chain (gateway → multiorch → pooler → postgres). Use this skill instead when you don't yet know which component or pod is at fault and need to scan broadly first.

## 1. Ensure Stern is Available

```bash
which stern || brew install stern
```

## 2. Tail Broadly, Filter by Level

Stern's first positional arg is a pod-name regex (not a label selector), so it matches across shards/cells without needing exact pod names:

```bash
KUBECONFIG=kubeconfig.yaml stern 'mgc-.*-postgres-.*-inf-p' -i "error"
```

To scope to a single cluster or component instead, use `-l` with the same labels as the rest of the toolchain:

```bash
KUBECONFIG=kubeconfig.yaml stern -l app.kubernetes.io/component=shard-pool -i "error"
```

Useful flags:

| Flag | Effect |
|------|--------|
| `-i "<pattern>"` | Case-insensitive include filter on log line |
| `-e "<pattern>"` | Exclude filter (strip known-noisy lines) |
| `-c postgres` / `-c multipooler` | Restrict to one container in multi-container pods |
| `--since 1h` | Limit to a time window instead of streaming live |
| `--no-follow` | One-shot dump instead of tailing (best for triage) |
| `-o extended` | Include pod/container/timestamp on every line |

For triage, prefer a bounded, non-following dump over live tailing:

```bash
KUBECONFIG=kubeconfig.yaml stern 'mgc-.*-postgres-.*-inf-p' -i "error" --since 1h --no-follow -o extended > /tmp/pooler-errors.log
```

## 3. Rank Recurring Patterns

Raw error lines differ by pod name, timestamp, and IDs, which hides the fact that they're the same root cause. Strip the variable tokens, then count:

```bash
grep -oE '"message":"[^"]+"' /tmp/pooler-errors.log \
  | sed -E 's/[0-9a-f]{8,}|schema "[^"]+"/<VAR>/g' \
  | sort | uniq -c | sort -rn | head -20
```

The message with the highest count is almost always the actual root cause — one pattern hitting every pod outranks incidental one-off errors. This is exactly how `schema "pg_pgrst_no_exposed_schemas" does not exist` was found: it was the only message that recurred across every pooler, once schema names were normalized to `<VAR>`.

## 4. Trace the Pattern to Code

Once you have the dominant message, grep for the literal string (or the closest static substring around the variable part) in the operator and upstream multigres source to find where it's emitted and why:

```bash
grep -rn "does not exist" --include="*.go" .
```

Cross-reference with `references/known-patterns.md` for signatures already triaged before, and with `diagnose_with_observer/references/code-investigation.md` to decide whether the fix belongs in the operator or upstream.

## 5. Report

State the dominant pattern, its occurrence count relative to total error lines, the pods/components it spans, and the code location — not a list of every distinct error seen.
