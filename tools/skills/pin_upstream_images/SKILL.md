---
name: pin_upstream_images
description: Pin multigres container image tags in image_defaults.go for operator releases. Compares upstream multigres code changes between the current and new SHA, highlights breaking changes and new features, then updates the tags. Triggered by user requests like "prepare images for release", "pin image tags", "pin upstream images", or "upgrade multigres images".
---

# Pin Upstream Images

Upgrade multigres container image tags in `api/v1alpha1/image_defaults.go` to the latest SHA tags, with an upstream code review between the old and new versions.

## Workflow

### 1. Fetch Latest SHA Tags

Run the fetch script to get the latest SHA tags:

```bash
python3 tools/skills/pin_upstream_images/scripts/fetch_latest_tags.py
```

This outputs `KEY_TAG=sha-XXXXXXX` for each image. The script resolves which `sha-*` tag currently points to the same digest as `main` for each of:
- `ghcr.io/multigres/multigres` -> used by DefaultMultiAdminImage, DefaultMultiOrchImage, DefaultMultiPoolerImage, DefaultMultiGatewayImage
- `ghcr.io/multigres/pgctld` -> used by DefaultPostgresImage
- `ghcr.io/multigres/multiadmin-web` -> used by DefaultMultiAdminWebImage

### 2. Update Image Tags

Immediately update `api/v1alpha1/image_defaults.go` by replacing the old `sha-XXXXXXX` tags with the new ones:

| Constant                   | Image                              | Tag source      |
|----------------------------|------------------------------------|-----------------|
| DefaultPostgresImage       | ghcr.io/multigres/pgctld           | PGCTLD_TAG      |
| DefaultMultiAdminImage     | ghcr.io/multigres/multigres        | MULTIGRES_TAG   |
| DefaultMultiAdminWebImage  | ghcr.io/multigres/multiadmin-web   | MULTIADMIN_WEB_TAG |
| DefaultMultiOrchImage      | ghcr.io/multigres/multigres        | MULTIGRES_TAG   |
| DefaultMultiPoolerImage    | ghcr.io/multigres/multigres        | MULTIGRES_TAG   |
| DefaultMultiGatewayImage   | ghcr.io/multigres/multigres        | MULTIGRES_TAG   |

Do NOT modify DefaultEtcdImage -- it uses a separate versioned release.

If old and new SHAs are identical for all images, inform the user that images are already up to date and stop.

### 3. Upstream Code Comparison

All multigres images (`multigres`, `pgctld`, `multiadmin-web`) are built from the same monorepo: `https://github.com/multigres/multigres`. Perform the comparison there.

1. Clone or pull the upstream multigres repo to `/tmp/multigres`:
   ```bash
   if [ -d /tmp/multigres ]; then
     cd /tmp/multigres && git fetch --all && git checkout main && git pull
   else
     git clone https://github.com/multigres/multigres /tmp/multigres
   fi
   ```

2. Identify the unique old and new SHA values from Step 2. Since `multigres/multigres` and `multigres/pgctld` may have different SHA tags, compare each distinct old->new pair. Typically:
   - **MULTIGRES_TAG** old SHA vs new SHA (covers multiadmin, multiorch, multipooler, multigateway, and pgctld if they share the same SHA)
   - **MULTIADMIN_WEB_TAG** old SHA vs new SHA (if different from the above)

3. For each distinct old->new SHA pair, run:
   ```bash
   cd /tmp/multigres
   git log --oneline <old-sha>..<new-sha>
   git diff --stat <old-sha>..<new-sha>
   ```

4. **Filter: only review changes to multigres binary code.** The images we pin are Go binaries (pgctld, multipooler, multiorch, multigateway, multiadmin, multiadmin-web). Only changes that end up compiled into those binaries can affect the operator. **Ignore** changes to:
   - `demo/` — demo manifests, k8s YAML examples. These are not compiled into any binary.
   - `go/test/` — test-only code, test helpers, end-to-end test infrastructure.
   - `*.md`, `docs/`, `README*` — documentation.
   - CI/CD files, Makefiles, Dockerfiles, scripts.
   - Any file that is not under `go/cmd/`, `go/services/`, `go/common/`, `go/provisioner/`, `go/tools/` (library code used by binaries).

   When listing upstream commits, still include all of them for completeness, but mark test-only or non-binary commits as "No impact (test/docs/demo only)" and skip the detailed diff review for those.

5. For the remaining binary-relevant changes, selectively review the full diff for files that affect the operator (e.g., changes to CLI flags, configuration, proto definitions, RPC interfaces, container entrypoints, health checks, pooler behavior, orchestrator logic, gateway behavior, backup/restore, topology management).

6. If the old and new SHAs are the same for an image group, note that no changes occurred and skip the diff.

### 4. Cross-Reference with Operator Code

This is the critical step. Do NOT make hypothetical recommendations. For each upstream change identified in Step 3, **search the operator codebase** to determine concrete impact.

For every potentially impactful upstream change:

1. **Search the operator code** using `grep_search`, `view_file_outline`, and `view_code_item` to find whether the operator references, uses, or depends on the changed upstream construct (proto field, gRPC service, CLI flag, topology record field, env var, behavior, etc.).

2. **Classify each change with evidence:**
   - **Action required** -- The operator code directly references or depends on something that changed. Cite the exact file and line in the operator. Describe what needs to change and why.
   - **New feature opportunity** -- The upstream change introduces a capability the operator *could* support but currently does not. Cite what the upstream change adds and confirm the operator has no existing support for it.
   - **No impact** -- The upstream change does not touch anything the operator interacts with. Confirm this by showing the search came up empty.

3. **Do not speculate.** Every recommendation must be backed by a concrete search result (or confirmed absence) in the operator codebase. No "if the operator does X" phrasing -- either it does or it doesn't.

4. **Per-container analysis is mandatory.** When an upstream change affects a component's requirements (env vars, flags, volumes, config files), verify the requirement is satisfied on the **specific container** for that component in `containers.go`. Finding a reference elsewhere in the operator is NOT sufficient — each container builder (`buildPgctldSidecar`, `buildMultiPoolerContainer`, `buildMultiOrchContainer`, etc.) is independent. A requirement met on one container does NOT mean it is met on another.

### 4a. Flag Compatibility Audit

**This step is mandatory.** Independent of the diff review above, mechanically verify that every CLI flag the operator passes to upstream components still exists. This catches flag removals/renames that the diff review might miss.

1. Extract every hardcoded `--flag-name` string from `pkg/resource-handler/controller/shard/containers.go`:
   ```bash
   grep -oE '\-\-[a-z][a-z0-9-]+' pkg/resource-handler/controller/shard/containers.go | sort -u
   ```

2. For each upstream component (pgctld, multipooler, multiorch), get its current flag set from the upstream repo at the **new SHA**:
   ```bash
   cd /tmp/multigres
   # pgctld flags
   grep -rh 'FlagName:' go/cmd/pgctld/ go/services/pgctld/ 2>/dev/null | grep -oE '"[a-z][a-z0-9-]+"' | tr -d '"' | sort -u
   # multipooler flags
   grep -rh 'FlagName:' go/services/multipooler/ 2>/dev/null | grep -oE '"[a-z][a-z0-9-]+"' | tr -d '"' | sort -u
   # multiorch flags
   grep -rh 'FlagName:' go/services/multiorch/ 2>/dev/null | grep -oE '"[a-z][a-z0-9-]+"' | tr -d '"' | sort -u
   ```

3. Cross-check: for each flag in the operator's `containers.go`, confirm it exists in the corresponding upstream component's flag set. Flag any mismatches as **Action required — flag removed/renamed upstream**.

### 4b. Environment Variable Compatibility Audit

**This step is mandatory.** Independent of the diff review, mechanically verify that every environment variable required by upstream components is set on the **correct operator container(s)**. This catches cases where an env var exists on one container but a different component now also requires it.

1. Find every required env var in upstream at the **new SHA**:
   ```bash
   cd /tmp/multigres
   # Required env vars (fatal if missing)
   grep -rn 'os.Getenv\|os.LookupEnv' go/services/multipooler/ go/cmd/pgctld/ go/services/multiorch/ go/services/multigateway/ 2>/dev/null | grep -v _test.go
   # Specifically look for hard failures on missing env vars
   grep -rn 'is required\|must be set\|MustGetenv' go/services/multipooler/ go/cmd/pgctld/ go/services/multiorch/ go/services/multigateway/ 2>/dev/null | grep -v _test.go
   ```

2. For each required env var found, check `containers.go` to confirm it is set on the **specific container** for that component:
   - `multipooler` env vars → must be in `buildMultiPoolerContainer`
   - `pgctld` env vars → must be in `buildPgctldSidecar`
   - `multiorch` env vars → must be in `buildMultiOrchContainer`
   - `multigateway` env vars → must be in `buildMultiGatewayContainer` (if exists)

   **Critical**: An env var being set on one container does NOT mean it's set on another. Check each component's container builder independently.

3. Flag any missing env vars as **Action required — env var required by [component] but not set on [container]**.

### 5. Present Results

Present a structured summary to the user:

**Commits between old and new:**
- List of commit messages (from `git log --oneline`)

**Impact Analysis:**
For each upstream change, include:
- What changed upstream (one-liner)
- Operator files affected (with file paths and line references) or "none found"
- Verdict: **Action required** / **New feature opportunity** / **No impact**
- For action-required items: specific description of what needs to change in the operator

**Flag Compatibility:**
- Table of all operator flags vs upstream status (present/missing)
- Any mismatches flagged as action items

**Environment Variable Compatibility:**
- Table of required env vars per component vs operator container status (present/missing)
- Any mismatches flagged as action items

### 5b. Decision Gate — Action Required vs Smoke Test

After presenting the report in Step 5, determine the next step based on the findings:

**If any action-required items were found:**
- Do NOT make code changes. Do NOT run smoke tests.
- Present the action-required items clearly and ask the user for permission to proceed with the fixes.
- Wait for explicit user approval before editing any operator code beyond `image_defaults.go`.
- Once the user approves and fixes are applied, proceed to the smoke test below.

**If no action-required items were found (only no-impact / new-feature-opportunity):**
- Proceed directly to the deployment smoke test to verify everything works end-to-end.

### 5c. Deployment Smoke Test

**Only run this when confident no operator code changes are needed**, or after the user has approved and all action-required fixes have been applied. This is a final verification that the new images work correctly with the operator.

1. Build and deploy to kind:
   ```bash
   make kind-redeploy
   ```
   If no kind cluster exists, use `make kind-deploy`.

2. Apply a sample workload:
   ```bash
   kubectl apply -f config/samples/minimal.yaml
   ```

3. Wait for pods and check health:
   ```bash
   # Wait up to 2 minutes for pods to stabilize
   sleep 60
   kubectl get pods
   ```

4. **If any pool pods are in CrashLoopBackOff or Init:Error:**
   - Check logs: `kubectl logs <pod> -c multipooler` and `kubectl logs <pod> -c postgres`
   - Diagnose the root cause (missing env var, removed flag, changed path, etc.)
   - Report the issue to the user and ask for permission before applying fixes
   - After fixes are approved and applied, re-run `make kind-redeploy` and re-verify
   - **Do NOT proceed to Step 6 until all pods are healthy.**

5. **If all pods reach Running/Ready**, the upgrade is verified. Clean up:
   ```bash
   kubectl delete multigrescluster --all
   ```

### 6. Finalize

1. Display the `image_defaults.go` diff to the user.

2. Suggest a branch name and generate a commit message using the `generate_commit_message` skill:
   - Branch name: `release/pin-image-tags-YYYY-MM-DD`
   - The commit message **must** include:
     - The old->new SHA transitions for each image group
     - A **complete list of upstream commits** (from `git log --oneline`) as bullet points under a "Upstream changes:" section
     - Any **action-required** or **new-feature-opportunity** items called out explicitly
   - Example body structure:
     ```
     deps(images): pin multigres images to sha-XXXXXXX

     Upgrade default container images for pgctld and multigres
     from sha-AAAAAAA to sha-BBBBBBB. multiadmin-web remains at
     sha-CCCCCCC.

     Upstream changes (AAAAAAA..BBBBBBB):
     - fix(component): description of fix
     - feat(component): description of feature
     - refactor(component): description of refactoring
     ...

     Operator impact:
     - No breaking changes detected
     - New feature opportunity: <description if any>
     ```
