const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const workflow = fs.readFileSync('.github/workflows/build-and-release.yaml', 'utf8');
const promotion = workflow.split('      - name: Promote only verified digests\n')[1]
  .split('        run: |\n')[1].split('\n').map(line => line.slice(10)).join('\n');

test('docs-only main pushes schedule a replacement for a superseded publication', () => {
  const main = fs.readFileSync('.github/workflows/main.yaml', 'utf8');
  const trigger = main.split('\non:\n')[1].split('\npermissions:')[0];
  assert.match(trigger, /^  push:/m);
  assert.match(trigger, /^    branches:\n      - main$/m);
  // The stale-SHA guards reject A after any newer main commit B. Filtering
  // README.md or notes.txt would leave no B run to finish publication.
  assert.doesNotMatch(trigger, /^    paths(?:-ignore)?:/m);
});

test('builder notification remains after current publication with the full source SHA', () => {
  const main = fs.readFileSync('.github/workflows/main.yaml', 'utf8');
  const notify = main.split('\n  notify-builder:\n')[1].split('\n  coverage:\n')[0];
  assert.match(notify, /needs: update-current-release/);
  assert.match(notify, /COMMIT_SHA: \$\{\{ github.sha \}\}/);
  assert.match(notify, /client_payload\[sha\]=\$COMMIT_SHA/);
  assert.match(main.split('\n  update-current-release:\n')[1], /^    needs: run$/m);
});

test('promotion preserves immutable tags and rejects registry errors and stale rolling builds', t => {
  const digest = `sha256:${'a'.repeat(64)}`;
  const sha = '1'.repeat(40);
  for (const [existing, error, ref, current, ok] of [
    [digest, '', 'refs/tags/v1.2.3', sha, true],
    ['', 'manifest unknown', 'refs/tags/v1.2.3', sha, true],
    [`sha256:${'b'.repeat(64)}`, '', 'refs/tags/v1.2.3', sha, false],
    ['', 'unauthorized', 'refs/tags/v1.2.3', sha, false],
    ['', 'connection refused', 'refs/tags/v1.2.3', sha, false],
    ['', 'host not found', 'refs/tags/v1.2.3', sha, false],
    ['', 'not-found', 'refs/tags/v1.2.3', sha, true],
    ['', 'manifest unknown', 'refs/heads/main', sha, true],
    ['', 'manifest unknown', 'refs/heads/main', 'docs-only-commit', false],
  ]) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-promotion-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
if [ "$3" = inspect ]; then
  if [ "$REGISTRY_ERROR" = not-found ]; then echo "ERROR: $4: not found" >&2; exit 1; fi
  if [ -n "$REGISTRY_ERROR" ]; then echo "$REGISTRY_ERROR" >&2; exit 1; fi
  printf '{"digest":"%s"}' "$EXISTING"
else
  printf '%s\\n' "$*" >> "$PROMOTED"
fi
`, { mode: 0o755 });
    fs.writeFileSync(path.join(dir, 'gh'), '#!/bin/bash\nprintf "%s" "$CURRENT"\n', { mode: 0o755 });
    const log = path.join(dir, 'tags');
    fs.writeFileSync(log, '');
    const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', promotion], {
      cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
        IMAGE: 'example.test/operator', RELEASE_TAG: ref === 'refs/heads/main' ? sha : 'v1.2.3', EXPECTED_DIGEST: digest,
        EXISTING: existing, REGISTRY_ERROR: error, PROMOTED: log, GITHUB_REF: ref,
        GITHUB_REPOSITORY: 'example/operator', GITHUB_SHA: sha, CURRENT: current },
    });
    assert.equal(result.status === 0, ok, `${existing} ${error} ${ref}: ${result.stderr}`);
    const tags = fs.readFileSync(log, 'utf8');
    if (!ok && current !== 'docs-only-commit') assert.equal(tags, '');
    assert.equal(tags.includes('--tag example.test/operator:latest'), ok && ref === 'refs/heads/main');
    if (ok) {
      assert.ok(tags.includes(`--tag example.test/operator:${sha} example.test/operator@${digest}`));
      assert.equal(tags.includes('--tag example.test/operator:v1.2.3 '), ref === 'refs/tags/v1.2.3');
    }
  }
});

test('a stable alias conflict cannot change the existing commit-SHA image', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-conflict-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const sha = '2'.repeat(40);
  const digest = `sha256:${'a'.repeat(64)}`;
  fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
if [ "$3" = inspect ]; then
  if [[ "$4" == *:v1.2.3 ]]; then
    printf '{"digest":"sha256:${'b'.repeat(64)}"}'
  else
    printf '{"digest":"${digest}"}'
  fi
else
  echo changed >> "$PROMOTED"
fi
`, { mode: 0o755 });
  const log = path.join(dir, 'tags');
  const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', promotion], {
    cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
      IMAGE: 'example.test/operator', RELEASE_TAG: 'v1.2.3', EXPECTED_DIGEST: digest,
      GITHUB_SHA: sha, PROMOTED: log },
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr + result.stdout, /Refusing to replace/);
  assert.equal(fs.existsSync(log), false);
});

test('installers retain the lane tag for consumer pin checks and the digest for direct installs', t => {
  const step = workflow.split('      - name: Generate tagged and digest-pinned install manifests\n')[1]
    .split('\n      - uses:')[0];
  assert.match(step, /RELEASE_TAG: \$\{\{ inputs.dry-run && github.sha \|\| inputs.release-tag \}\}/);
  assert.match(step, /DIGEST: \$\{\{ needs.build.outputs.digest \}\}/);
  const generate = step
    .split('        run: |\n')[1].split('\n      - uses:')[0]
    .split('\n').map(line => line.slice(10)).join('\n');
  const sha = '3'.repeat(40);
  const digest = `sha256:${'d'.repeat(64)}`;
  for (const tag of [sha, 'v0.11.0', 'v0.12.0-rc.1']) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-installer-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    // Stand in for Kustomize here; real generated installers are also checked
    // by the workflow before uploading them.
    fs.writeFileSync(path.join(dir, 'make'), `#!/bin/bash
set -eu
image="\${2#IMG=}"
mkdir -p dist
for file in install.yaml install-certmanager.yaml install-observability.yaml; do
  printf 'image: %s\\nimage: %s\\n' "$image" "\${GC_IMAGE:-$image}" > "dist/$file"
done
printf 'image:\\nimage: grafana/grafana:11.4.0\\n' >> dist/install-observability.yaml
`, { mode: 0o755 });
    const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', generate], {
      cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
        IMAGE: 'ghcr.io/multigres/multigres-operator', GITHUB_SHA: sha, RELEASE_TAG: tag, DIGEST: digest },
    });
    assert.equal(result.status, 0, result.stderr);
    const manifest = fs.readFileSync(path.join(dir, 'dist/install.yaml'), 'utf8');
    // The consumer extracts the tag up to '@'; Kubernetes uses the digest.
    assert.deepEqual([...new Set([...manifest.matchAll(/ghcr\.io\/multigres\/multigres-operator:([\w.-]+)/g)]
      .map(match => match[1]))], [tag]);
    assert.deepEqual(manifest.trim().split('\n'),
      Array(2).fill(`image: ghcr.io/multigres/multigres-operator:${tag}@${digest}`));
    for (const gcImage of [
      `ghcr.io/multigres/multigres-operator:${tag}`,
      `ghcr.io/multigres/multigres-operator:${tag}@sha256:${'e'.repeat(64)}`,
      `ghcr.io/multigres/multigres-operator:latest@${digest}`,
    ]) {
      const rejected = spawnSync('/bin/bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', generate], {
        cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
          IMAGE: 'ghcr.io/multigres/multigres-operator', GITHUB_SHA: sha,
          RELEASE_TAG: tag, DIGEST: digest, GC_IMAGE: gcImage },
      });
      assert.notEqual(rejected.status, 0, `accepted mismatching GC image: ${gcImage}`);
    }
  }
});

test('stable releases reuse only the commit image accepted by signature and source verification', t => {
  const reuse = workflow.split('      - name: Reuse the verified main image for a stable release\n')[1]
    .split('        run: |\n')[1].split('\n      - name:')[0]
    .split('\n').map(line => line.slice(10)).join('\n');
  const build = workflow.split('      - name: Build OCI archive without publishing\n')[1]
    .split('\n      - uses:')[0];
  assert.match(build, /if: \$\{\{ !startsWith\(github.ref, 'refs\/tags\/'\) \}\}/);
  const sha = '4'.repeat(40);
  const digest = `sha256:${'c'.repeat(64)}`;
  for (const failure of ['', 'missing', 'signature', 'provenance']) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-reuse-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
set -eu
if [ "$1" = buildx ]; then
  test "$4" = "ghcr.io/multigres/multigres-operator:$GITHUB_SHA"
  test "$FAILURE" != missing
  printf '{"digest":"${digest}"}'
else
  [[ "$*" == *"copy --all --preserve-digests docker://ghcr.io/multigres/multigres-operator@${digest} oci-archive:/work/operator.tar" ]]
  touch operator.tar
fi
`, { mode: 0o755 });
    fs.writeFileSync(path.join(dir, 'cosign'), `#!/bin/bash
test "$FAILURE" != signature
`, { mode: 0o755 });
    fs.writeFileSync(path.join(dir, 'gh'), `#!/bin/bash
set -eu
test "$FAILURE" != provenance
[[ "$*" == *"--source-ref refs/heads/main --source-digest $GITHUB_SHA --deny-self-hosted-runners" ]]
[[ "$*" == *"--signer-digest $GITHUB_SHA"* ]]
`, { mode: 0o755 });
    const output = path.join(dir, 'outputs');
    const result = spawnSync('/bin/bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', reuse], {
      cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
        GITHUB_REPOSITORY: 'multigres/multigres-operator', REPOSITORY: 'multigres/multigres-operator',
        GITHUB_SHA: sha, GITHUB_OUTPUT: output, FAILURE: failure },
    });
    assert.equal(result.status === 0, !failure, result.stderr);
    assert.equal(fs.existsSync(path.join(dir, 'operator.tar')), !failure);
    if (!failure) assert.equal(fs.readFileSync(output, 'utf8'), `digest=${digest}\n`);
  }
});
