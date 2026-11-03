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

test('promotion preserves immutable tags and rejects registry errors and stale rolling builds', t => {
  const digest = `sha256:${'a'.repeat(64)}`;
  for (const [existing, error, ref, current, ok] of [
    [digest, '', 'refs/tags/v1.2.3', 'new', true],
    ['', 'manifest unknown', 'refs/tags/v1.2.3', 'new', true],
    [`sha256:${'b'.repeat(64)}`, '', 'refs/tags/v1.2.3', 'new', false],
    ['', 'unauthorized', 'refs/tags/v1.2.3', 'new', false],
    ['', 'connection refused', 'refs/tags/v1.2.3', 'new', false],
    ['', 'host not found', 'refs/tags/v1.2.3', 'new', false],
    ['', 'ERROR: example.test/operator:v1.2.3: not found', 'refs/tags/v1.2.3', 'new', true],
    ['', 'manifest unknown', 'refs/heads/main', 'new', true],
    ['', 'manifest unknown', 'refs/heads/main', 'docs-only-commit', false],
  ]) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-promotion-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
if [ "$3" = inspect ]; then
  if [ -n "$REGISTRY_ERROR" ]; then echo "$REGISTRY_ERROR" >&2; exit 1; fi
  printf '{"digest":"%s"}' "$EXISTING"
else
  printf '%s\\n' "$*" >> "$PROMOTED"
fi
`, { mode: 0o755 });
    fs.writeFileSync(path.join(dir, 'gh'), '#!/bin/bash\nprintf "%s" "$CURRENT"\n', { mode: 0o755 });
    const log = path.join(dir, 'tags');
    fs.writeFileSync(log, '');
    const result = spawnSync('/bin/bash', ['-eo', 'pipefail', '-c', promotion], {
      cwd: dir, encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`,
        IMAGE: 'example.test/operator', RELEASE_TAG: 'v1.2.3', EXPECTED_DIGEST: digest,
        EXISTING: existing, REGISTRY_ERROR: error, PROMOTED: log, GITHUB_REF: ref,
        GITHUB_REPOSITORY: 'example/operator', GITHUB_SHA: 'new', CURRENT: current },
    });
    assert.equal(result.status === 0, ok, `${existing} ${error} ${ref}: ${result.stderr}`);
    const tags = fs.readFileSync(log, 'utf8');
    if (!ok && current !== 'docs-only-commit') assert.equal(tags, '');
    assert.equal(tags.includes('--tag example.test/operator:latest'), ok && ref === 'refs/heads/main');
    if (ok) assert.ok(tags.includes(`example.test/operator@${digest}`));
  }
});
