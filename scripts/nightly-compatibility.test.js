const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const workflow = fs.readFileSync('.github/workflows/nightly-compatibility.yaml', 'utf8');

// Execute the workflow's actual shell snippets with registry/API stand-ins.
function step(name, key = 'run', indent = 8) {
  const block = workflow.split(`      - name: ${name}\n`)[1]?.split(/\n      - |\n  [a-z]/)[0];
  assert.ok(block, `Missing step ${name}`);
  const code = block.split(`${' '.repeat(indent)}${key}: |\n`)[1];
  assert.ok(code, `Missing ${key} in ${name}`);
  return code.split('\n').map((line) => line.slice(indent + 2)).join('\n');
}

function shell(t, code, env = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'image-promotion-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
if [ "$4" = "$FAIL_IMAGE" ]; then exit 1; fi
printf '%s\\n' "$DOCKER_JSON"
`, { mode: 0o755 });
  fs.writeFileSync(path.join(dir, 'curl'), `#!/bin/bash
case "$*" in
  */commits/*) printf '%s\\n' "$OPERATOR_JSON" ;;
  *) printf '%s\\n' "$NIGHTLY_JSON" ;;
esac
`, { mode: 0o755 });
  fs.writeFileSync(path.join(dir, 'gh'), `#!/bin/bash
printf '%s\\n' "$*" >> "$MOCK_LOG"
if [ "$FAIL_VERIFY" = "true" ]; then exit 1; fi
name="\${3#oci://}"
name="\${name%@*}"
if [ "$WRONG_SUBJECT" = "true" ]; then name=wrong; fi
printf '[{"verificationResult":{"statement":{"subject":[{"name":"%s"}]}}}]' "$name"
`, { mode: 0o755 });
  const output = path.join(dir, 'output');
  const log = path.join(dir, 'log');
  fs.writeFileSync(output, '');
  fs.writeFileSync(log, '');
  const result = spawnSync('bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', code], {
    encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, GITHUB_OUTPUT: output, MOCK_LOG: log, ...env },
  });
  return { ...result, output: fs.readFileSync(output, 'utf8'), log: fs.readFileSync(log, 'utf8') };
}

const images = {
  MULTIGRES_IMAGE: `ghcr.io/multigres/multigres@sha256:${'a'.repeat(64)}`,
  PGCTLD_IMAGE: `ghcr.io/multigres/pgctld@sha256:${'b'.repeat(64)}`,
  MULTIADMIN_WEB_IMAGE: `ghcr.io/multigres/multiadmin-web@sha256:${'c'.repeat(64)}`,
};

test('scheduled resolution freezes the operator SHA and captures the nightly run attempt', (t) => {
  const result = shell(t, step('Resolve upstream SHA'), {
    INPUT_SHA: '', INPUT_OPERATOR_REF: '', GITHUB_EVENT_NAME: 'schedule', GITHUB_SHA: 'a'.repeat(40),
    NIGHTLY_JSON: JSON.stringify({ workflow_runs: [{ head_sha: 'b'.repeat(40), id: 123, run_attempt: 2, conclusion: 'success', head_branch: 'main', event: 'schedule' }] }),
  });
  assert.equal(result.status, 0, result.stderr);
  assert.ok(result.output.includes(`operator_sha=${'a'.repeat(40)}\n`));
  assert.ok(result.output.includes(`sha=${'b'.repeat(40)}\n`));
  assert.ok(result.output.includes('nightly_run_id=123\nnightly_run_attempt=2\n'));
});

test('manual SHA diagnostics do not depend on an available nightly run', (t) => {
  const result = shell(t, step('Resolve upstream SHA'), {
    INPUT_SHA: 'b'.repeat(40), INPUT_OPERATOR_REF: 'feature/skew', GITHUB_EVENT_NAME: 'workflow_dispatch',
    GITHUB_REPOSITORY: 'multigres/multigres-operator', NIGHTLY_JSON: '{}',
    OPERATOR_JSON: JSON.stringify({ sha: 'c'.repeat(40) }),
  });
  assert.equal(result.status, 0, result.stderr);
  assert.ok(result.output.includes(`operator_sha=${'c'.repeat(40)}\n`));
});

for (const invalid of [{}, { head_branch: 'other' }, { conclusion: 'failure' }, { event: 'workflow_dispatch' }]) {
  test(`rejects invalid upstream nightly selection: ${JSON.stringify(invalid)}`, (t) => {
    const candidate = Object.keys(invalid).length ? { head_sha: 'b'.repeat(40), conclusion: 'success', head_branch: 'main', event: 'schedule', ...invalid } : undefined;
    const result = shell(t, step('Resolve upstream SHA'), {
      INPUT_SHA: '', NIGHTLY_JSON: JSON.stringify({ workflow_runs: candidate ? [candidate] : [] }),
    });
    assert.notEqual(result.status, 0);
    assert.equal(result.output, '');
  });
}

for (const failed of Object.values(images)) {
  test(`digest resolution failure is not swallowed: ${failed.split('@')[0]}`, (t) => {
    const result = shell(t, step('Resolve immutable nightly image digests'), {
      ...images, FAIL_IMAGE: failed, DOCKER_JSON: JSON.stringify({ digest: `sha256:${'a'.repeat(64)}` }),
    });
    assert.notEqual(result.status, 0);
    assert.equal(result.output, '');
  });
}

for (const platforms of [['amd64', 'arm64'], ['amd64'], ['arm64'], []]) {
  test(`architecture preflight with ${platforms.join('/') || 'no platforms'}`, (t) => {
    const result = shell(t, step('Verify both supported architectures'), {
      ...images, DOCKER_JSON: JSON.stringify({ manifests: platforms.map((architecture) => ({ platform: { os: 'linux', architecture } })) }),
    });
    assert.equal(result.status === 0, platforms.length === 2, result.stderr);
  });
}

test('provenance checks every digest against source revision and trusted workflow', (t) => {
  const result = shell(t, step('Verify nightly image provenance'), { ...images, UPSTREAM_SHA: 'd'.repeat(40) });
  assert.equal(result.status, 0, result.stderr);
  for (const image of Object.values(images)) assert.ok(result.log.includes(`oci://${image}`));
  for (const line of result.log.trim().split('\n')) {
    assert.ok(line.includes('--signer-workflow multigres/multigres/.github/workflows/nightly-build.yml'));
    assert.ok(line.includes('--source-ref refs/heads/main'));
    assert.ok(line.includes(`--source-digest ${'d'.repeat(40)}`));
    assert.ok(line.includes('--deny-self-hosted-runners'));
  }
});

for (const flags of [{ FAIL_VERIFY: 'true' }, { WRONG_SUBJECT: 'true' }]) {
  test(`provenance fails closed: ${Object.keys(flags)[0]}`, (t) => {
    assert.notEqual(shell(t, step('Verify nightly image provenance'), { ...images, ...flags }).status, 0);
  });
}

test('a green canary closes the incident independently of a failed promotion', async (t) => {
  const env = { RESOLVE_RESULT: 'success', PREFLIGHT_RESULT: 'success', E2E_RESULT: 'success', PROMOTION_RESULT: 'failure', EVENT_NAME: 'schedule', OPERATOR_REF: 'main', UPSTREAM_SHA: 'a'.repeat(40) };
  for (const [key, value] of Object.entries(env)) {
    const original = process.env[key];
    process.env[key] = value;
    t.after(() => { if (original === undefined) delete process.env[key]; else process.env[key] = original; });
  }
  const calls = [];
  const issues = {
    getLabel: async () => {}, listForRepo: async () => {},
    createComment: async () => {},
    update: async (args) => calls.push(args),
    create: async () => assert.fail('Promotion failure must not create a compatibility incident'),
  };
  const github = { rest: { issues }, paginate: async () => [{ number: 574, body: '<!-- nightly-compatibility-canary -->' }] };
  const context = { repo: { owner: 'multigres', repo: 'multigres-operator' }, ref: 'refs/heads/main', runId: 123 };
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  await new AsyncFunction('github', 'context', 'core', step('Update tracking issue', 'script', 10))(github, context, {});
  assert.equal(calls.length, 1);
  assert.equal(calls[0].state, 'closed');
});

test('report and promotion have separate permissions and only promotion writes handled state', () => {
  const report = workflow.split('\n  report:\n')[1].split('\n  promote:\n')[0];
  const promotion = workflow.split('\n  promote:\n')[1];
  assert.match(report, /needs: \[resolve, preflight, e2e\]/);
  assert.match(promotion, /needs: \[resolve, preflight, e2e\]/);
  assert.match(report, /permissions:\n      issues: write[^\n]*\n    steps:/);
  assert.match(promotion, /permissions:\n      contents: write\n      pull-requests: write\n    steps:/);
  assert.ok(!report.includes('upload-artifact'));
  assert.ok(!workflow.includes('nightly-compatibility-green-'));
  assert.ok(promotion.indexOf('Create or update promotion PR') < promotion.indexOf('Write promoted state'));
  assert.ok(!promotion.includes('continue-on-error'));
});
