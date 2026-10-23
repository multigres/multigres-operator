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
  assert.match(promotion, /needs: \[resolve, preflight, e2e, report\]/);
  assert.match(report, /permissions:\n      issues: write[^\n]*\n    steps:/);
  assert.match(promotion, /permissions:\n      contents: write\n      pull-requests: write\n    steps:/);
  assert.ok(!report.includes('upload-artifact'));
  assert.ok(!workflow.includes('nightly-compatibility-green-'));
  assert.ok(promotion.indexOf('Create or update promotion PR') < promotion.indexOf('Write promoted state'));
  assert.ok(!promotion.includes('continue-on-error'));
});

function manualBuild(overrides = {}) {
  return {
    id: 123, run_attempt: 2, head_sha: 'b'.repeat(40), status: 'completed',
    conclusion: 'success', head_branch: 'main', event: 'workflow_dispatch',
    path: '.github/workflows/nightly-build.yml',
    repository: { full_name: 'multigres/multigres' },
    head_repository: { full_name: 'multigres/multigres' }, ...overrides,
  };
}
function manualEnv(overrides = {}) {
  return {
    INPUT_SHA: '', INPUT_NIGHTLY_RUN_ID: '123', INPUT_PROMOTE: 'true', INPUT_OPERATOR_REF: 'main',
    GITHUB_EVENT_NAME: 'workflow_dispatch', GITHUB_REF: 'refs/heads/main',
    GITHUB_REPOSITORY: 'multigres/multigres-operator', GITHUB_SHA: 'a'.repeat(40),
    NIGHTLY_JSON: JSON.stringify(manualBuild()), ...overrides,
  };
}
test('manual promotion resolves a successful main build and freezes workflow main', (t) => {
  const r = shell(t, step('Resolve upstream SHA'), manualEnv());
  assert.equal(r.status, 0, r.stderr);
  assert.match(r.output, /nightly_run_id=123\nnightly_run_attempt=2\n/);
  assert.ok(r.output.includes(`sha=${'b'.repeat(40)}\n`));
  assert.ok(r.output.includes(`operator_sha=${'a'.repeat(40)}\n`));
});
for (const invalid of [
  { status: 'in_progress' }, { conclusion: 'failure' }, { conclusion: 'cancelled' },
  { id: 456 }, { head_branch: 'feature' }, { event: 'pull_request' },
  { path: '.github/workflows/docker-build.yml' },
  { repository: { full_name: 'fork/multigres' } },
  { head_repository: { full_name: 'fork/multigres' } },
]) {
  test(`manual build rejects ${JSON.stringify(invalid)}`, (t) => {
    const r = shell(t, step('Resolve upstream SHA'), manualEnv({ NIGHTLY_JSON: JSON.stringify(manualBuild(invalid)) }));
    assert.notEqual(r.status, 0);
    assert.equal(r.output, '');
  });
}
for (const invalid of [
  { INPUT_NIGHTLY_RUN_ID: '' }, { INPUT_NIGHTLY_RUN_ID: '../123' },
  { INPUT_SHA: 'c'.repeat(40) }, { INPUT_OPERATOR_REF: 'feature' },
  { GITHUB_REF: 'refs/heads/feature' }, { GITHUB_REPOSITORY: 'fork/operator' },
]) {
  test(`manual promotion rejects ${JSON.stringify(invalid)}`, (t) => {
    assert.notEqual(shell(t, step('Resolve upstream SHA'), manualEnv(invalid)).status, 0);
  });
}
test('a branch can diagnose an explicit build without enabling promotion', (t) => {
  const r = shell(t, step('Resolve upstream SHA'), manualEnv({
    INPUT_PROMOTE: 'false', GITHUB_REF: 'refs/heads/feature', INPUT_OPERATOR_REF: 'feature',
    OPERATOR_JSON: JSON.stringify({ sha: 'c'.repeat(40) }),
  }));
  assert.equal(r.status, 0, r.stderr);
  assert.ok(r.output.includes(`operator_sha=${'c'.repeat(40)}\n`));
});
test('manual promotion is opt-in, main-only and requires fresh successful verification and e2e', () => {
  const promotion = workflow.split('\n  promote:\n')[1];
  assert.match(workflow, /promote:\n[\s\S]*?default: false\n        type: boolean/);
  assert.ok(promotion.includes("github.event_name == 'workflow_dispatch' && inputs.promote"));
  assert.ok(promotion.includes("github.ref == 'refs/heads/main'"));
  assert.ok(promotion.includes("needs.resolve.outputs.operator-ref == 'main'"));
  for (const job of ['preflight', 'e2e']) assert.ok(promotion.includes(`needs.${job}.result == 'success'`));
  assert.match(workflow.split('\n  e2e:\n')[1].split('\n  report:\n')[0], /timeout-minutes: 60/);
  assert.match(fs.readFileSync('.github/workflows/pull-request.yaml', 'utf8').split('\n  promotion-e2e:\n')[1], /timeout-minutes: 60/);
});

for (const scenario of [
  { name: 'explicit manual promotion', promote: 'true', ref: 'refs/heads/main', operator: 'main', owner: 'multigres', closes: true },
  { name: 'manual diagnostic on main', promote: 'false', ref: 'refs/heads/main', operator: 'main', owner: 'multigres', closes: false },
  { name: 'branch workflow', promote: 'true', ref: 'refs/heads/feature', operator: 'main', owner: 'multigres', closes: false },
  { name: 'operator skew diagnostic', promote: 'true', ref: 'refs/heads/main', operator: 'feature', owner: 'multigres', closes: false },
  { name: 'fork workflow', promote: 'true', ref: 'refs/heads/main', operator: 'main', owner: 'fork', closes: false },
]) {
  test(`incident recovery from ${scenario.name}`, async (t) => {
    const env = {
      RESOLVE_RESULT: 'success', PREFLIGHT_RESULT: 'success', E2E_RESULT: 'success',
      EVENT_NAME: 'workflow_dispatch', PROMOTE: scenario.promote, OPERATOR_REF: scenario.operator,
      UPSTREAM_SHA: 'a'.repeat(40), GITHUB_SERVER_URL: 'https://github.com',
    };
    for (const [key, value] of Object.entries(env)) {
      const previous = process.env[key];
      process.env[key] = value;
      t.after(() => { if (previous === undefined) delete process.env[key]; else process.env[key] = previous; });
    }
    const updates = [], comments = [];
    const issues = {
      getLabel: async () => {}, listForRepo: async () => {},
      update: async args => updates.push(args), createComment: async args => comments.push(args),
      create: async () => assert.fail('A successful run must not open an incident'),
    };
    const github = { rest: { issues }, paginate: async () => [{ number: 741, body: '<!-- nightly-compatibility-canary -->' }] };
    const context = { repo: { owner: scenario.owner, repo: 'multigres-operator' }, ref: scenario.ref, runId: 123 };
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    await new AsyncFunction('github', 'context', 'core', step('Update tracking issue', 'script', 10))(github, context, { info() {} });
    assert.equal(updates.length, scenario.closes ? 1 : 0);
    assert.equal(comments.length, scenario.closes ? 1 : 0);
    if (scenario.closes) {
      assert.equal(updates[0].state, 'closed');
      assert.equal(updates[0].issue_number, 741);
      assert.ok(comments[0].body.includes('/actions/runs/123'));
    }
  });
}
test('promotion cannot checkpoint when incident reporting fails', () => {
  const promotion = workflow.split('\n  promote:\n')[1];
  assert.match(promotion, /needs: \[resolve, preflight, e2e, report\]/);
  assert.ok(promotion.includes("needs.report.result == 'success'"));
  const reportStep = workflow.split('      - name: Update tracking issue\n')[1].split('\n        with:')[0];
  assert.ok(reportStep.includes('PROMOTE: ${{ inputs.promote }}'));
});
