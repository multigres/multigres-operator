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
  const env = { RESOLVE_RESULT: 'success', PREFLIGHT_RESULT: 'success', E2E_RESULT: 'success', SUPABASE_RESULT: 'success', SUPABASE_E2E_RESULT: 'success', PROMOTION_RESULT: 'failure', EVENT_NAME: 'schedule', OPERATOR_REF: 'main', UPSTREAM_SHA: 'a'.repeat(40) };
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
  await new AsyncFunction('github', 'context', 'core', step('Update tracking issues', 'script', 10))(github, context, {});
  assert.equal(calls.length, 1);
  assert.equal(calls[0].state, 'closed');
});

test('report and promotion have separate permissions and only promotion writes handled state', () => {
  const report = workflow.split('\n  report:\n')[1].split('\n  promote:\n')[0];
  const promotion = workflow.split('\n  promote:\n')[1];
  assert.match(report, /needs: \[resolve, preflight, supabase, e2e, e2e-supabase\]/);
  assert.match(promotion, /needs: \[resolve, preflight, supabase, e2e, e2e-supabase\]/);
  assert.match(report, /permissions:\n      contents: read[^\n]*\n      issues: write[^\n]*\n    steps:/);
  assert.match(promotion, /permissions:\n      contents: write\n      pull-requests: write\n    steps:/);
  assert.ok(!report.includes('upload-artifact'));
  assert.ok(!workflow.includes('nightly-compatibility-green-'));
  assert.ok(promotion.indexOf('Create or update promotion PR') < promotion.indexOf('Write promoted state'));
  assert.ok(!promotion.includes('continue-on-error'));
});

for (const relation of ['identical', 'ahead', 'behind', 'diverged']) {
  test(`scheduled gate handles ${relation} upstream revision`, (t) => {
    // The first comparison verifies membership in main; the second compares
    // with the last promoted revision.
    const code = step('Validate and gate upstream SHA');
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'canary-gate-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    fs.writeFileSync(path.join(dir, 'curl'), `#!/bin/bash
case "$*" in
  *compare/main...*) printf '%s' '{"status":"behind"}' ;;
  *) printf '%s' '{"status":"${relation}"}' ;;
esac
`, { mode: 0o755 });
    const output = path.join(dir, 'output');
    fs.writeFileSync(output, '');
    const result = spawnSync('bash', ['-eo', 'pipefail', '-c', code], {
      encoding: 'utf8', env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, GITHUB_OUTPUT: output,
        SHA: 'a'.repeat(40), LAST_GREEN_SHA: 'b'.repeat(40), EVENT_NAME: 'schedule', OPERATOR_REF: 'main' },
    });
    assert.equal(result.status === 0, relation !== 'diverged', result.stderr);
    assert.equal(fs.readFileSync(output, 'utf8'), relation === 'diverged' ? '' : `should_run=${relation !== 'behind'}\n`);
  });
}

for (const failed of ['resolve', 'preflight', 'supabase', 'e2e', 'e2e-supabase']) {
  for (const result of ['failure', 'cancelled', 'skipped', '']) {
    test(`promotion gate rejects ${failed} ${result || 'missing'}`, () => {
      const condition = workflow.split('\n  promote:\n')[1].split('    if: >-\n')[1].split('    runs-on:')[0];
      const values = {
        'github.repository': 'multigres/multigres-operator', 'github.event_name': 'schedule',
        'github.ref': 'refs/heads/main', 'needs.resolve.outputs.operator-ref': 'main',
        'needs.resolve.outputs.should-run': 'true',
      };
      for (const job of ['resolve', 'preflight', 'supabase', 'e2e', 'e2e-supabase']) values[`needs.${job}.result`] = job === failed ? result : 'success';
      // GitHub applies success() implicitly to this job's needs. Evaluate it
      // alongside the explicit condition to cover failed and absent results.
      const dependenciesPass = ['resolve', 'preflight', 'supabase', 'e2e', 'e2e-supabase'].every((job) => values[`needs.${job}.result`] === 'success');
      const expression = condition.replace(/(?:github|needs)\.[\w.-]+/g, (key) => {
        assert.ok(Object.hasOwn(values, key), `Unknown workflow input ${key}`);
        return JSON.stringify(values[key]);
      });
      assert.equal(dependenciesPass && new Function(`return (${expression});`)(), false);
    });
  }
}

test('Supabase lane uses the same operator and non-Postgres images', () => {
  const vanilla = workflow.split('\n  e2e:\n')[1].split('\n  e2e-supabase:\n')[0];
  const supabase = workflow.split('\n  e2e-supabase:\n')[1].split('\n  report:\n')[0];
  for (const key of ['ref', 'multiadmin-image', 'multiadmin-web-image', 'multiorch-image', 'multipooler-image', 'multigateway-image']) {
    const input = (text) => text.match(new RegExp(`^      ${key}: (.+)$`, 'm'))[1];
    assert.equal(input(vanilla), input(supabase));
  }
  assert.match(supabase, /postgres-image: \$\{\{ needs.supabase.outputs.image \}\}/);
  assert.match(supabase, /artifact-suffix: supabase/);
});

const supabaseImage = `docker.io/supabase/postgres@sha256:${'d'.repeat(64)}`;
const upstreamIssue = { number: 574, labels: ['upstream-compatibility'], body: '<!-- nightly-compatibility-canary -->' };
const supabaseIssue = { number: 575, labels: ['supabase-compatibility'], body: '<!-- nightly-compatibility-supabase -->' };

async function report(t, env, { open = [], promotion } = {}) {
  env = {
    RESOLVE_RESULT: 'success', PREFLIGHT_RESULT: 'success', E2E_RESULT: 'success',
    SUPABASE_RESULT: 'success', SUPABASE_E2E_RESULT: 'success', SUPABASE_IMAGE: supabaseImage,
    EVENT_NAME: 'schedule', OPERATOR_REF: 'main', UPSTREAM_SHA: 'a'.repeat(40), SHORT_SHA: 'aaaaaaa',
    OPERATOR_SHA: 'b'.repeat(40), ...env,
  };
  for (const [key, value] of Object.entries(env)) {
    const original = process.env[key];
    process.env[key] = value;
    t.after(() => { if (original === undefined) delete process.env[key]; else process.env[key] = original; });
  }
  const calls = [];
  const record = (name) => async (args) => { calls.push({ name, ...args }); return { data: {} }; };
  const issues = {
    getLabel: record('getLabel'), createLabel: record('createLabel'), listForRepo: async () => {},
    createComment: record('comment'), update: record('update'), create: record('create'),
  };
  const repos = {
    getContent: async ({ path: file, ref }) => {
      assert.equal(file, 'config/runtime-image-promotion.json');
      assert.equal(ref, env.OPERATOR_SHA);
      if (!promotion) throw Object.assign(new Error('Not found'), { status: 404 });
      return { data: { content: Buffer.from(JSON.stringify(promotion)).toString('base64') } };
    },
  };
  const github = {
    rest: { issues, repos },
    paginate: async (_, { labels }) => open.filter((issue) => issue.labels.includes(labels)),
  };
  const context = { repo: { owner: 'multigres', repo: 'multigres-operator' }, ref: 'refs/heads/main', runId: 123 };
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  await new AsyncFunction('github', 'context', 'core', step('Update tracking issues', 'script', 10))(github, context, { info: () => {} });
  return calls;
}

const closed = (calls, number) => calls.some((call) => call.name === 'update' && call.issue_number === number && call.state === 'closed');
const touched = (calls, number) => calls.some((call) => call.issue_number === number);
const lastPromotion = { upstream_sha: 'c'.repeat(40), operator_sha: 'e'.repeat(40), supabase: { image: supabaseImage } };

for (const result of ['failure', 'cancelled', 'skipped', '']) {
  test(`Supabase-only ${result || 'missing'} result opens its own issue and closes the upstream issue`, async (t) => {
    const calls = await report(t, { SUPABASE_E2E_RESULT: result }, { open: [upstreamIssue], promotion: lastPromotion });
    assert.ok(closed(calls, 574));
    const created = calls.filter((call) => call.name === 'create');
    assert.equal(created.length, 1);
    assert.deepEqual(created[0].labels, ['supabase-compatibility']);
    assert.equal(created[0].title, 'Supabase Postgres lane failing at multigres/aaaaaaa');
    assert.ok(created[0].body.startsWith('<!-- nightly-compatibility-supabase -->'));
    assert.ok(created[0].body.includes('**Supabase operator e2e**'));
    assert.ok(created[0].body.includes(`multigres/compare/${'c'.repeat(40)}...${'a'.repeat(40)}`));
    assert.ok(created[0].body.includes(`multigres-operator/compare/${'e'.repeat(40)}...${'b'.repeat(40)}`));
    assert.ok(created[0].body.includes('Supabase image: unchanged'));
  });
}

test('Supabase image verification failure names that stage', async (t) => {
  const calls = await report(t, { SUPABASE_RESULT: 'failure', SUPABASE_E2E_RESULT: 'skipped' }, { promotion: lastPromotion });
  const created = calls.find((call) => call.name === 'create');
  assert.ok(created.body.includes('**Supabase image verification**'));
});

for (const [promotion, expected] of [
  [{ ...lastPromotion, supabase: { image: `docker.io/supabase/postgres@sha256:${'f'.repeat(64)}` } }, `changed from \`docker.io/supabase/postgres@sha256:${'f'.repeat(64)}\``],
  [{ upstream_sha: 'c'.repeat(40), operator_sha: 'e'.repeat(40) }, 'no earlier Supabase result recorded'],
  [undefined, 'Multigres: unavailable'],
]) {
  test(`Supabase issue reports ${expected.split(' ')[0]} against the last promotion`, async (t) => {
    const calls = await report(t, { SUPABASE_E2E_RESULT: 'failure' }, { promotion });
    assert.ok(calls.find((call) => call.name === 'create').body.includes(expected));
  });
}

test('an open Supabase issue is updated instead of duplicated', async (t) => {
  const calls = await report(t, { SUPABASE_E2E_RESULT: 'failure' }, { open: [supabaseIssue], promotion: lastPromotion });
  assert.ok(!calls.some((call) => call.name === 'create'));
  assert.ok(calls.some((call) => call.name === 'update' && call.issue_number === 575 && !call.state));
  assert.ok(calls.some((call) => call.name === 'comment' && call.issue_number === 575 && call.body.includes('Supabase operator e2e')));
});

test('a vanilla failure is reported once, on the upstream issue', async (t) => {
  const calls = await report(t, { E2E_RESULT: 'failure', SUPABASE_E2E_RESULT: 'failure' }, { open: [upstreamIssue, supabaseIssue] });
  assert.ok(!touched(calls, 575));
  assert.ok(!closed(calls, 574));
  const update = calls.find((call) => call.name === 'update' && call.issue_number === 574);
  assert.ok(update.body.includes('**vanilla operator e2e**'));
  assert.ok(update.body.includes('**Supabase lane:** did not pass (image check: success, e2e: failure)'));
});

test('a green canary closes both issues', async (t) => {
  const calls = await report(t, {}, { open: [upstreamIssue, supabaseIssue] });
  assert.ok(closed(calls, 574));
  assert.ok(closed(calls, 575));
});

test('a diagnostic run cannot close either issue', async (t) => {
  const calls = await report(t, { EVENT_NAME: 'workflow_dispatch' }, { open: [upstreamIssue, supabaseIssue] });
  assert.equal(calls.length, 0);
});

const digest = `sha256:${'d'.repeat(64)}`;
for (const [input, fallback, expected] of [
  ['', `docker.io/supabase/postgres@${digest}`, `docker.io/supabase/postgres@${digest}`],
  [`docker.io/supabase/postgres@${digest}`, `docker.io/supabase/postgres@sha256:${'e'.repeat(64)}`, `docker.io/supabase/postgres@${digest}`],
  ['', `localhost:5000/postgres@${digest}`, undefined],
  ['', '', undefined],
  ['', 'docker.io/supabase/postgres:latest', undefined],
  ['', `supabase/postgres@${digest}`, undefined],
  ['', 'docker.io/supabase/postgres@sha256:abc', undefined],
]) {
  test(`Supabase image selection: ${input || fallback || 'unset'}`, (t) => {
    const result = shell(t, step('Select Supabase image'), { INPUT_IMAGE: input, DEFAULT_IMAGE: fallback });
    assert.equal(result.status === 0, Boolean(expected), result.stdout);
    assert.equal(result.output, expected ? `image=${expected}\n` : '');
  });
}

function pgbackrest(t, supabase, multipooler) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pgbackrest-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.writeFileSync(path.join(dir, 'docker'), `#!/bin/bash
[ "$1 $2 $3 $4 $6" = "run --rm --entrypoint pgbackrest version" ] || exit 2
case "$5" in
  "$SUPABASE_IMAGE") version="$SUPABASE_VERSION" ;;
  "$MULTIPOOLER_IMAGE") version="$MULTIPOOLER_VERSION" ;;
  *) exit 3 ;;
esac
[ "$version" != missing ] || exit 1
printf '%s\\n' "$version"
`, { mode: 0o755 });
  return spawnSync('bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', step('Compare pgbackrest versions')], {
    encoding: 'utf8',
    env: {
      ...process.env, PATH: `${dir}:${process.env.PATH}`,
      SUPABASE_IMAGE: supabaseImage, MULTIPOOLER_IMAGE: images.MULTIGRES_IMAGE,
      SUPABASE_VERSION: supabase, MULTIPOOLER_VERSION: multipooler,
    },
  });
}

test('matching pgbackrest versions pass', (t) => {
  const result = pgbackrest(t, 'pgBackRest 2.56.0', 'pgBackRest 2.56.0');
  assert.equal(result.status, 0, result.stdout);
});

for (const [supabase, multipooler] of [
  ['pgBackRest 2.55.1', 'pgBackRest 2.56.0'],
  ['missing', 'pgBackRest 2.56.0'],
  ['pgBackRest 2.56.0', 'missing'],
  ['', ''],
  ['unexpected', 'unexpected'],
]) {
  test(`pgbackrest comparison fails closed: ${supabase || 'empty'} vs ${multipooler || 'empty'}`, (t) => {
    assert.notEqual(pgbackrest(t, supabase, multipooler).status, 0);
  });
}

test('a passing Supabase lane closes its issue even when the vanilla lane fails', async (t) => {
  const calls = await report(t, { E2E_RESULT: 'failure' }, { open: [upstreamIssue, supabaseIssue] });
  assert.ok(closed(calls, 575));
  assert.ok(!closed(calls, 574));
  const update = calls.find((call) => call.name === 'update' && call.issue_number === 574);
  assert.ok(update.body.includes('**Supabase lane:** passed'));
});
