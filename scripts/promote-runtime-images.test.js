const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {
  promote, recordFromEnv, validateDefaults, replaceDefaults, readDefaults,
  DEFAULTS, RECORD, BRANCH,
} = require('./promote-runtime-images');

// Model the pre-promotion state even when these tests run on a generated PR
// whose checked-out defaults have already been pinned by digest.
let source = fs.readFileSync(DEFAULTS, 'utf8');
for (const image of Object.values(readDefaults(source))) {
  source = source.replaceAll(image, `${image.split(/[:@]/)[0]}:sha-1111111`);
}
const sha = (digit) => digit.repeat(40);
const image = (name, digit) => `ghcr.io/multigres/${name}@sha256:${digit.repeat(64)}`;
function record(digit = '2') {
  return recordFromEnv({
    UPSTREAM_SHA: sha(digit), OPERATOR_SHA: sha('a'),
    NIGHTLY_RUN_ID: '100', NIGHTLY_RUN_ATTEMPT: '2',
    GITHUB_RUN_ID: '200', GITHUB_RUN_ATTEMPT: '1',
    MULTIGRES_IMAGE: image('multigres', digit), PGCTLD_IMAGE: image('pgctld', digit),
    MULTIADMIN_WEB_IMAGE: image('multiadmin-web', digit),
  });
}

// Model the Git database separately from refs: a failed ref update leaves the
// committed pair unreachable, whereas a failed PR call leaves a repairable branch.
function fixture({ mainRecord, pendingRecord, existingPR = false } = {}) {
  const calls = [];
  const files = (r) => ({
    [DEFAULTS]: r ? replaceDefaults(source, r) : source,
    ...(r ? { [RECORD]: JSON.stringify(r) } : {}),
    'unrelated.txt': 'keep current main',
  });
  const states = new Map([[sha('a'), files(mainRecord)]]);
  let main = sha('a');
  let branch;
  if (pendingRecord) { branch = sha('b'); states.set(branch, files(pendingRecord)); }
  let pr = existingPR ? { number: 17, title: 'old title', body: 'old body', state: 'open', head: { sha: branch } } : null;
  let nextTree;
  let writes = 0;
  const failures = {};
  const missing = () => { throw Object.assign(new Error('Not found'), { status: 404 }); };
  const method = (name, impl) => async (args) => {
    calls.push({ name, args });
    if (failures[name]) throw new Error(`injected ${name} failure`);
    return { data: impl(args) };
  };
  const rest = {
    repos: {
      getContent: method('getContent', ({ path, ref }) => {
        const content = states.get(ref)?.[path];
        return content === undefined ? missing() : { type: 'file', content: Buffer.from(content).toString('base64') };
      }),
      compareCommits: method('compareCommits', ({ base, head }) => ({ status:
        base === 'main' ? 'behind' : base === head ? 'identical' : base < head ? 'ahead' : 'behind',
      })),
    },
    git: {
      getRef: method('getRef', ({ ref }) => {
        const value = ref === 'heads/main' ? main : branch;
        return value ? { object: { sha: value } } : missing();
      }),
      getCommit: method('getCommit', ({ commit_sha }) => ({ tree: { sha: commit_sha } })),
      createTree: method('createTree', ({ base_tree, tree }) => {
        nextTree = { ...states.get(base_tree), ...Object.fromEntries(tree.map((entry) => [entry.path, entry.content])) };
        return { sha: 'tree' };
      }),
      createCommit: method('createCommit', () => {
        const commit = `commit-${++writes}`;
        states.set(commit, nextTree);
        return { sha: commit };
      }),
      createRef: method('createRef', ({ sha: value }) => {
        assert.equal(branch, undefined);
        branch = value;
      }),
      updateRef: method('updateRef', ({ sha: value, force }) => {
        assert.equal(force, false);
        branch = value;
        if (pr) pr.head.sha = value;
      }),
    },
    pulls: {
      list: async () => {},
      create: method('createPR', (args) => {
        pr = { ...args, number: 17, state: 'open', html_url: 'https://github.com/multigres/multigres-operator/pull/17', head: { sha: branch } };
        return pr;
      }),
      update: method('updatePR', (args) => {
        Object.assign(pr, args);
        return pr;
      }),
    },
  };
  return {
    github: { rest, paginate: async () => pr ? [pr] : [] },
    context: { repo: { owner: 'multigres', repo: 'multigres-operator' }, eventName: 'schedule', ref: 'refs/heads/main', sha: sha('a'), runId: 200, runAttempt: 1 },
    record: record(), calls, failures,
    get branchFiles() { return states.get(branch); },
    get branch() { return branch; },
    setMain(value) { main = value; },
  };
}

test('promotes the complete verified set and evidence in one commit and one PR', async () => {
  const f = fixture();
  const result = await promote(f);
  assert.equal(result.changed, true);
  const committed = JSON.parse(f.branchFiles[RECORD]);
  assert.deepEqual(committed, f.record);
  validateDefaults(f.branchFiles[DEFAULTS], committed);
  assert.equal(f.branchFiles['unrelated.txt'], 'keep current main');
  for (const name of ['DefaultEtcdImage', 'DefaultPostgresExporterImage']) {
    const pattern = new RegExp(`${name} = "[^"]+"`);
    assert.equal(f.branchFiles[DEFAULTS].match(pattern)[0], source.match(pattern)[0]);
  }
  const trees = f.calls.filter((call) => call.name === 'createTree');
  assert.equal(trees.length, 1);
  assert.deepEqual(trees[0].args.tree.map((entry) => entry.path), [DEFAULTS, RECORD]);
  assert.equal(f.calls.filter((call) => call.name === 'createRef').length, 1);
  const pr = f.calls.find((call) => call.name === 'createPR').args;
  assert.equal(pr.head, BRANCH);
  assert.equal(pr.base, 'main');
  for (const old of Object.values(readDefaults(source))) assert.ok(pr.body.includes(old));
  for (const next of Object.values(committed.images)) assert.ok(pr.body.includes(next));
  assert.ok(pr.body.includes(committed.nightly_run.url));
  assert.ok(pr.body.includes(committed.canary_run.url));
});

test('reprocessing the same digest set is a no-op, including evidence and PR metadata', async () => {
  const f = fixture();
  await promote(f);
  const files = structuredClone(f.branchFiles);
  f.calls.length = 0;
  f.context.runId = 201;
  f.record.canary_run = { id: '201', attempt: '1', url: 'https://github.com/multigres/multigres-operator/actions/runs/201/attempts/1' };
  assert.equal((await promote(f)).changed, false);
  assert.deepEqual(f.branchFiles, files);
  assert.ok(!f.calls.some((call) => /^(create|update)/.test(call.name)));
});

test('a merged digest set is a successful no-op', async () => {
  const f = fixture({ mainRecord: record() });
  assert.deepEqual(await promote(f), { changed: false, merged: true });
  assert.ok(!f.calls.some((call) => /^(create|update)/.test(call.name)));
});

test('a newer green set updates the existing PR atomically', async () => {
  const f = fixture({ pendingRecord: record('1'), existingPR: true });
  await promote(f);
  validateDefaults(f.branchFiles[DEFAULTS], f.record);
  assert.ok(!f.calls.some((call) => call.name === 'createPR'));
  assert.equal(f.calls.find((call) => call.name === 'updatePR').args.pull_number, 17);
  assert.deepEqual(f.calls.find((call) => call.name === 'createCommit').args.parents, [sha('b'), sha('a')]);
});

for (const operation of ['createPR', 'updatePR']) {
  test(`failed ${operation} rejects and retry repairs the PR without another commit`, async () => {
    const f = fixture(operation === 'updatePR' ? { pendingRecord: record('1'), existingPR: true } : {});
    f.failures[operation] = true;
    await assert.rejects(promote(f), /injected/);
    validateDefaults(f.branchFiles[DEFAULTS], f.record);
    f.failures[operation] = false;
    f.calls.length = 0;
    assert.equal((await promote(f)).changed, false);
    assert.ok(!f.calls.some((call) => call.name === 'createCommit'));
    assert.ok(f.calls.some((call) => call.name === operation));
  });
}

test('failed ref update cannot publish part of an image set', async () => {
  const f = fixture({ pendingRecord: record('1'), existingPR: true });
  const before = structuredClone(f.branchFiles);
  f.failures.updateRef = true;
  await assert.rejects(promote(f), /injected/);
  assert.deepEqual(f.branchFiles, before);
  assert.ok(!f.calls.some((call) => call.name === 'updatePR'));
});

for (const [name, change] of [
  ['manual run', (f) => { f.context.eventName = 'workflow_dispatch'; }],
  ['fork', (f) => { f.context.repo.owner = 'fork'; }],
  ['non-main ref', (f) => { f.context.ref = 'refs/heads/test'; }],
  ['untested operator', (f) => { f.record.operator_sha = sha('c'); }],
  ['unrelated canary', (f) => { f.context.runId = 300; }],
  ['wrong attempt', (f) => { f.context.runAttempt = 2; }],
  ['mutable image', (f) => { f.record.images.multigres = 'ghcr.io/multigres/multigres:main'; }],
  ['wrong registry', (f) => { f.record.images.pgctld = image('pgctld', '2').replace('ghcr.io', 'evil.io'); }],
  ['missing image', (f) => { delete f.record.images['multiadmin-web']; }],
  ['invalid nightly evidence', (f) => { f.record.nightly_run.id = ''; }],
  ['main advanced during e2e', (f) => { f.setMain(sha('c')); }],
]) {
  test(`rejects ${name} before writing`, async () => {
    const f = fixture();
    change(f);
    await assert.rejects(promote(f));
    assert.ok(!f.calls.some((call) => /^(create|update)/.test(call.name)));
  });
}

for (const location of ['mainRecord', 'pendingRecord']) {
  test(`rejects a revision older than ${location}, even without a checkpoint artifact`, async () => {
    const f = fixture({ [location]: record('3') });
    await assert.rejects(promote(f), /Stale/);
    assert.ok(!f.calls.some((call) => /^(create|update)/.test(call.name)));
  });
}

test('rejects digest drift at an already recorded upstream revision', async () => {
  const f = fixture({ pendingRecord: record() });
  f.record.images.pgctld = image('pgctld', '3');
  await assert.rejects(promote(f), /Immutable image set changed/);
});

test('fails closed when a constant is missing, repeated, or disagrees with the record', () => {
  assert.throws(() => replaceDefaults(source.replace('DefaultPostgresImage =', 'RenamedImage ='), record()));
  assert.throws(() => replaceDefaults(`${source}\nDefaultPostgresImage = "duplicate"`, record()));
  const updated = replaceDefaults(source, record());
  assert.throws(() => validateDefaults(updated.replace(image('pgctld', '2'), image('pgctld', '3')), record()));
});

test('operator main moving just before publication leaves refs untouched', async () => {
  const f = fixture();
  const createCommit = f.github.rest.git.createCommit;
  f.github.rest.git.createCommit = async (args) => {
    const result = await createCommit(args);
    f.setMain(sha('c'));
    return result;
  };
  await assert.rejects(promote(f), /Operator main changed/);
  assert.equal(f.branch, undefined);
});

test('first promotion rejects a nightly older than the existing source-pinned defaults', async () => {
  const f = fixture();
  const compare = f.github.rest.repos.compareCommits;
  f.github.rest.repos.compareCommits = (args) => args.base === '1111111'
    ? Promise.resolve({ data: { status: 'behind' } }) : compare(args);
  await assert.rejects(promote(f), /older than the initial runtime defaults/);
  assert.ok(!f.calls.some((call) => /^(create|update)/.test(call.name)));
});
