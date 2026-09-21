// The workflow supplies only digests that passed provenance verification and e2e.
// Use one Git tree and one ref update so the record and all defaults move together.
const fs = require('node:fs');
const assert = require('node:assert/strict');

const BRANCH = 'chore/promote-runtime-images';
const DEFAULTS = 'api/v1alpha1/image_defaults.go';
const RECORD = 'config/runtime-image-promotion.json';
const COMPONENTS = {
  DefaultPostgresImage: 'pgctld',
  DefaultMultiadminImage: 'multigres',
  DefaultMultiadminWebImage: 'multiadmin-web',
  DefaultMultiorchImage: 'multigres',
  DefaultMultipoolerImage: 'multigres',
  DefaultMultigatewayImage: 'multigres',
};

function validateRecord(record) {
  assert.equal(record.schema_version, 1);
  for (const sha of [record.upstream_sha, record.operator_sha]) {
    assert.match(sha, /^[0-9a-f]{40}$/);
  }
  for (const [name, repo] of [['nightly_run', 'multigres'], ['canary_run', 'multigres-operator']]) {
    const run = record[name];
    assert.match(String(run.id), /^[1-9][0-9]*$/);
    assert.match(String(run.attempt), /^[1-9][0-9]*$/);
    assert.equal(run.url, `https://github.com/multigres/${repo}/actions/runs/${run.id}/attempts/${run.attempt}`);
  }
  assert.deepEqual(Object.keys(record.images).sort(), ['multiadmin-web', 'multigres', 'pgctld']);
  for (const [name, image] of Object.entries(record.images)) {
    assert.match(image, new RegExp(`^ghcr\\.io/multigres/${name}@sha256:[0-9a-f]{64}$`));
  }
}

function readDefaults(source) {
  return Object.fromEntries(Object.keys(COMPONENTS).map((name) => {
    const matches = [...source.matchAll(new RegExp(`^(\\s*${name}\\s*=\\s*")([^"\\n]+)(")$`, 'gm'))];
    assert.equal(matches.length, 1, `Expected exactly one ${name}`);
    return [name, matches[0][2]];
  }));
}

function validateDefaults(source, record) {
  validateRecord(record);
  const defaults = readDefaults(source);
  for (const [name, component] of Object.entries(COMPONENTS)) {
    assert.equal(defaults[name], record.images[component], `${name} differs from promotion record`);
  }
}

function replaceDefaults(source, record) {
  validateRecord(record);
  readDefaults(source);
  for (const [name, component] of Object.entries(COMPONENTS)) {
    source = source.replace(new RegExp(`^(\\s*${name}\\s*=\\s*")[^"\\n]+(")$`, 'm'),
      `$1${record.images[component]}$2`);
  }
  validateDefaults(source, record);
  return source;
}

function sameImages(a, b) {
  return Object.keys(a.images).every((name) => a.images[name] === b.images[name]);
}

function pullRequestBody(previous, record) {
  const rows = Object.entries(COMPONENTS).map(([name, component]) =>
    `| ${name} | \`${previous[name]}\` | \`${record.images[component]}\` |`).join('\n');
  return `Promote the runtime images verified by the scheduled compatibility canary.

- Upstream revision: [${record.upstream_sha}](https://github.com/multigres/multigres/commit/${record.upstream_sha})
- Tested operator revision: [${record.operator_sha}](https://github.com/multigres/multigres-operator/commit/${record.operator_sha})
- [Upstream nightly](${record.nightly_run.url}) · [Green canary](${record.canary_run.url})

| Default | Current main | Proposed |
| --- | --- | --- |
${rows}

All three digests passed source-revision and build-provenance checks and contain linux/amd64 and linux/arm64 images. The promotion record and six defaults are committed atomically. Etcd and Postgres exporter defaults are unchanged.

The promotion e2e check builds this PR's committed defaults without image overrides. Maintainer review and passing checks are required before merge.
`;
}

async function promote({ github, context, record }) {
  assert.equal(context.repo.owner, 'multigres');
  assert.equal(context.repo.repo, 'multigres-operator');
  assert.equal(context.eventName, 'schedule');
  assert.equal(context.ref, 'refs/heads/main');
  validateRecord(record);
  assert.equal(record.operator_sha, context.sha);
  assert.equal(String(record.canary_run.id), String(context.runId));
  assert.equal(String(record.canary_run.attempt), String(context.runAttempt));

  const repo = context.repo;
  const api = github.rest;
  async function optional(read) {
    try { return (await read()).data; } catch (error) {
      if (error.status === 404) return null;
      throw error;
    }
  }
  async function file(path, ref) {
    const data = await optional(() => api.repos.getContent({ ...repo, path, ref }));
    if (!data) return null;
    assert.equal(data.type, 'file');
    return Buffer.from(data.content, 'base64').toString('utf8');
  }
  async function state(ref) {
    const source = await file(DEFAULTS, ref);
    assert.ok(source, `Missing defaults at ${ref}`);
    const json = await file(RECORD, ref);
    const prior = json ? JSON.parse(json) : null;
    if (prior) validateDefaults(source, prior);
    return { source, record: prior };
  }
  async function compare(base, head) {
    const { data } = await api.repos.compareCommits({ owner: 'multigres', repo: 'multigres', base, head });
    return data.status;
  }
  async function assertCurrentMain() {
    const { data } = await api.git.getRef({ ...repo, ref: 'heads/main' });
    assert.equal(data.object.sha, record.operator_sha, 'Operator main changed since the canary; retry on current main');
    return data.object.sha;
  }

  const mainSha = await assertCurrentMain();
  assert.ok(['behind', 'identical'].includes(await compare('main', record.upstream_sha)), 'Upstream SHA is not on main');
  const main = await state(mainSha);
  if (!main.record) {
    // Bootstrap from the source-pinned defaults that predate promotion records.
    // An older successful nightly must not roll those defaults back either.
    const revisions = new Set(Object.values(readDefaults(main.source)).map((image) => {
      const match = image.match(/:(?:nightly-)?sha-([0-9a-f]{7,40})$/);
      assert.ok(match, 'Defaults need a promotion record or source-pinned tags');
      return match[1];
    }));
    for (const revision of revisions) {
      assert.ok(['ahead', 'identical'].includes(await compare(revision, record.upstream_sha)),
        'Upstream revision is older than the initial runtime defaults');
    }
  }
  const branch = await optional(() => api.git.getRef({ ...repo, ref: `heads/${BRANCH}` }));
  const pending = branch ? await state(branch.object.sha) : null;
  if (pending) assert.ok(pending.record, 'Promotion branch is missing its record');
  // Consult both main and the branch, including a branch left by a failed PR API
  // call. Artifact retention or an old workflow rerun must never allow rollback.
  for (const prior of [main.record, pending?.record].filter(Boolean)) {
    const relation = await compare(prior.upstream_sha, record.upstream_sha);
    assert.ok(['ahead', 'identical'].includes(relation), 'Stale or divergent upstream revision');
    if (relation === 'identical') assert.ok(sameImages(prior, record), 'Immutable image set changed for the same revision');
  }

  if (main.record && sameImages(main.record, record)) {
    return { changed: false, merged: true };
  }

  let head = branch?.object.sha;
  let proposed = record;
  const unchanged = pending?.record && sameImages(pending.record, record);
  if (unchanged) {
    // Keep the original evidence for this digest set, but still repair a missing
    // or failed PR update before the workflow may checkpoint the revision.
    proposed = pending.record;
  } else {
    const source = replaceDefaults(main.source, record);
    const { data: base } = await api.git.getCommit({ ...repo, commit_sha: mainSha });
    const { data: tree } = await api.git.createTree({
      ...repo, base_tree: base.tree.sha,
      tree: [
        { path: DEFAULTS, mode: '100644', type: 'blob', content: source },
        { path: RECORD, mode: '100644', type: 'blob', content: `${JSON.stringify(record, null, 2)}\n` },
      ],
    });
    const { data: commit } = await api.git.createCommit({
      ...repo, message: `chore(images): promote canary-validated ${record.upstream_sha.slice(0, 7)}`,
      tree: tree.sha,
      // Retain branch ancestry for a non-forced update and include current main
      // so the PR diff contains only the promotion even when main has advanced.
      parents: [...new Set([head, mainSha].filter(Boolean))],
    });
    await assertCurrentMain();
    if (branch) {
      await api.git.updateRef({ ...repo, ref: `heads/${BRANCH}`, sha: commit.sha, force: false });
    } else {
      await api.git.createRef({ ...repo, ref: `refs/heads/${BRANCH}`, sha: commit.sha });
    }
    head = commit.sha;
  }

  const prs = await github.paginate(api.pulls.list, { ...repo, state: 'open', head: `${repo.owner}:${BRANCH}`, base: 'main', per_page: 100 });
  assert.ok(prs.length <= 1, 'Multiple open promotion PRs');
  const fields = {
    ...repo,
    title: `chore(images): promote canary-validated ${proposed.upstream_sha.slice(0, 7)}`,
    body: pullRequestBody(readDefaults(main.source), proposed),
  };
  let pr = prs[0];
  if (!pr) {
    ({ data: pr } = await api.pulls.create({ ...fields, head: BRANCH, base: 'main' }));
  } else if (pr.title !== fields.title || pr.body !== fields.body) {
    ({ data: pr } = await api.pulls.update({ ...fields, pull_number: pr.number }));
  }
  assert.equal(pr.head.sha, head, 'Promotion PR head changed concurrently');
  assert.equal(pr.state, 'open');
  return { changed: !unchanged, url: pr.html_url, head };
}

function recordFromEnv(env) {
  const run = (repo, id, attempt) => ({ id, attempt, url: `https://github.com/multigres/${repo}/actions/runs/${id}/attempts/${attempt}` });
  return {
    schema_version: 1,
    upstream_sha: env.UPSTREAM_SHA,
    operator_sha: env.OPERATOR_SHA,
    nightly_run: run('multigres', env.NIGHTLY_RUN_ID, env.NIGHTLY_RUN_ATTEMPT),
    canary_run: run('multigres-operator', env.GITHUB_RUN_ID, env.GITHUB_RUN_ATTEMPT),
    images: { multigres: env.MULTIGRES_IMAGE, pgctld: env.PGCTLD_IMAGE, 'multiadmin-web': env.MULTIADMIN_WEB_IMAGE },
  };
}

module.exports = { promote, recordFromEnv, validateRecord, validateDefaults, replaceDefaults, readDefaults, BRANCH, DEFAULTS, RECORD };

if (require.main === module) {
  assert.equal(process.argv[2], '--check', 'Usage: node scripts/promote-runtime-images.js --check');
  validateDefaults(fs.readFileSync(DEFAULTS, 'utf8'), JSON.parse(fs.readFileSync(RECORD, 'utf8')));
}
