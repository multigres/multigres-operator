const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const workflow = fs.readFileSync('.github/workflows/proto-sync-merged.yaml', 'utf8');
const script = workflow.split('          script: |\n')[1]
  .split('\n').map((line) => line.slice(12)).join('\n');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const mergedSha = 'b'.repeat(40);
const failedSha = 'a'.repeat(40);
const repo = { owner: 'multigres', repo: 'multigres-operator' };
const trackingIssue = (number, sha = failedSha) => ({
  number, body: `<!-- upstream-impact:${sha} -->`,
});

async function run(options = {}) {
  const calls = { comments: [], updates: [], comparisons: [], failures: [], warnings: [], reads: [] };
  const pr = {
    number: 819, merged: true, merge_commit_sha: 'c'.repeat(40),
    html_url: 'https://github.com/multigres/multigres-operator/pull/819',
    base: { ref: 'main' },
    head: { ref: 'chore/sync-proto-bbbbbbb', repo: { full_name: 'multigres/multigres-operator' } },
    user: { login: 'multigres-bot[bot]', type: 'Bot' },
    ...options.pr,
  };
  const github = {
    rest: {
      repos: {
        getContent: async (args) => {
          calls.reads.push(args);
          return { data: { content: Buffer.from(options.goMod ??
            `module example\nrequire (\n\tgithub.com/multigres/multigres v0.0.0-20261112064255-${mergedSha.slice(0, 12)}\n)\n`
          ).toString('base64') } };
        },
      },
      issues: {
        listForRepo: () => {},
        createComment: async (args) => {
          if (options.commentError) throw options.commentError;
          calls.comments.push(args);
        },
        update: async (args) => calls.updates.push(args),
      },
    },
    paginate: async (method, args) => {
      assert.equal(method, github.rest.issues.listForRepo);
      assert.deepEqual(args, { ...repo, state: 'open', labels: 'upstream-impact', per_page: 100 });
      return options.issues ?? [trackingIssue(818)];
    },
  };
  const upstream = { rest: { repos: {
    getCommit: async (args) => {
      assert.deepEqual(args, { owner: 'multigres', repo: 'multigres', ref: mergedSha.slice(0, 12) });
      return { data: { sha: mergedSha } };
    },
    compareCommitsWithBasehead: async (args) => {
      calls.comparisons.push(args);
      if (options.compareError) throw options.compareError;
      return { data: {
        status: options.status ?? 'ahead',
        html_url: `https://github.com/multigres/multigres/compare/${args.basehead}`,
      } };
    },
  } } };
  const core = {
    info: () => {}, setFailed: (message) => calls.failures.push(message),
    warning: (message) => calls.warnings.push(message),
  };
  const getOctokit = (token) => {
    assert.equal(token, 'upstream-read-token');
    return upstream;
  };
  try {
    await new AsyncFunction('github', 'context', 'core', 'getOctokit', 'process', script)(
      github, { repo, payload: { pull_request: pr } }, core, getOctokit,
      { env: { EXPECTED_BOT_LOGIN: 'multigres-bot[bot]', UPSTREAM_TOKEN: 'upstream-read-token', ...options.env } },
    );
  } catch (error) {
    calls.error = error;
  }
  return calls;
}

test('closes both tracking formats with the merged revision and an explanatory comment', async () => {
  const result = await run({
    pr: { body: 'A stale description naming a different revision' },
    issues: [trackingIssue(818), {
      number: 820,
      body: `<!-- proto-sync-failure:817 -->\n## Upstream proto sync failed\n\n- **Upstream SHA:** \`${failedSha}\`\n`,
    }],
  });
  assert.equal(result.error, undefined);
  assert.equal(result.failures.length, 0);
  assert.deepEqual(result.reads, [{ ...repo, path: 'go.mod', ref: 'c'.repeat(40) }]);
  assert.deepEqual(result.updates.map(({ issue_number, state, state_reason }) =>
    ({ issue_number, state, state_reason })), [818, 820].map((issue_number) =>
    ({ issue_number, state: 'closed', state_reason: 'completed' })));
  for (const comment of result.comments) {
    assert.match(comment.body, /merged sync PR #819/);
    assert.ok(comment.body.includes(`${failedSha}...${mergedSha}`));
  }
  assert.equal(result.comparisons[0].basehead, `${failedSha}...${mergedSha}`);
});

test('closes an issue for the exact merged revision', async () => {
  const result = await run({ status: 'identical', issues: [trackingIssue(818, mergedSha)] });
  assert.equal(result.error, undefined);
  assert.equal(result.updates.length, 1);
});

for (const status of ['behind', 'diverged', 'unknown']) {
  test(`leaves issues open when comparison is ${status}`, async () => {
    const result = await run({ status });
    assert.equal(result.error, undefined);
    assert.equal(result.updates.length, 0);
    assert.equal(result.comments.length, 0);
  });
}

test('ignores PRs, unrelated incidents and malformed tracking bodies', async () => {
  const result = await run({ issues: [
    { ...trackingIssue(1), pull_request: {} },
    { number: 2, body: '<!-- nightly-compatibility-canary -->' },
    trackingIssue(3, 'bad-sha'),
    { number: 4, body: `- **Upstream SHA:** \`${failedSha}\`` },
    { number: 5, body: '<!-- proto-sync-failure:123 -->' },
    { number: 6, body: null },
  ] });
  assert.equal(result.error, undefined);
  assert.deepEqual(result.comparisons, []);
  assert.deepEqual(result.updates, []);
});

for (const [name, pr] of Object.entries({
  unmerged: { merged: false },
  'other base': { base: { ref: 'release' } },
  fork: { head: { ref: 'chore/sync-proto-bbbbbbb', repo: { full_name: 'someone/fork' } } },
  'other branch': { head: { ref: 'feature', repo: { full_name: 'multigres/multigres-operator' } } },
  human: { user: { login: 'multigres-bot[bot]', type: 'User' } },
  'other bot': { user: { login: 'other[bot]', type: 'Bot' } },
})) {
  test(`ignores ${name} PRs`, async () => {
    const result = await run({ pr });
    assert.equal(result.error, undefined);
    assert.deepEqual(result.reads, []);
    assert.deepEqual(result.updates, []);
  });
}

test('fails closed without the configured bot identity or a usable dependency pin', async () => {
  for (const options of [{ env: { EXPECTED_BOT_LOGIN: '' } }, { goMod: 'module example\n' }]) {
    const result = await run(options);
    assert.equal(result.failures.length, 1);
    assert.deepEqual(result.updates, []);
  }
});

test('leaves unresolvable revisions open', async () => {
  const result = await run({ compareError: Object.assign(new Error('Not found'), { status: 404 }) });
  assert.equal(result.error, undefined);
  assert.equal(result.warnings.length, 1);
  assert.deepEqual(result.updates, []);
});

test('surfaces upstream API failures without closing issues', async () => {
  const error = Object.assign(new Error('Rate limited'), { status: 403 });
  const result = await run({ compareError: error });
  assert.equal(result.error, error);
  assert.deepEqual(result.updates, []);
});

test('does not close an issue when posting its explanation fails', async () => {
  const error = new Error('Comment failed');
  const result = await run({ commentError: error });
  assert.equal(result.error, error);
  assert.deepEqual(result.updates, []);
});
