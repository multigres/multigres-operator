const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

function step(file, name, key, indent) {
  const workflow = fs.readFileSync(`.github/workflows/${file}.yaml`, 'utf8');
  const block = workflow.split(`      - name: ${name}\n`)[1]?.split(/\n      - |\n  [a-z]/)[0];
  assert.ok(block, `Missing ${name}`);
  return block.split(`${' '.repeat(indent)}${key}: |\n`)[1]
    .split(new RegExp(`\\n(?= {0,${indent + 1}}\\S)`))[0]
    .split('\n').map(line => line.slice(indent + 2)).join('\n');
}

test('coverage threshold accepts the boundary and rejects low or injected values', () => {
  const code = step('_reusable-test-coverage', 'Check coverage status', 'run', 8);
  for (const [coverage, ok] of [['70.0', true], ['69.9', false], ['70; echo bypass', false], ['', false]]) {
    const result = spawnSync('bash', ['-eo', 'pipefail', '-c', code], {
      encoding: 'utf8', env: { ...process.env, THRESHOLD: '70', NEW_COVERAGE: coverage, PREV_COVERAGE: '68' },
    });
    assert.equal(result.status === 0, ok, result.stderr);
  }
});

test('coverage archives reject traversal, duplicate entries, and oversized contents', t => {
  const code = step('coverage-comment', 'Extract artifact', 'run', 8);
  for (const variant of ['valid', 'traversal', 'duplicate', 'oversized']) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'coverage-archive-'));
    t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const fixture = spawnSync('python3', ['-c', `
import sys, zipfile
directory, variant = sys.argv[1:]
names = ['pr_number', 'threshold', 'new_coverage', 'prev_coverage', 'coverage_report.txt']
if variant == 'traversal': names[-1] = '../escaped'
if variant == 'duplicate': names[-1] = 'pr_number'
with zipfile.ZipFile(directory + '/coverage-report.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
    for name in names:
        archive.writestr(name, 'x' * (128 * 1024 + 1) if variant == 'oversized' else '42')
`, dir, variant], { encoding: 'utf8' });
    assert.equal(fixture.status, 0, fixture.stderr);
    const result = spawnSync('bash', ['-eo', 'pipefail', '-c', code.replaceAll('/tmp/coverage-', `${dir}/coverage-`)], { encoding: 'utf8' });
    assert.equal(result.status === 0, variant === 'valid', result.stderr);
    assert.equal(fs.existsSync(path.join(dir, 'escaped')), false);
    if (variant === 'valid') assert.equal(fs.readFileSync(path.join(dir, 'coverage-artifact/pr_number'), 'utf8'), '42');
  }
});

test('coverage comments require the triggering PR revision and treat report text as data', async () => {
  const code = step('coverage-comment', 'Post PR comment', 'script', 10);
  const run = new (Object.getPrototypeOf(async function () {}).constructor)('require', 'github', 'context', code);
  const sha = 'a'.repeat(40);
  const base = { state: 'open', head: { sha, repo: { id: 2 } }, base: { repo: { id: 1 } } };
  const payload = { repository: { id: 1 }, workflow_run: { head_sha: sha, head_repository: { id: 2 }, pull_requests: [{ number: 42 }] } };
  for (const change of [null, { state: 'closed' }, { head: { sha: 'b'.repeat(40), repo: { id: 2 } } },
    { head: { sha, repo: { id: 3 } } }, { base: { repo: { id: 4 } } }]) {
    const comments = [];
    const github = { rest: {
      pulls: { get: async () => ({ data: { ...base, ...change } }) },
      issues: { listComments: async () => ({ data: [] }), createComment: async args => comments.push(args) },
    } };
    const data = { pr_number: '42', threshold: '70', new_coverage: '81.2', prev_coverage: '80', coverage_report: '```\n@someone' };
    const fakeRequire = name => {
      assert.equal(name, 'fs/promises');
      return { readFile: async file => data[file.split('/').pop().replace('.txt', '')] };
    };
    const operation = run(fakeRequire, github, { repo: { owner: 'multigres', repo: 'multigres-operator' }, payload });
    if (change) {
      await assert.rejects(operation, /triggering PR revision/);
      assert.equal(comments.length, 0);
    } else {
      await operation;
      assert.equal(comments[0].issue_number, 42);
      assert.match(comments[0].body, /81.2%/);
      assert.ok(!comments[0].body.includes('@someone'));
      assert.ok(comments[0].body.includes("'''"));
    }
  }
});
