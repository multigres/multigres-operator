const { test } = require('node:test');
const assert = require('node:assert/strict');
const { resolveSupabase, sourceRevision, bundledRevision, operatorPin, latestImage } = require('./supabase-compatibility');

const source = 'a'.repeat(40);
const operator = 'b'.repeat(40);
const multigres = 'c'.repeat(40);
const image = `docker.io/supabase/postgres@sha256:${'d'.repeat(64)}`;
const version = '17.11.0.001-multigres';
function provenance(revision = source) {
  const SLSA = { buildDefinition: { externalParameters: { configSource: {
    digest: { sha1: revision }, path: 'Dockerfile-multigres',
    uri: `https://github.com/supabase/postgres.git#${revision}`,
  } } } };
  return { 'linux/amd64': { SLSA }, 'linux/arm64': { SLSA: structuredClone(SLSA) } };
}
function lock() {
  return { nodes: { root: { inputs: { multigres: 'multigres' } }, multigres: {
    locked: { type: 'github', owner: 'multigres', repo: 'multigres', rev: multigres },
  } } };
}
function fixture() {
  const calls = [];
  const files = {
    'flake.lock': JSON.stringify(lock()),
    'Dockerfile-multigres': 'RUN nix profile add path:.#pgctld\n',
    'go.mod': `require github.com/multigres/multigres v0.0.0-20260901000000-${multigres.slice(0, 12)}\n`,
  };
  const options = {
    operatorSha: operator,
    json: async () => ({ results: [{ name: version, digest: image.split('@')[1] }] }),
    inspect: async (ref) => { assert.equal(ref, image); return provenance(); },
    github: { rest: { repos: {
      getContent: async (args) => {
        calls.push(args);
        return { data: { type: 'file', encoding: 'base64', content: Buffer.from(files[args.path]).toString('base64') } };
      },
      getCommit: async ({ ref }) => { assert.equal(ref, multigres.slice(0, 12)); return { data: { sha: multigres } }; },
      compareCommits: async ({ base, head }) => { assert.equal(base, multigres); assert.equal(head, multigres); return { data: { status: 'identical' } }; },
    } } },
  };
  // go.mod's usual block form and a single-line require are both supported.
  files['go.mod'] = `require (\n\tgithub.com/multigres/multigres v0.0.0-20260901000000-${multigres.slice(0, 12)}\n)\n`;
  return { options, calls, files };
}

test('resolves published image once and reads metadata at immutable revisions', async () => {
  const f = fixture();
  const result = await resolveSupabase(f.options);
  assert.deepEqual(result, { image, version, source_sha: source, multigres_sha: multigres, operator_multigres_sha: multigres });
  assert.deepEqual(f.calls.map(({ repo, path, ref }) => ({ repo, path, ref })), [
    { repo: 'postgres', path: 'flake.lock', ref: source },
    { repo: 'postgres', path: 'Dockerfile-multigres', ref: source },
    { repo: 'multigres-operator', path: 'go.mod', ref: operator },
  ]);
});

test('on-demand digest validation does not select a registry tag', async () => {
  const f = fixture();
  f.options.image = image;
  f.options.json = () => assert.fail('Unexpected tag selection');
  assert.equal((await resolveSupabase(f.options)).image, image);
});

for (const status of ['ahead', 'behind', 'diverged', undefined]) {
  for (const allowNewer of [false, true]) {
    test(`revision ordering ${status}, pgctld-only upgrade ${allowNewer}`, async () => {
      const f = fixture();
      f.options.allowNewer = allowNewer;
      f.options.github.rest.repos.compareCommits = async () => ({ data: { status } });
      if (status === 'ahead' && allowNewer) await resolveSupabase(f.options);
      else await assert.rejects(resolveSupabase(f.options), /relative to operator pin/);
    });
  }
}

for (const ref of ['supabase/postgres:latest', image.replace('docker.io', 'evil.example'), `${image}\nextra`, `${image};id`]) {
  test(`rejects invalid candidate before inspecting: ${JSON.stringify(ref)}`, async () => {
    const f = fixture();
    f.options.image = ref;
    f.options.inspect = () => assert.fail('Invalid image reached Docker');
    await assert.rejects(resolveSupabase(f.options), /pinned by digest/);
  });
}

test('latest selection skips architecture and OrioleDB tags and follows pages', async () => {
  const calls = [];
  const selected = await latestImage(async (url) => {
    calls.push(url);
    return calls.length === 1
      ? { results: [{ name: `${version}_amd64` }, { name: '17.11.0.002-orioledb-multigres' }], next: 'next' }
      : { results: [{ name: version, digest: image.split('@')[1] }] };
  });
  assert.equal(calls.length, 2);
  assert.ok(calls[1].endsWith('page=2'));
  assert.deepEqual(selected, { version, image });
});

test('missing tags, digest, or registry access fails selection', async () => {
  await assert.rejects(latestImage(async () => ({ results: [], next: null })), /No published/);
  await assert.rejects(latestImage(async () => ({ results: [{ name: version }] })), /no digest/);
  await assert.rejects(latestImage(async () => { throw new Error('registry unavailable'); }), /registry unavailable/);
});

test('missing, conflicting, or foreign source metadata fails closed', () => {
  assert.throws(() => sourceRevision({}), /missing Linux amd64/);
  const conflict = provenance();
  conflict['linux/arm64'] = provenance(operator)['linux/arm64'];
  assert.throws(() => sourceRevision(conflict), /different source/);
  const foreign = provenance();
  foreign['linux/amd64'].SLSA.buildDefinition.externalParameters.configSource.uri = `https://example.com/repo#${source}`;
  assert.throws(() => sourceRevision(foreign));
  const missing = provenance();
  delete missing['linux/amd64'].SLSA.buildDefinition.externalParameters.configSource.digest;
  assert.throws(() => sourceRevision(missing));
});

test('reads the source from earlier BuildKit provenance layout', () => {
  const data = provenance();
  for (const value of Object.values(data)) {
    const configSource = value.SLSA.buildDefinition.externalParameters.configSource;
    configSource.entryPoint = configSource.path;
    delete configSource.path;
    value.SLSA = { invocation: { configSource } };
  }
  assert.equal(sourceRevision(data), source);
});

test('a Docker build argument cannot be mistaken for the flake pgctld revision', async () => {
  const f = fixture();
  f.files['Dockerfile-multigres'] = `ARG PGCTLD_REV=${multigres}\n`;
  await assert.rejects(resolveSupabase(f.options), /does not build pgctld/);
});

test('rejects missing or foreign flake pins and missing operator pins', () => {
  assert.throws(() => bundledRevision({}));
  const foreign = lock();
  foreign.nodes.multigres.locked.owner = 'other';
  assert.throws(() => bundledRevision(foreign));
  assert.throws(() => operatorPin('module example.com/module\n'));
  assert.equal(operatorPin('\tgithub.com/multigres/multigres v0.1.0\n'), 'v0.1.0');
});
