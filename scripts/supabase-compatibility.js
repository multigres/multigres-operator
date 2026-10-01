const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');

const IMAGE = /^docker\.io\/supabase\/postgres@sha256:[0-9a-f]{64}$/;
const VERSION = /^[0-9]+(?:\.[0-9]+){3}-multigres$/;
const SHA = /^[0-9a-f]{40}$/;

function sourceRevision(provenance) {
  const revisions = Object.entries(provenance).filter(([platform]) => platform.startsWith('linux/'))
    .map(([, value]) => {
      const slsa = value.SLSA;
      const source = slsa?.buildDefinition?.externalParameters?.configSource || slsa?.invocation?.configSource;
      assert.ok(source, 'Supabase image is missing source provenance');
      const revision = source.digest?.sha1;
      assert.match(revision || '', SHA, 'Supabase provenance must contain a full source revision');
      assert.equal(source.uri, `https://github.com/supabase/postgres.git#${revision}`);
      assert.equal(source.path || source.entryPoint, 'Dockerfile-multigres');
      return revision;
    });
  assert.ok(provenance['linux/amd64'], 'Supabase image is missing Linux amd64 provenance');
  assert.equal(new Set(revisions).size, 1, 'Supabase platforms have different source revisions');
  return revisions[0];
}

function bundledRevision(lock) {
  const input = lock.nodes?.root?.inputs?.multigres;
  assert.equal(typeof input, 'string', 'Missing direct Multigres flake input');
  const locked = lock.nodes[input]?.locked;
  assert.equal(locked?.type, 'github');
  assert.equal(locked.owner, 'multigres');
  assert.equal(locked.repo, 'multigres');
  assert.match(locked.rev || '', SHA, 'Missing bundled Multigres revision');
  return locked.rev;
}

function operatorPin(goMod) {
  const matches = [...goMod.matchAll(/^\s*(?:require\s+)?github\.com\/multigres\/multigres\s+(v\S+)\s*$/gm)];
  assert.equal(matches.length, 1, 'Expected one Multigres dependency');
  assert.ok(!/^replace\b[\s\S]*github\.com\/multigres\/multigres/m.test(goMod), 'Replaced Multigres dependency is unsupported');
  const version = matches[0][1];
  return version.match(/-([0-9a-f]{12,40})$/)?.[1] || version;
}

async function latestImage(json) {
  for (let page = 1; page <= 100; page++) {
    const tags = await json(`https://hub.docker.com/v2/repositories/supabase/postgres/tags/?page_size=100&name=-multigres&ordering=last_updated&page=${page}`);
    const tag = tags.results.find((entry) => VERSION.test(entry.name));
    if (tag) {
      assert.match(tag.digest || '', /^sha256:[0-9a-f]{64}$/, 'Published Supabase tag has no digest');
      return { version: tag.name, image: `docker.io/supabase/postgres@${tag.digest}` };
    }
    if (!tags.next) break;
  }
  throw new Error('No published Supabase Multigres image found');
}

async function resolveSupabase({ github, operatorSha, image = '', allowNewer = false, json = fetchJSON, inspect = inspectProvenance }) {
  assert.match(operatorSha, SHA);
  const selected = image ? { image, version: 'digest' } : await latestImage(json);
  assert.match(selected.image, IMAGE, 'Expected a docker.io/supabase/postgres image pinned by digest');
  const source = sourceRevision(await inspect(selected.image));
  async function contents(owner, repo, path, ref) {
    const { data } = await github.rest.repos.getContent({ owner, repo, path, ref });
    assert.equal(data.type, 'file');
    assert.equal(data.encoding, 'base64');
    return Buffer.from(data.content, 'base64').toString('utf8');
  }
  const lock = JSON.parse(await contents('supabase', 'postgres', 'flake.lock', source));
  const dockerfile = await contents('supabase', 'postgres', 'Dockerfile-multigres', source);
  // Older images built pgctld from a separate Docker build argument. Their
  // flake input cannot establish the revision of the bundled binary.
  assert.ok(dockerfile.includes('path:.#pgctld') && !dockerfile.includes('PGCTLD_REV'),
    'Supabase image does not build pgctld from the recorded flake input');
  const revision = bundledRevision(lock);
  const goMod = await contents('multigres', 'multigres-operator', 'go.mod', operatorSha);
  const { data: pin } = await github.rest.repos.getCommit({ owner: 'multigres', repo: 'multigres', ref: operatorPin(goMod) });
  assert.match(pin.sha, SHA);
  const { data: comparison } = await github.rest.repos.compareCommits({ owner: 'multigres', repo: 'multigres', base: pin.sha, head: revision });
  assert.ok(comparison.status === 'identical' || (allowNewer && comparison.status === 'ahead'),
    `Supabase ${selected.version} (${selected.image}) bundles pgctld ${revision}, which is ${comparison.status} relative to operator pin ${pin.sha}; ` +
    'require equality or explicitly allow a newer pgctld-only upgrade');
  return { ...selected, source_sha: source, multigres_sha: revision, operator_multigres_sha: pin.sha };
}

async function fetchJSON(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
  assert.ok(response.ok, `Registry request failed: HTTP ${response.status}`);
  return response.json();
}

function inspectProvenance(image) {
  return JSON.parse(execFileSync('docker', ['buildx', 'imagetools', 'inspect', image, '--format', '{{json .Provenance}}'],
    { encoding: 'utf8', timeout: 120000, maxBuffer: 16 * 1024 * 1024 }));
}

module.exports = { resolveSupabase, sourceRevision, bundledRevision, operatorPin, latestImage };
