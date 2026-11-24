const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const makefile = fs.readFileSync('Makefile', 'utf8');
const workflow = fs.readFileSync('.github/workflows/_reusable-e2e.yaml', 'utf8');
const pullStep = workflow.split('      - name: Pull e2e images\n')[1]
  .split('\n      - name:')[0].split('        run: |\n')[1]
  .split('\n').map((line) => line.slice(10)).join('\n');
const defaults = {
  DefaultPostgresImage: 'ghcr.io/multigres/pgctld:sha-1111111',
  DefaultMultiadminImage: 'ghcr.io/multigres/multigres:sha-2222222',
  DefaultMultiadminWebImage: 'ghcr.io/multigres/multiadmin-web:sha-3333333',
  DefaultMultiorchImage: 'ghcr.io/multigres/multigres:sha-2222222',
  DefaultMultipoolerImage: 'ghcr.io/multigres/multigres:sha-2222222',
  DefaultMultigatewayImage: 'ghcr.io/multigres/multigres:sha-2222222',
  DefaultEtcdImage: 'gcr.io/etcd-development/etcd:v3.6.7',
  DefaultPostgresExporterImage: 'quay.io/prometheuscommunity/postgres-exporter:v0.20.1',
};
const legacy = [
  'ghcr.io/multigres/multigres:main',
  'ghcr.io/multigres/pgctld:main',
  'ghcr.io/multigres/multiadmin-web:main',
  defaults.DefaultEtcdImage,
];

test('a dedicated qualification requires an explicit pass for its named test', (t) => {
  assert.match(workflow, /- name: Apply runtime image overrides\n        if: inputs\.qualification-test == ''/);
  const step = workflow.split('      - name: Run e2e tests\n')[1]
    .split('\n      - name:')[0].split('        run: |\n')[1]
    .split('\n').map((line) => line.slice(10)).join('\n');
  const dedicated = step.split('\nif [ "$MODE" != "shared" ]; then\n')[1]
    .split('\nfi')[0];
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-qualification-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.writeFileSync(path.join(dir, 'go'), '#!/bin/sh\nprintf "%s\\n" "$GO_TEST_JSON"\nexit "$GO_TEST_STATUS"\n', { mode: 0o755 });
  const name = 'TestDynamicTablegroupLifecycle';
  for (const [action, testName, status, expected] of [
    ['pass', name, 0, 0], ['skip', name, 0, 1], ['pass', 'TestOther', 0, 1], ['pass', name, 2, 2],
  ]) {
    const result = spawnSync('bash', ['-c', `${step.split('\n')[0]}\npackages=(./test/e2e/dedicated/tablegroups/)\n${dedicated}`], {
      encoding: 'utf8', env: {
        ...process.env, PATH: `${dir}:${process.env.PATH}`, RUNNER_TEMP: dir,
        TIMEOUT_MINUTES: '60', REQUIRED_TEST: name, GO_TEST_STATUS: String(status),
        GO_TEST_JSON: JSON.stringify({ Action: action, Test: testName }),
      },
    });
    assert.equal(result.status, expected, result.stderr || result.stdout);
  }
});

function fixture(t, { oldFramework = false, images = defaults, failImage = '' } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-images-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  for (const name of ['api/v1alpha1', 'pkg/testutil', 'test/e2e/framework', 'bin']) {
    fs.mkdirSync(path.join(dir, name), { recursive: true });
  }
  // Historical Makefiles defaulted to four mutable images and pulled digest
  // references on the host too. Preserve both behaviors in the compatibility case.
  let fixtureMakefile = makefile;
  if (oldFramework) {
    fixtureMakefile = fixtureMakefile
      .replace(/^E2E_IMAGES \?=.*$/m, `E2E_IMAGES ?= ${legacy.join(' ')}`)
      .replace(/^pull-e2e-images:[^\n]*\n(?:\t[^\n]*\n)+/m,
        () => 'pull-e2e-images:\n\t@for img in $(E2E_IMAGES); do docker pull "$$img"; done\n');
  }
  fs.writeFileSync(path.join(dir, 'Makefile'), fixtureMakefile);
  fs.writeFileSync(path.join(dir, 'api/v1alpha1/image_defaults.go'),
    `package v1alpha1\nconst (\n${Object.entries(images).map(([name, image]) => `\t${name} = "${image}"`).join('\n')}\n)\n`);
  // Both generations contain this legacy variable, but only the older framework
  // uses it. Presence of the declaration alone must not add :main to new runs.
  fs.writeFileSync(path.join(dir, 'pkg/testutil/e2e.go'),
    `package testutil\nvar MultigresImages = []string{\n${legacy.map((image) => `\t"${image}",`).join('\n')}\n}\n`);
  fs.writeFileSync(path.join(dir, 'test/e2e/framework/image_overrides.go'), oldFramework
    ? 'package framework\nvar images = testutil.MultigresImages\n'
    : fs.readFileSync('test/e2e/framework/image_overrides.go', 'utf8'));
  const cache = path.join(dir, 'docker-cache');
  fs.writeFileSync(cache, '');
  fs.writeFileSync(path.join(dir, 'bin/docker'), `#!/bin/sh
case "$1" in
  pull) [ "$2" != "$MOCK_FAIL_IMAGE" ] || exit 9; printf '%s\\n' "$2" >> "$MOCK_CACHE" ;;
  save) grep -Fx -- "$2" "$MOCK_CACHE" > /dev/null ;;
  *) exit 1 ;;
esac
`, { mode: 0o755 });
  fs.writeFileSync(path.join(dir, 'bin/go'), '#!/bin/sh\nexit 0\n', { mode: 0o755 });
  const env = { ...process.env, PATH: `${path.join(dir, 'bin')}:${process.env.PATH}`, MOCK_CACHE: cache, MOCK_FAIL_IMAGE: failImage };
  delete env.E2E_IMAGES;
  delete env.MULTIGRES_IMAGES;
  function run(command, args, expectedStatus = 0) {
    const result = spawnSync(command, args, { cwd: dir, env, encoding: 'utf8' });
    assert.equal(result.status, expectedStatus, result.stderr || result.stdout);
  }
  return {
    run,
    pulled: () => fs.readFileSync(cache, 'utf8').trim().split('\n').filter(Boolean).sort(),
    checkLoadable: (images) => {
      for (const image of images) {
        if (!image.includes('@')) run('docker', ['save', image]);
      }
    },
  };
}

const unique = (images) => [...new Set(images)].sort();

test('local make pulls tagged committed defaults into an empty Docker cache', (t) => {
  const f = fixture(t);
  f.run('make', ['-s', 'pull-e2e-images']);
  assert.deepEqual(f.pulled(), unique(Object.values(defaults)));
  f.checkLoadable(Object.values(defaults));
});

test('local make retains an explicit E2E_IMAGES override', (t) => {
  const f = fixture(t);
  f.run('make', ['-s', 'pull-e2e-images', 'E2E_IMAGES=example.test/custom:tag']);
  assert.deepEqual(f.pulled(), ['example.test/custom:tag']);
});

test('local make leaves digest references for preparation inside Kind', (t) => {
  const digest = `example.test/runtime@sha256:${'a'.repeat(64)}`;
  const f = fixture(t);
  f.run('make', ['-s', 'pull-e2e-images', `E2E_IMAGES=${digest} example.test/exporter:tag`]);
  assert.deepEqual(f.pulled(), ['example.test/exporter:tag']);
});

test('local make stops immediately when a tagged image cannot be pulled', (t) => {
  const f = fixture(t, { failImage: 'example.test/fail:tag' });
  f.run('make', ['-s', 'pull-e2e-images', 'E2E_IMAGES=example.test/fail:tag example.test/later:tag'], 2);
  assert.deepEqual(f.pulled(), []);
});

for (const oldFramework of [false, true]) {
  for (const overrides of ['empty', 'partial', 'complete']) {
    test(`workflow pulls images needed by ${oldFramework ? 'older' : 'current'} refs with ${overrides} overrides`, (t) => {
      // The workflow applies overrides to image_defaults.go before pulling.
      const images = { ...defaults };
      if (overrides !== 'empty') images.DefaultPostgresImage = 'example.test/pgctld:custom';
      if (overrides === 'complete') {
        for (const name of ['DefaultMultiadminImage', 'DefaultMultiorchImage', 'DefaultMultipoolerImage', 'DefaultMultigatewayImage']) {
          images[name] = `ghcr.io/multigres/multigres@sha256:${'a'.repeat(64)}`;
        }
        images.DefaultMultiadminWebImage = 'example.test/multiadmin-web:custom';
      }
      const f = fixture(t, { oldFramework, images });
      f.run('bash', ['--noprofile', '--norc', '-eo', 'pipefail', '-c', pullStep]);
      const required = oldFramework ? [defaults.DefaultEtcdImage,
        overrides === 'empty' ? legacy[1] : images.DefaultPostgresImage,
        overrides === 'complete' ? images.DefaultMultiadminWebImage : legacy[2],
        overrides === 'complete' ? images.DefaultMultiadminImage : legacy[0],
      ] : Object.values(images);
      f.checkLoadable(required);
      const expected = Object.values(images).filter((image) => oldFramework || !image.includes('@'));
      for (const image of expected) assert.ok(f.pulled().includes(image));
      if (!oldFramework) assert.deepEqual(f.pulled(), unique(expected));
    });
  }
}
