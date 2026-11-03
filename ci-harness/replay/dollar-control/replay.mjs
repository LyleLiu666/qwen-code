import assert from 'node:assert/strict';
import { mkdtemp, readFile, stat, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [repo, argPath] = process.argv.slice(2);
const load = (path) => import(pathToFileURL(resolve(repo, 'packages/core/dist/src', path)).href);
const [{ EditTool }, { makeFakeConfig }] = await Promise.all([load('tools/edit.js'), load('test-utils/config.js')]);
const fixture = await mkdtemp(join(tmpdir(), 'q15595-edit-control-'));
const file = join(fixture, 'regex.yml');
const original = "| select('match', '^[sv]d[a-z]$')\n";
const expected = "| select('match', '^[sv]d[a-z]$') # updated\n";
try {
  const config = makeFakeConfig({ targetDir: fixture, cwd: fixture, telemetry: { enabled: false }, deferTelemetryInitialization: true });
  const tool = new EditTool(config);
  const steps = argPath ? JSON.parse(await readFile(argPath, 'utf8')) : [{ old_string: original.trimEnd(), new_string: expected.trimEnd() }];
  await writeFile(file, original);
  const outcomes = [];
  for (const args of steps) {
    await readFile(file);
    config.getFileReadCache().recordRead(file, await stat(file), { full: true, cacheable: true });
    const result = await tool.build({ ...args, file_path: file }).execute(new AbortController().signal);
    outcomes.push({ old_string: args.old_string, new_string: args.new_string, error: result.error ?? null, disk: await readFile(file, 'utf8') });
  }
  const disk = await readFile(file, 'utf8');
  if (!argPath) assert.equal(disk, expected);
  console.log(JSON.stringify({ original, expected, disk, matches: disk === expected, outcomes }));
} finally {
  await rm(fixture, { recursive: true, force: true });
}
