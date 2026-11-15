import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { generate } from '../scripts/generate';
import { createMrlClient } from './fixtures/fixture-api.gen';

const here = path.dirname(fileURLToPath(import.meta.url));
const spec = path.join(here, 'fixtures/fixture-api.yaml');
const committed = path.join(here, 'fixtures/fixture-api.gen.ts');

let dir: string;
let out: string;

beforeAll(async () => {
  dir = await mkdtemp(path.join(tmpdir(), 'mrl-api-gen-'));
  out = path.join(dir, 'api.ts');
  await generate({ input: spec, out });
}, 30_000);

afterAll(async () => {
  await rm(dir, { recursive: true, force: true });
});

// Correct calls must compile; each misuse must be a type error (checked by @ts-expect-error).
const usage = `
import { createMrlClient } from './api';
const api = createMrlClient({ baseUrl: 'http://127.0.0.1:7801', token: 't' });

export async function ok() {
  const page = await api.GET('/api/v1/projects', { query: { limit: 10 } });
  const first: string | undefined = page.items[0]?.name;
  const next: string | null = page.next_cursor;
  await api.GET('/api/v1/projects');
  const created = await api.POST('/api/v1/projects', { body: { name: 'demo' } });
  const id: string = created.id;
  const one = await api.GET('/api/v1/projects/{prj}', { path: { prj: 'prj_1' } });
  const gone: undefined = await api.DELETE('/api/v1/projects/{prj}', { path: { prj: 'prj_1' } });
  const cancel = await api.POST('/api/v1/runs/{run}:cancel', { path: { run: 'run_1' } });
  const state: 'CANCELLING' = cancel.state;
  return [first, next, id, one.name, gone, state];
}

export async function bad() {
  // @ts-expect-error unknown path
  await api.GET('/api/v1/nope');
  // @ts-expect-error no PUT on this path
  await api.PUT('/api/v1/projects');
  // @ts-expect-error missing path params
  await api.GET('/api/v1/projects/{prj}');
  // @ts-expect-error body has the wrong shape
  await api.POST('/api/v1/projects', { body: { title: 'x' } });
  // @ts-expect-error body is required
  await api.POST('/api/v1/projects');
  // @ts-expect-error query value has the wrong type
  await api.GET('/api/v1/projects', { query: { limit: 'ten' } });
  const p = await api.GET('/api/v1/projects/{prj}', { path: { prj: 'prj_1' } });
  // @ts-expect-error reply type is checked
  const n: number = p.name;
  return n;
}
`;

describe('generate', () => {
  it('writes types and a typed client that TypeScript checks', async () => {
    await writeFile(path.join(dir, 'usage.ts'), usage);
    const program = ts.createProgram([path.join(dir, 'usage.ts')], {
      strict: true,
      noUncheckedIndexedAccess: true,
      target: ts.ScriptTarget.ES2023,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
      lib: ['lib.es2023.d.ts', 'lib.dom.d.ts'],
      types: [],
      noEmit: true,
      skipLibCheck: true,
    });
    const messages = ts
      .getPreEmitDiagnostics(program)
      .map((d) => ts.flattenDiagnosticMessageText(d.messageText, '\n'));
    expect(messages).toEqual([]);
  }, 60_000);

  it('produces a client whose calls hit the right URLs', async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(Response.json({ items: [], next_cursor: null }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const api = createMrlClient({ baseUrl: 'http://127.0.0.1:7801', token: 't', fetch });

    await expect(
      api.GET('/api/v1/projects', { query: { limit: 5, cursor: 'c 1' } }),
    ).resolves.toEqual({
      items: [],
      next_cursor: null,
    });
    await expect(
      api.DELETE('/api/v1/projects/{prj}', { path: { prj: 'prj_a/b' } }),
    ).resolves.toBeUndefined();

    const urls = fetch.mock.calls.map(([url]) => String(url as URL));
    expect(urls).toEqual([
      'http://127.0.0.1:7801/api/v1/projects?limit=5&cursor=c+1',
      'http://127.0.0.1:7801/api/v1/projects/prj_a%2Fb',
    ]);
    expect(fetch.mock.calls[1]?.[1]?.method).toBe('DELETE');
  });

  it('keeps the committed fixture client up to date', async () => {
    await expect(generate({ input: spec, out: committed, check: true })).resolves.toBe(true);
  }, 30_000);

  it('--check reports whether the file is current', async () => {
    await expect(generate({ input: spec, out, check: true })).resolves.toBe(true);
    await writeFile(out, '// stale\n');
    await expect(generate({ input: spec, out, check: true })).resolves.toBe(false);
    await generate({ input: spec, out });
  }, 30_000);
});
