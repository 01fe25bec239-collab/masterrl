import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { DEFAULT_GRPC_PORT, GrpcPortMemory, portFree } from './ports';

const file = () => path.join(mkdtempSync(path.join(tmpdir(), 'mrl-ports-')), 'core-ports.json');

describe('gRPC port memory', () => {
  it('asks for the default first, then for the port the core used last', async () => {
    const f = file();
    const m = new GrpcPortMemory(f, () => Promise.resolve(true));
    expect(await m.choose()).toBe(DEFAULT_GRPC_PORT);
    await m.remember(50123);
    expect(await m.choose()).toBe(50123);
    expect(JSON.parse(readFileSync(f, 'utf8'))).toEqual({ grpc: 50123 });
  });

  it('asks for any free port when the remembered one is taken', async () => {
    const f = file();
    const m = new GrpcPortMemory(f, (p) => Promise.resolve(p !== 50123));
    await m.remember(50123);
    expect(await m.choose()).toBe(0);
  });

  it('ignores a damaged or hostile file', async () => {
    for (const text of ['not json', '{"grpc": 70000}', '{"grpc": "9443"}', '[]']) {
      const f = file();
      writeFileSync(f, text);
      expect(await new GrpcPortMemory(f, () => Promise.resolve(true)).choose()).toBe(
        DEFAULT_GRPC_PORT,
      );
    }
  });

  it('sees a port in use as taken', async () => {
    const server = createServer();
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const address = server.address();
    const port = typeof address === 'object' && address ? address.port : 0;
    expect(await portFree(port)).toBe(false);
    await new Promise<void>((r) =>
      server.close(() => {
        r();
      }),
    );
    expect(await portFree(port)).toBe(true);
  });
});
