import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import path from 'node:path';
import * as z from 'zod/mini';

/** The core's gRPC port when nothing is remembered yet (`server.listeners.grpc`'s default). */
export const DEFAULT_GRPC_PORT = 9443;

const saved = z.object({ grpc: z.number().check(z.int(), z.gte(1), z.lte(65535)) });

/** True when nothing listens on `127.0.0.1:<port>` right now. */
export function portFree(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = createServer();
    server.once('error', () => {
      resolve(false);
    });
    server.listen({ port, host: '127.0.0.1', exclusive: true }, () => {
      server.close(() => {
        resolve(true);
      });
    });
  });
}

/**
 * Keeps the Solo core's gRPC port the same from launch to launch, so local agents (which dial
 * the gateway there and read their settings only when they start) keep reaching it.
 *
 * Each start asks for the last port the core used (the default on first run). Only when that
 * port is taken does it ask for any free one (`0`); the new port is remembered once the core
 * reports it, and the agent settings are rewritten then.
 */
export class GrpcPortMemory {
  constructor(
    private readonly file: string,
    private readonly isFree: (port: number) => Promise<boolean> = portFree,
  ) {}

  /** The last port the core used, if any. */
  async remembered(): Promise<number | undefined> {
    try {
      const parsed = saved.safeParse(JSON.parse(await readFile(this.file, 'utf8')));
      return parsed.success ? parsed.data.grpc : undefined;
    } catch {
      return undefined;
    }
  }

  /** The port to ask for: the remembered one (or the default) when free, else `0`. */
  async choose(): Promise<number> {
    const want = (await this.remembered()) ?? DEFAULT_GRPC_PORT;
    return (await this.isFree(want)) ? want : 0;
  }

  /** Notes the port the core reported. */
  async remember(port: number): Promise<void> {
    if ((await this.remembered()) === port) return;
    await mkdir(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${String(process.pid)}.tmp`;
    await writeFile(tmp, `${JSON.stringify({ grpc: port })}\n`, { mode: 0o600 });
    await rename(tmp, this.file);
  }
}
