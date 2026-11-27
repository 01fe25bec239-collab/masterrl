import { mkdir, readdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import * as z from 'zod/mini';
import type { CrashSummary } from '../ipc.registry';
import { redact } from './redact';

/** Format tag shared with the core's and agent's reports. */
export const SCHEMA = 'mrl-crash/v1';
/** Where crash reports become GitHub issues (opened for the person to review and send). */
export const ISSUES_URL = 'https://github.com/01fe25bec239-collab/masterrl/issues/new';
/** Reports kept per program. */
export const KEEP = 20;
/** Browsers cut long addresses; keep the issue link well below that. */
const MAX_URL = 7500;

const reportSchema = z.object({
  schema: z.literal(SCHEMA),
  program: z.string().check(z.maxLength(40)),
  version: z.string().check(z.maxLength(64)),
  commit: z.optional(z.string().check(z.maxLength(64))),
  os: z.string().check(z.maxLength(32)),
  arch: z.string().check(z.maxLength(32)),
  time: z.string().check(z.maxLength(40)),
  thread: z.optional(z.string()),
  message: z.string(),
  location: z.optional(z.string()),
  frames: z.optional(z.array(z.string())),
});
export type CrashReport = z.output<typeof reportSchema>;

const ID = /^[a-z][a-z0-9-]{0,40}-\d{1,16}-\d{1,10}$/;

/** Every text field passes `redact` again on the way out, whoever wrote the file. */
function scrub(r: CrashReport): CrashReport {
  return {
    ...r,
    thread: r.thread === undefined ? undefined : redact(r.thread),
    message: redact(r.message),
    location: r.location === undefined ? undefined : redact(r.location),
    frames: r.frames?.slice(0, 64).map(redact),
  };
}

/**
 * The crash reports on this computer: the app's own folder (where the app's and the core's
 * reports go) and the agent's default folder. Nothing here is ever uploaded.
 */
export class CrashReports {
  constructor(
    /** The app's folder, first; reports are written here. */
    private readonly folders: readonly string[],
  ) {}

  /** Where the app (and the core it starts) writes reports. */
  get folder(): string {
    return this.folders[0] ?? '';
  }

  private async find(id: string): Promise<string | null> {
    if (!ID.test(id)) return null;
    for (const dir of this.folders) {
      const file = path.join(dir, `${id}.json`);
      try {
        await readFile(file, { flag: 'r' });
        return file;
      } catch {
        // not in this folder
      }
    }
    return null;
  }

  private async load(file: string): Promise<CrashReport | null> {
    try {
      const raw = await readFile(file, 'utf8');
      if (raw.length > 512_000) return null;
      const r = reportSchema.safeParse(JSON.parse(raw));
      return r.success ? scrub(r.data) : null;
    } catch {
      return null;
    }
  }

  /** Newest first. Files that aren't reports are skipped. */
  async list(): Promise<CrashSummary[]> {
    const out: (CrashSummary & { at: number })[] = [];
    const seen = new Set<string>();
    for (const dir of this.folders) {
      let names: string[] = [];
      try {
        names = await readdir(dir);
      } catch {
        continue;
      }
      for (const name of names) {
        const id = name.replace(/\.json$/, '');
        if (!name.endsWith('.json') || !ID.test(id) || seen.has(id)) continue;
        const r = await this.load(path.join(dir, name));
        if (!r) continue;
        seen.add(id);
        const at = Date.parse(r.time);
        out.push({ id, program: r.program, time: r.time, message: r.message, at });
      }
    }
    out.sort((a, b) => b.at - a.at || b.id.localeCompare(a.id));
    return out.map((s) => ({ id: s.id, program: s.program, time: s.time, message: s.message }));
  }

  /** The report as text to copy or share. */
  async read(id: string): Promise<string> {
    const file = await this.find(id);
    const r = file ? await this.load(file) : null;
    if (!r) throw new Error('This crash report is gone. Refresh the list.');
    return JSON.stringify(r, null, 2);
  }

  async delete(id: string): Promise<boolean> {
    const file = await this.find(id);
    if (!file) return false;
    await rm(file, { force: true });
    return true;
  }

  /** GitHub's new-issue form, filled in with the report (cut to fit an address). */
  async issueUrl(id: string): Promise<string> {
    const text = await this.read(id);
    const r = JSON.parse(text) as CrashReport;
    const title = `Crash: ${r.program} ${r.version}: ${r.message.slice(0, 80)}`;
    const intro =
      'What were you doing when it happened?\n\n\n' +
      '<!-- The report below was made on your computer with paths, keys and data removed. ' +
      'Check it before you submit. -->\n\n';
    const build = (body: string) => {
      const u = new URL(ISSUES_URL);
      u.searchParams.set('title', title);
      u.searchParams.set('labels', 'crash');
      u.searchParams.set('body', `${intro}\`\`\`json\n${body}\n\`\`\``);
      return u.toString();
    };
    let body = text;
    let url = build(body);
    while (url.length > MAX_URL && body.length > 200) {
      body = `${body.slice(0, Math.floor(body.length * 0.8))}\n…(cut)`;
      url = build(body);
    }
    return url;
  }

  /** Writes a report of the app itself (main process or a window), redacted. */
  async write(
    program: string,
    fields: Pick<CrashReport, 'version' | 'message'> & Partial<CrashReport>,
  ): Promise<string> {
    const report: CrashReport = scrub({
      schema: SCHEMA,
      program,
      os: process.platform === 'win32' ? 'windows' : process.platform,
      arch: process.arch,
      time: new Date().toISOString(),
      ...fields,
    });
    await mkdir(this.folder, { recursive: true, mode: 0o700 });
    const id = `${program}-${String(Date.now()).padStart(13, '0')}-${String(process.pid)}`;
    const file = path.join(this.folder, `${id}.json`);
    const partial = path.join(this.folder, `.${id}.json.partial`);
    await writeFile(partial, JSON.stringify(report), { mode: 0o600 });
    await rename(partial, file);
    const mine = (await readdir(this.folder))
      .filter((n) => n.startsWith(`${program}-`) && n.endsWith('.json'))
      .sort();
    for (const old of mine.slice(0, Math.max(0, mine.length - KEEP))) {
      await rm(path.join(this.folder, old), { force: true });
    }
    return id;
  }
}

/** Stack frames of a JavaScript error as function names only (no files or lines). */
export function framesOf(stack: string | undefined): string[] {
  return (stack ?? '')
    .split('\n')
    .map((l) => /^\s*at\s+([^\s(]+)/.exec(l)?.[1])
    .filter((f): f is string => f !== undefined && !f.includes('/') && !f.includes('\\'))
    .slice(0, 64);
}
