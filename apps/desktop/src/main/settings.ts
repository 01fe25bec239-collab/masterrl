import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import * as z from 'zod/mini';
import type { DesktopSettings } from './ipc.registry';

const schema = z.object({
  closeToTray: z.catch(z.boolean(), true),
  checkForUpdates: z.catch(z.boolean(), true),
});

/** The desktop app's own settings, kept in `<userData>/desktop-settings.json`. */
export class SettingsStore {
  private current: DesktopSettings = { closeToTray: true, checkForUpdates: true };
  private writing: Promise<void> = Promise.resolve();

  constructor(private readonly file: string) {}

  /** Reads the file; a missing or damaged one means the defaults. */
  async load(): Promise<DesktopSettings> {
    let raw: unknown = {};
    try {
      raw = JSON.parse(await readFile(this.file, 'utf8'));
    } catch {
      raw = {};
    }
    this.current = schema.parse(raw !== null && typeof raw === 'object' ? raw : {});
    return this.current;
  }

  get(): DesktopSettings {
    return this.current;
  }

  /** Saves a change (writes one at a time, each fully or not at all). */
  async set(patch: Partial<DesktopSettings>): Promise<DesktopSettings> {
    this.current = schema.parse({ ...this.current, ...patch });
    const text = JSON.stringify(this.current, null, 2);
    this.writing = this.writing.then(async () => {
      await mkdir(path.dirname(this.file), { recursive: true });
      const partial = `${this.file}.partial`;
      await writeFile(partial, text, { mode: 0o600 });
      await rename(partial, this.file);
    });
    await this.writing;
    return this.current;
  }
}
