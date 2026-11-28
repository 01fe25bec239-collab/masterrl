import { describe, expect, it, vi } from 'vitest';
import type { CoreClient } from './core/client';
import { Updates, type Updater } from './updates';

function setup(version: string | null, prepare: unknown) {
  const order: string[] = [];
  const u: Updater = {
    autoDownload: true,
    autoInstallOnAppQuit: true,
    checkForUpdates: vi.fn(() => Promise.resolve(version ? { updateInfo: { version } } : null)),
    downloadUpdate: vi.fn(() => {
      order.push('download');
      return Promise.resolve([]);
    }),
    quitAndInstall: vi.fn(() => order.push('install')),
    on: vi.fn(),
  };
  const core = {
    call: vi.fn((_m: string, path: string) => {
      order.push(path);
      return Promise.resolve(prepare);
    }),
  } as unknown as CoreClient;
  return {
    u,
    order,
    updates: new Updates({ updater: () => Promise.resolve(u), core, current: '1.0.0' }),
  };
}

describe('updates', () => {
  it('finds nothing new when the feed has this version', async () => {
    const { updates, u } = setup('1.0.0', null);
    expect((await updates.check()).phase).toBe('up_to_date');
    expect(u.autoDownload).toBe(false);
    expect(u.autoInstallOnAppQuit).toBe(false);
  });

  it('saves checkpoints and a backup through the core before installing', async () => {
    const { updates, order } = setup('1.1.0', {
      ok: true,
      value: { ready: true, checkpoints: [{ name: 'a', outcome: 'timed_out' }], backup: 'b' },
    });
    expect(await updates.check()).toEqual({ phase: 'available', version: '1.1.0' });
    const s = await updates.install();
    expect(order).toEqual(['download', '/api/v1/desktop/update/prepare', 'install']);
    expect(s.detail).toMatch(/1 run\(s\) could not save a checkpoint/);
  });

  it('waits when the backup failed or the core is away', async () => {
    for (const prepare of [
      { ok: true, value: { ready: false, checkpoints: [], backup_error: 'disk full' } },
      { ok: false, failure: { code: 'MRL-ENV-302', detail: 'The app is reconnecting.' } },
    ]) {
      const { updates, order } = setup('1.1.0', prepare);
      await updates.check();
      const s = await updates.install();
      expect(s.phase).toBe('ready');
      expect(order).not.toContain('install');
    }
  });

  it('does not install without an update', async () => {
    const { updates, order } = setup(null, null);
    await updates.install();
    expect(order).toEqual([]);
  });
});
