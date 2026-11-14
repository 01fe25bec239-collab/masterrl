import { readFileSync } from 'node:fs';
import path from 'node:path';
import { _electron as electron, expect, test } from '@playwright/test';

const release = path.join(import.meta.dirname, '../release');
const { version } = JSON.parse(
  readFileSync(path.join(import.meta.dirname, '../package.json'), 'utf8'),
) as {
  version: string;
};

/** The unpacked app that `electron-builder --dir` writes for this OS. */
function packagedApp(): string {
  if (process.platform === 'darwin') {
    const dir = process.arch === 'arm64' ? 'mac-arm64' : 'mac';
    return path.join(release, dir, 'MasterRL.app/Contents/MacOS/MasterRL');
  }
  if (process.platform === 'win32') return path.join(release, 'win-unpacked/MasterRL.exe');
  return path.join(release, 'linux-unpacked/masterrl');
}

test('packaged app opens one secure window', async () => {
  const app = await electron.launch({ executablePath: packagedApp() });
  try {
    const page = await app.firstWindow();
    await expect(page.getByRole('heading', { name: 'MasterRL' })).toBeVisible();
    expect(page.url()).toBe('app://app/index.html');
    expect(app.windows()).toHaveLength(1);

    // Typed bridge works; Node is not reachable from the page.
    expect(await page.evaluate(() => window.mrl.app.getVersion())).toBe(version);
    expect(
      await page.evaluate(() => ['require', 'process'].filter((k) => k in globalThis)),
    ).toEqual([]);

    // CSP blocks inline scripts.
    const inlineRan = await page.evaluate(() => {
      const script = document.createElement('script');
      script.textContent = 'document.body.dataset.inline = "ran"';
      document.head.append(script);
      return document.body.dataset.inline === 'ran';
    });
    expect(inlineRan).toBe(false);
  } finally {
    await app.close();
  }
});
