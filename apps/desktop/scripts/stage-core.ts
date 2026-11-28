// Copies the built core and agent into dist/bin, which the dev package ships as
// resources/bin (package.json `extraResources`). Electron main starts resources/bin/mrl-server
// when packaged; the core finds PostgreSQL in postgres/bin next to it, then on PATH.
//
// From MRL_BIN_DIR, else ../../target/release (`cargo build --release -p mrl-server -p mrl-agent`).
import { chmodSync, copyFileSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import path from 'node:path';

const here = import.meta.dirname;
const from = process.env.MRL_BIN_DIR ?? path.join(here, '../../../target/release');
const to = path.join(here, '../dist/bin');
const exe = (name: string) => (process.platform === 'win32' ? `${name}.exe` : name);

const missing = ['mrl-server', 'mrl-agent'].filter((n) => !existsSync(path.join(from, exe(n))));
if (missing.length > 0) {
  console.error(
    `stage-core: ${missing.map(exe).join(' and ')} not found in ${from}. ` +
      'Build them first: cargo build --release -p mrl-server -p mrl-agent (or set MRL_BIN_DIR).',
  );
  process.exit(1);
}
rmSync(to, { recursive: true, force: true });
mkdirSync(to, { recursive: true });
for (const name of ['mrl-server', 'mrl-agent']) {
  const target = path.join(to, exe(name));
  copyFileSync(path.join(from, exe(name)), target);
  chmodSync(target, 0o755);
  console.log(`stage-core: ${exe(name)} → dist/bin`);
}
