// Starts the web dev server, then Electron pointed at it. Ctrl+C or closing the window stops both.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';

const electronPath = createRequire(import.meta.url)('electron') as string;
const server = await createServer({ root: fileURLToPath(new URL('../../web', import.meta.url)) });
await server.listen();
const url = server.resolvedUrls?.local[0];
if (!url) throw new Error('The web dev server did not report its address.');

const electron = spawn(electronPath, ['.'], {
  stdio: 'inherit',
  env: { ...process.env, MRL_DEV_SERVER_URL: url },
});
electron.on('exit', (code) => {
  void server.close().then(() => process.exit(code ?? 0));
});
