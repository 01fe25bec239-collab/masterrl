// Fails if a BrowserWindow webPreference or the CSP is weakened. Runs as part of `pnpm lint`.
import { readdirSync, readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import {
  CONTENT_SECURITY_POLICY,
  DEV_CONTENT_SECURITY_POLICY,
  windowOptions,
} from '../src/main/security.ts';

const REQUIRED_WEB_PREFERENCES = {
  contextIsolation: true,
  sandbox: true,
  nodeIntegration: false,
  nodeIntegrationInWorker: false,
  nodeIntegrationInSubFrames: false,
  webSecurity: true,
  allowRunningInsecureContent: false,
  webviewTag: false,
  experimentalFeatures: false,
  preload: '/preload.cjs',
};
const REQUIRED_CSP =
  "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
  "font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; " +
  "frame-ancestors 'none'; object-src 'none'";
const REQUIRED_DEV_CSP = REQUIRED_CSP.replace(
  "script-src 'self'",
  "script-src 'self' 'unsafe-inline'",
).replace("style-src 'self'", "style-src 'self' 'unsafe-inline'");

// Source patterns that bypass or weaken the settings above.
const BANNED: [RegExp, string][] = [
  [/webPreferences\s*:/, 'webPreferences may only be set in src/main/security.ts'],
  [
    /new BrowserWindow\((?!windowOptions\()/,
    'create windows with new BrowserWindow(windowOptions(...))',
  ],
  [/@electron\/remote|enableRemoteModule/, 'the remote module is not allowed'],
  [
    /no-sandbox|disable-web-security|ignore-certificate-errors/,
    'Chromium security switches must stay on',
  ],
  [/'unsafe-eval'/, "'unsafe-eval' is not allowed in any CSP"],
  [/\.loadFile\(|registerFileProtocol|file:\/\//, 'serve the renderer through app://, not file://'],
];

const failures: string[] = [];
const webPreferences = windowOptions('/preload.cjs').webPreferences;
if (!isDeepStrictEqual(webPreferences, REQUIRED_WEB_PREFERENCES)) {
  failures.push(`webPreferences changed: ${JSON.stringify(webPreferences)}`);
}
if (CONTENT_SECURITY_POLICY !== REQUIRED_CSP)
  failures.push(`CSP changed: ${CONTENT_SECURITY_POLICY}`);
if (DEV_CONTENT_SECURITY_POLICY !== REQUIRED_DEV_CSP) {
  failures.push(`dev CSP changed: ${DEV_CONTENT_SECURITY_POLICY}`);
}

const srcDir = new URL('../src/', import.meta.url);
for (const name of readdirSync(srcDir, { recursive: true, encoding: 'utf8' })) {
  if (!name.endsWith('.ts') || name.endsWith('.test.ts') || name.endsWith('security.ts')) continue;
  const text = readFileSync(new URL(name, srcDir), 'utf8');
  for (const [pattern, reason] of BANNED) {
    if (pattern.test(text)) failures.push(`src/${name}: ${reason}`);
  }
}

if (failures.length > 0) {
  console.error(`Electron security check failed:\n- ${failures.join('\n- ')}`);
  process.exit(1);
}
console.log('Electron security check passed.');
