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
)
  .replace("style-src 'self'", "style-src 'self' 'unsafe-inline'")
  .replace("connect-src 'self'", "connect-src 'self' app://app");

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
  [/default-src|script-src/, 'CSP strings may only be defined in src/main/security.ts'],
];

// How src/main/index.ts must apply the CSP: strict on app://, the dev one only with a dev server.
const REQUIRED_IN_INDEX: [RegExp, string][] = [
  [
    /const devServerUrl = app\.isPackaged \? undefined : process\.env\.MRL_DEV_SERVER_URL;/,
    'the dev server URL must be ignored in packaged builds',
  ],
  [
    /headers\.set\('Content-Security-Policy', CONTENT_SECURITY_POLICY\);/,
    'app:// responses must send CONTENT_SECURITY_POLICY',
  ],
  [
    /if \(devServerUrl\) \{\s*session\.defaultSession\.webRequest\.onHeadersReceived\(/,
    'the dev CSP override must stay inside if (devServerUrl)',
  ],
  [
    /'Content-Security-Policy': \[DEV_CONTENT_SECURITY_POLICY\]/,
    'the dev server must get DEV_CONTENT_SECURITY_POLICY',
  ],
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
  const file = name.replaceAll('\\', '/');
  if (!file.endsWith('.ts') || file.endsWith('.test.ts') || file === 'main/security.ts') continue;
  const text = readFileSync(new URL(name, srcDir), 'utf8');
  for (const [pattern, reason] of BANNED) {
    if (pattern.test(text)) failures.push(`src/${name}: ${reason}`);
  }
}

const index = readFileSync(new URL('main/index.ts', srcDir), 'utf8');
for (const [pattern, reason] of REQUIRED_IN_INDEX) {
  if (!pattern.test(index)) failures.push(`src/main/index.ts: ${reason}`);
}
const count = (pattern: RegExp) => index.match(pattern)?.length ?? 0;
if (count(/Content-Security-Policy/g) !== 2 || count(/onHeadersReceived/g) !== 1) {
  failures.push('src/main/index.ts: set the CSP only in the two places checked above');
}

if (failures.length > 0) {
  console.error(`Electron security check failed:\n- ${failures.join('\n- ')}`);
  process.exit(1);
}
console.log('Electron security check passed.');
