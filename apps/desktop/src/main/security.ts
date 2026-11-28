import type { BrowserWindowConstructorOptions, WebPreferences } from 'electron';

// Checked by scripts/check-security.ts: weakening any of these fails the build.
export const WEB_PREFERENCES = {
  contextIsolation: true,
  sandbox: true,
  nodeIntegration: false,
  nodeIntegrationInWorker: false,
  nodeIntegrationInSubFrames: false,
  webSecurity: true,
  allowRunningInsecureContent: false,
  webviewTag: false,
  experimentalFeatures: false,
} as const satisfies WebPreferences;

export const CONTENT_SECURITY_POLICY = [
  "default-src 'none'",
  "script-src 'self'",
  "style-src 'self'",
  "img-src 'self' data:",
  "font-src 'self'",
  "connect-src 'self'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
  "object-src 'none'",
].join('; ');

/**
 * Dev only: Vite's hot reload injects inline scripts and styles, and the page (on the Vite
 * server) reaches the core through the app's own `app://app/api/…` address. Never used when
 * packaged, where the page itself is on `app://app`.
 */
export const DEV_CONTENT_SECURITY_POLICY = CONTENT_SECURITY_POLICY.replace(
  "script-src 'self'",
  "script-src 'self' 'unsafe-inline'",
)
  .replace("style-src 'self'", "style-src 'self' 'unsafe-inline'")
  .replace("connect-src 'self'", "connect-src 'self' app://app");

/** External sites the app may open in the system browser. Everything else is blocked. */
export const EXTERNAL_ORIGINS: readonly string[] = ['https://github.com'];

export function isAllowedExternal(url: string): boolean {
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'https:' && EXTERNAL_ORIGINS.includes(parsed.origin);
  } catch {
    return false;
  }
}

export function windowOptions(preload: string): BrowserWindowConstructorOptions {
  return {
    width: 1280,
    height: 800,
    show: false,
    title: 'MasterRL',
    webPreferences: { ...WEB_PREFERENCES, preload },
  };
}

/**
 * Where an unpackaged build under test writes the addresses it would open in the browser
 * (`MRL_BROWSER_LOG`, used by tests/e2e/desktop). Always undefined in an installed app.
 */
export function browserLogFile(packaged: boolean, env: NodeJS.ProcessEnv): string | undefined {
  if (packaged) return undefined;
  const file = env.MRL_BROWSER_LOG;
  return file && file.trim() !== '' ? file : undefined;
}
