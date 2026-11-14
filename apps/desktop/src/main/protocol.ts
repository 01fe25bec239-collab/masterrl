import path from 'node:path';

export const APP_URL = 'app://app/index.html';

/** Map an `app://app/...` URL to a file inside `root`; null for anything outside it. */
export function resolveAppFile(root: string, requestUrl: string): string | null {
  try {
    const url = new URL(requestUrl);
    if (url.protocol !== 'app:' || url.host !== 'app') return null;
    const relative = decodeURIComponent(url.pathname);
    const file = path.resolve(root, `.${relative === '/' ? '/index.html' : relative}`);
    return file.startsWith(path.resolve(root) + path.sep) ? file : null;
  } catch {
    return null;
  }
}
