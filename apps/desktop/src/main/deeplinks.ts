import { deepLinks, type DeepLink, type DeepLinkContext } from './deeplinks.registry';

/** The app's URL scheme. */
export const SCHEME = 'masterrl';
const MAX_LENGTH = 4096;

/** `masterrl://auth/callback?flow=…` → `{ path: 'auth/callback', query }`; null if not ours. */
export function parseDeepLink(raw: string): { path: string; query: Record<string, string> } | null {
  if (raw.length > MAX_LENGTH) return null;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol !== `${SCHEME}:` || url.username || url.password || url.port) return null;
  const path = `${url.host}${url.pathname}`.replace(/^\/+|\/+$/g, '').toLowerCase();
  const query: Record<string, string> = {};
  for (const [k, v] of url.searchParams) {
    // A repeated key is ambiguous: refuse the link rather than pick one.
    if (k in query) return null;
    query[k] = v;
  }
  return { path, query };
}

/** The deep link among a program's arguments (Windows and Linux pass it there). */
export function deepLinkIn(argv: readonly string[]): string | undefined {
  return argv.find((a) => a.toLowerCase().startsWith(`${SCHEME}://`));
}

/**
 * Hands `raw` to its registered handler after checking its query. Returns the handled path,
 * or null for links the app doesn't answer (they are ignored).
 */
export function routeDeepLink(
  raw: string,
  ctx: DeepLinkContext,
  registry: readonly DeepLink[] = deepLinks,
): string | null {
  const link = parseDeepLink(raw);
  if (!link) return null;
  const entry = registry.find((e) => e.path === link.path);
  if (!entry) return null;
  const params = entry.params.safeParse(link.query);
  if (!params.success) return null;
  entry.handle(params.data, ctx);
  return entry.path;
}
