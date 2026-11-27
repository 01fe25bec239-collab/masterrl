import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { parse, stringify, type TomlTable } from 'smol-toml';

/** What a Solo agent on this computer needs to reach this launch's core. */
export interface AgentLink {
  /** `http://127.0.0.1:<port>` for server-proxied transfers. */
  serverUrl: string;
  /** File holding the launch secret (private to this account). */
  secretFile: string;
  /** `127.0.0.1:<grpc port>` for the agent gateway, when the core runs one. */
  gateway?: string;
}

/** The agent's settings file, found the way `mrl-agent` finds it (P12). */
export function agentConfigPath(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
  home = env.HOME ?? env.USERPROFILE ?? '',
): string {
  const set = (k: string) => (env[k] ? env[k] : undefined);
  const named = set('MRL_AGENT_CONFIG');
  if (named) return named;
  const p = platform === 'win32' ? path.win32 : path.posix;
  if (platform === 'win32') return p.join(set('APPDATA') ?? '', 'MasterRL', 'agent.toml');
  if (platform === 'darwin') {
    return p.join(home, 'Library', 'Application Support', 'MasterRL', 'agent.toml');
  }
  return p.join(set('XDG_CONFIG_HOME') ?? p.join(home, '.config'), 'masterrl', 'agent.toml');
}

const header = (table: string) => new RegExp(`^\\s*\\[\\s*${table}\\s*\\]\\s*(#.*)?$`);

/** Sets `key = value` in `[table]`, keeping every other line (and its comments). */
function setKey(lines: string[], table: string, key: string, value: string): string[] {
  const line = `${key} = ${JSON.stringify(value)}`;
  const start = lines.findIndex((l) => header(table).test(l));
  if (start < 0) {
    const out = [...lines];
    while (out.length > 0 && out.at(-1)?.trim() === '') out.pop();
    return [...out, ...(out.length > 0 ? [''] : []), `[${table}]`, line, ''];
  }
  let end = lines.findIndex((l, i) => i > start && /^\s*\[/.test(l));
  if (end < 0) end = lines.length;
  const keyLine = new RegExp(`^\\s*${key}\\s*=`);
  const at = lines.findIndex((l, i) => i > start && i < end && keyLine.test(l));
  if (at >= 0) return lines.map((l, i) => (i === at ? line : l));
  let insert = end;
  while (insert - 1 > start && lines[insert - 1]?.trim() === '') insert--;
  return [...lines.slice(0, insert), line, ...lines.slice(insert)];
}

const table = (v: unknown): TomlTable =>
  v !== null && typeof v === 'object' && !Array.isArray(v) ? (v as TomlTable) : {};

/** True when the agent dials the core directly (so the gateway address is ours to set). */
function dialsDirectly(doc: TomlTable): boolean {
  const g = table(doc.gateway);
  return g.listen === undefined && (g.transport === undefined || g.transport === 'direct');
}

function matches(doc: TomlTable, link: AgentLink): boolean {
  const t = table(doc.transfer);
  const g = table(doc.gateway);
  return (
    t.server_url === link.serverUrl &&
    t.server_launch_secret_file === link.secretFile &&
    (link.gateway === undefined || !dialsDirectly(doc) || g.server === link.gateway)
  );
}

/**
 * The agent settings `text` with this launch's link: `[transfer] server_url` and
 * `server_launch_secret_file`, and `[gateway] server` when the agent dials directly.
 * Other settings and comments stay. Throws (plain words) when `text` is not valid TOML.
 */
export function withLink(text: string, link: AgentLink): string {
  let doc: TomlTable;
  try {
    doc = parse(text);
  } catch {
    throw new Error('The agent settings file has a mistake, so the app left it unchanged.');
  }
  if (matches(doc, link)) return text;
  const eol = text.includes('\r\n') ? '\r\n' : '\n';
  let lines = text === '' ? [] : text.split(/\r?\n/);
  lines = setKey(lines, 'transfer', 'server_url', link.serverUrl);
  lines = setKey(lines, 'transfer', 'server_launch_secret_file', link.secretFile);
  if (link.gateway !== undefined && dialsDirectly(doc)) {
    lines = setKey(lines, 'gateway', 'server', link.gateway);
  }
  const edited = lines.join(eol);
  try {
    if (matches(parse(edited), link)) return edited;
  } catch {
    // Unusual layouts (dotted keys, inline tables): fall back to rewriting the whole file.
  }
  doc.transfer = {
    ...table(doc.transfer),
    server_url: link.serverUrl,
    server_launch_secret_file: link.secretFile,
  };
  if (link.gateway !== undefined && dialsDirectly(doc)) {
    doc.gateway = { ...table(doc.gateway), server: link.gateway };
  }
  return `${stringify(doc)}\n`;
}

async function writeAtomic(file: string, text: string, mode: number): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true });
  const partial = `${file}.${String(process.pid)}.partial`;
  await writeFile(partial, text, { mode });
  await rename(partial, file);
}

/**
 * Writes the launch secret file (0600) and points the local agent's settings at this launch's
 * core. Returns whether the settings file changed (an agent reads it when it starts).
 */
export async function linkLocalAgent(
  configFile: string,
  secret: string,
  link: AgentLink,
): Promise<boolean> {
  await writeAtomic(link.secretFile, secret, 0o600);
  let text = '';
  try {
    text = await readFile(configFile, 'utf8');
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e;
  }
  const next = withLink(text, link);
  if (next === text) return false;
  await writeAtomic(configFile, next, 0o600);
  return true;
}
