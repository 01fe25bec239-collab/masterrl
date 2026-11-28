// The same rules as the core's and agent's panic hook (crates/mrl-agent/src/crash.rs `redact`),
// so a report reads the same whichever program wrote it. Tests share their cases.

const MAX_MESSAGE = 2000;
const SECRET_PREFIXES = [
  'mrl_pat_',
  'mrlj_',
  'hf_',
  'sk-',
  'sk_',
  'ghp_',
  'gho_',
  'github_pat_',
  'xox',
  'akia',
  'eyj',
  'glpat-',
];
const SECRET_WORDS = [
  'bearer',
  'basic',
  'password',
  'passphrase',
  'secret',
  'token',
  'apikey',
  'api_key',
  'authorization',
];

/** `"…"`, `'…'` and `` `…` `` become `"<text>"`; an unclosed quote hides the rest. */
function stripQuotes(text: string): string {
  let out = '';
  for (let i = 0; i < text.length; i++) {
    const c = text.charAt(i);
    const quote = c === '"' || c === '`' || (c === "'" && !/[\p{L}\p{N}]$/u.test(out));
    if (!quote) {
      out += c;
      continue;
    }
    let escaped = false;
    for (i++; i < text.length; i++) {
      const d = text.charAt(i);
      if (escaped) escaped = false;
      else if (d === '\\') escaped = true;
      else if (d === c) break;
    }
    out += '"<text>"';
  }
  return out;
}

const PUNCT = new Set(['(', ')', '[', ']', '{', '}', ',', ';', '<', '>']);

function trimPunct(word: string): [string, string, string] {
  let start = 0;
  while (start < word.length && PUNCT.has(word.charAt(start))) start++;
  let end = word.length;
  while (end > start && (PUNCT.has(word.charAt(end - 1)) || ':.'.includes(word.charAt(end - 1)))) {
    end--;
  }
  return [word.slice(0, start), word.slice(start, end), word.slice(end)];
}

function classify(word: string): string | null {
  if (word === '"<text>"') return null;
  const lower = word.toLowerCase();
  if (lower.includes('://')) return '<url>';
  const eq = word.indexOf('=');
  if (eq >= 0) {
    const key = word.slice(0, eq);
    const k = key.toLowerCase();
    if (SECRET_WORDS.some((w) => k.includes(w)) || k.endsWith('key')) return `${key}=<secret>`;
  }
  if (SECRET_PREFIXES.some((p) => lower.startsWith(p)) && word.length > 6) return '<secret>';
  if (word.includes('/') || word.includes('\\') || word.startsWith('~')) return '<path>';
  if (/^[A-Za-z]:./.test(word)) return '<path>';
  if (word.includes('@') && word.includes('.')) return '<email>';
  if (lower.startsWith('0x') && word.length > 6) return '<address>';
  if (word.length >= 20 && /^[A-Za-z0-9+=_-]+$/.test(word) && /\d/.test(word)) {
    return '<redacted>';
  }
  return null;
}

/**
 * Replaces everything that could be a person's data with a placeholder: quoted text, paths,
 * URLs, e-mail addresses, keys and tokens, long random-looking strings and PEM blocks. Also
 * cuts the text to 2,000 characters.
 */
export function redact(input: string): string {
  const pem = input.indexOf('-----BEGIN');
  const text = stripQuotes(pem >= 0 ? `${input.slice(0, pem)}<secret>` : input);
  let out = '';
  let announce = false;
  for (const piece of text.match(/\S+\s*|\s+/g) ?? []) {
    const word = piece.trimEnd();
    const space = piece.slice(word.length);
    const [lead, core, trail] = trimPunct(word);
    const replaced = core === '' ? null : announce ? '<secret>' : classify(core);
    announce = SECRET_WORDS.includes(core.toLowerCase().replace(/:+$/, ''));
    out += replaced === null ? word : `${lead}${replaced}${trail}`;
    out += space;
  }
  const chars = Array.from(out);
  return chars.length > MAX_MESSAGE ? `${chars.slice(0, MAX_MESSAGE).join('')}…` : out;
}
