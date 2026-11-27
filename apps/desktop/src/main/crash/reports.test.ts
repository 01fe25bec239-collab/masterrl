import { mkdtempSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { CrashReports, framesOf, ISSUES_URL, KEEP } from './reports';
import { redact } from './redact';

// The same cases as crates/mrl-agent/src/crash.rs, so both languages redact alike.
const CASES: [string, string][] = [
  [
    'called `Result::unwrap()` on an `Err` value: Io("prompt: tell me a secret")',
    'called "<text>" on an "<text>" value: Io("<text>")',
  ],
  ['could not open /Users/ada/data/train.jsonl', 'could not open <path>'],
  ['failed C:\\Users\\ada\\models\\x.bin', 'failed <path>'],
  ['read ~/secret.txt now', 'read <path> now'],
  ['token mrl_pat_abcdefghijklmnop leaked', 'token <secret> leaked'],
  ['Authorization: Bearer abc.def.ghi', 'Authorization: <secret> <secret>'],
  ['got hf_abcdefABCDEF123 and sk-proj-1234', 'got <secret> and <secret>'],
  ['mail ada@example.com', 'mail <email>'],
  ['GET https://hub.example/x?token=1 failed', 'GET <url> failed'],
  ['password=hunter2 api_key=xyz', 'password=<secret> api_key=<secret>'],
  ['id 7f3a9c2b1d4e5f60718293a4b5c6d7e8 bad', 'id <redacted> bad'],
  ['key -----BEGIN PRIVATE KEY-----\nMIIE', 'key <secret>'],
  ['at 0x7ff8a1b2c3d4', 'at <address>'],
  [
    'index out of bounds: the len is 3 but the index is 7',
    'index out of bounds: the len is 3 but the index is 7',
  ],
];

const report = (over: Record<string, unknown> = {}) =>
  JSON.stringify({
    schema: 'mrl-crash/v1',
    program: 'mrl-server',
    version: '0.1.0',
    os: 'macos',
    arch: 'aarch64',
    time: '2026-11-27T10:00:00Z',
    message: 'boom',
    frames: ['mrl_server::run'],
    ...over,
  });

describe('crash report redaction', () => {
  it.each(CASES)('%s', (input, want) => {
    expect(redact(input)).toBe(want);
  });

  it('keeps function names of a JavaScript stack, never files', () => {
    const stack = 'Error: x\n    at startCore (/Users/ada/app/main.js:10:3)\n    at /x/y.js:1:1';
    expect(framesOf(stack)).toEqual(['startCore']);
  });
});

describe('crash reports', () => {
  const setup = () => {
    const own = mkdtempSync(path.join(tmpdir(), 'mrl-crash-'));
    const agent = mkdtempSync(path.join(tmpdir(), 'mrl-crash-agent-'));
    return { own, agent, reports: new CrashReports([own, agent]) };
  };

  it('lists reports of every program newest first, skipping other files', async () => {
    const { own, agent, reports } = setup();
    writeFileSync(path.join(own, 'mrl-server-0000000000001-1.json'), report());
    writeFileSync(
      path.join(agent, 'mrl-agent-0000000000002-2.json'),
      report({ program: 'mrl-agent', time: '2026-11-27T11:00:00Z' }),
    );
    writeFileSync(path.join(own, 'notes.json'), '{}');
    writeFileSync(path.join(own, 'mrl-server-0000000000003-3.json'), '{"schema":"other"}');
    const list = await reports.list();
    expect(list.map((r) => r.id)).toEqual([
      'mrl-agent-0000000000002-2',
      'mrl-server-0000000000001-1',
    ]);
  });

  it('redacts a report again when reading it, whoever wrote it', async () => {
    const { own, reports } = setup();
    writeFileSync(
      path.join(own, 'mrl-server-0000000000001-1.json'),
      report({ message: 'lost /home/ada/x with hf_abcdefghij123', frames: ['at /home/ada'] }),
    );
    const text = await reports.read('mrl-server-0000000000001-1');
    expect(text).not.toMatch(/ada|hf_abc/);
    expect(text).toContain('<path>');
  });

  it.each(['../../etc/passwd', 'mrl-server-1-1/../x', 'MRL-1-1'])(
    'refuses ids that are not report names (%s)',
    async (id) => {
      const { reports } = setup();
      await expect(reports.read(id)).rejects.toThrow(/gone/);
      expect(await reports.delete(id)).toBe(false);
    },
  );

  it('writes its own reports privately, keeps the newest and deletes on request', async () => {
    const { own, reports } = setup();
    let last = '';
    for (let i = 0; i < KEEP + 2; i++) {
      last = await reports.write('desktop', { version: '1.0.0', message: `n ${String(i)}` });
      await new Promise((r) => setTimeout(r, 2));
    }
    const files = readdirSync(own).filter((n) => n.startsWith('desktop-'));
    expect(files).toHaveLength(KEEP);
    if (process.platform !== 'win32') {
      expect(statSync(path.join(own, `${last}.json`)).mode & 0o077).toBe(0);
    }
    expect(await reports.delete(last)).toBe(true);
    expect((await reports.list()).some((r) => r.id === last)).toBe(false);
  });

  it('opens a pre-filled GitHub issue that fits in an address', async () => {
    const { own, reports } = setup();
    writeFileSync(
      path.join(own, 'mrl-server-0000000000001-1.json'),
      report({
        frames: Array.from({ length: 64 }, (_, i) => `frame_${'x'.repeat(200)}_${String(i)}`),
      }),
    );
    const url = new URL(await reports.issueUrl('mrl-server-0000000000001-1'));
    expect(`${url.origin}${url.pathname}`).toBe(ISSUES_URL);
    expect(url.searchParams.get('title')).toBe('Crash: mrl-server 0.1.0: boom');
    expect(url.searchParams.get('body')).toContain('"schema": "mrl-crash/v1"');
    expect(url.toString().length).toBeLessThanOrEqual(7500);
  });
});
