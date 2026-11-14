// @vitest-environment node
import { fileURLToPath } from 'node:url';
import { ESLint } from 'eslint';
import { describe, expect, it } from 'vitest';

const eslint = new ESLint({ cwd: fileURLToPath(new URL('../../..', import.meta.url)) });

async function messages(code: string) {
  const [result] = await eslint.lintText(code, { filePath: 'apps/web/src/App.tsx' });
  return (result?.messages ?? []).map((m) => m.ruleId);
}

describe('renderer lint rules', () => {
  it.each([
    "void fetch('/api/runs');",
    'void fetch(`/api/runs/${String(1)}`);',
    "void window.fetch('/api/runs');",
  ])('bans raw API fetch: %s', async (code) => {
    expect(await messages(code)).toContain('no-restricted-syntax');
  });

  it('allows other fetch calls', async () => {
    expect(await messages("void fetch('https://example.com/data.json');")).not.toContain(
      'no-restricted-syntax',
    );
  });
});
