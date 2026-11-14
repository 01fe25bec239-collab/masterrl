import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { colors, textPairs, type Theme } from './colors';
import { contrastRatio } from './contrast';

const css = (name: string) => readFileSync(new URL(name, import.meta.url), 'utf8');

function cssVars(block: string): Record<string, string> {
  const vars: Record<string, string> = {};
  for (const [, name = '', value = ''] of block.matchAll(
    /--mrl-color-([\w-]+):\s*(#[0-9a-f]{6});/gi,
  )) {
    vars[name] = value;
  }
  return vars;
}

describe('contrastRatio', () => {
  it('matches known WCAG values', () => {
    expect(contrastRatio('#000000', '#ffffff')).toBeCloseTo(21, 5);
    expect(contrastRatio('#ffffff', '#ffffff')).toBe(1);
    expect(contrastRatio('#777777', '#ffffff')).toBeCloseTo(4.48, 2);
  });

  it('rejects malformed colors', () => {
    expect(() => contrastRatio('red', '#ffffff')).toThrow(/#1a2b3c/);
  });
});

describe.each(Object.keys(colors) as Theme[])('%s theme', (theme) => {
  it.each(textPairs)('%s on %s meets WCAG AA (4.5:1)', (text, background) => {
    expect(contrastRatio(colors[theme][text], colors[theme][background])).toBeGreaterThanOrEqual(
      4.5,
    );
  });
});

describe('tokens.css', () => {
  const [light = '', dark = ''] = css('./tokens.css').split('@media (prefers-color-scheme: dark)');

  it('defines the same values as the TS constants', () => {
    expect(cssVars(light)).toEqual(colors.light);
    expect(cssVars(dark)).toEqual(colors.dark);
  });

  it('is mapped by the Tailwind preset', () => {
    const preset = css('./tailwind-preset.css');
    for (const token of Object.keys(colors.light)) {
      expect(preset).toContain(`--color-${token}: var(--mrl-color-${token});`);
    }
  });
});
