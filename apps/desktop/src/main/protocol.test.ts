import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { resolveAppFile } from './protocol';

const root = path.resolve('/srv/renderer');

describe('resolveAppFile', () => {
  it('serves files inside the renderer folder', () => {
    expect(resolveAppFile(root, 'app://app/')).toBe(path.join(root, 'index.html'));
    expect(resolveAppFile(root, 'app://app/assets/index.js')).toBe(
      path.join(root, 'assets', 'index.js'),
    );
  });

  it.each([
    'app://app/../secret.txt',
    'app://app/%2e%2e/secret.txt',
    'app://app/..%2fsecret.txt',
    'app://app/..%5csecret.txt',
    'app://app/%E0%A4%A',
    'app://other/index.html',
    'file:///etc/passwd',
    'not a url',
  ])('refuses %s', (url) => {
    const file = resolveAppFile(root, url);
    expect(file === null || file.startsWith(root + path.sep)).toBe(true);
  });

  it('refuses paths that escape the folder', () => {
    expect(resolveAppFile(root, 'app://app/..%2fsecret.txt')).toBeNull();
    expect(resolveAppFile(root, 'app://other/index.html')).toBeNull();
  });
});
