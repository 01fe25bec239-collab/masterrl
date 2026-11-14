import { describe, expect, it } from 'vitest';
import { isAllowedExternal } from './security';

describe('isAllowedExternal', () => {
  it('allows listed https origins', () => {
    expect(isAllowedExternal('https://github.com/masterrl')).toBe(true);
  });

  it.each([
    'http://github.com/',
    'https://github.com.evil.example/',
    'https://evil.example/',
    'file:///etc/passwd',
    'javascript:alert(1)',
    'not a url',
  ])('blocks %s', (url) => {
    expect(isAllowedExternal(url)).toBe(false);
  });
});
