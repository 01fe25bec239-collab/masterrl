import { describe, expect, it } from 'vitest';
import { browserLogFile, isAllowedExternal } from './security';

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

describe('browserLogFile', () => {
  it('is never used by an installed app', () => {
    expect(browserLogFile(true, { MRL_BROWSER_LOG: '/tmp/browser.log' })).toBeUndefined();
  });

  it('is used by an unpackaged build only when set', () => {
    expect(browserLogFile(false, { MRL_BROWSER_LOG: '/tmp/browser.log' })).toBe('/tmp/browser.log');
    expect(browserLogFile(false, {})).toBeUndefined();
    expect(browserLogFile(false, { MRL_BROWSER_LOG: ' ' })).toBeUndefined();
  });
});
