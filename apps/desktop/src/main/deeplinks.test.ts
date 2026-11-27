import { describe, expect, it, vi } from 'vitest';
import { deepLinkIn, parseDeepLink, routeDeepLink } from './deeplinks';
import { deepLinks } from './deeplinks.registry';

const ctx = () => ({ completeSignIn: vi.fn(() => true), navigate: vi.fn() });

describe('deep links', () => {
  it('registers auth/callback and open', () => {
    expect(deepLinks.map((d) => d.path)).toEqual(['auth/callback', 'open']);
  });

  it('finishes a sign-in from the browser', () => {
    const c = ctx();
    expect(routeDeepLink('masterrl://auth/callback?flow=f_1&code=abc.DEF-9', c)).toBe(
      'auth/callback',
    );
    expect(c.completeSignIn).toHaveBeenCalledWith('f_1', 'abc.DEF-9');
  });

  it('opens a screen', () => {
    const c = ctx();
    routeDeepLink('masterrl://open?screen=/settings', c);
    expect(c.navigate).toHaveBeenCalledWith('/settings');
  });

  it.each([
    'https://auth/callback?flow=a&code=b',
    'masterrl://auth/callback?flow=a',
    'masterrl://auth/callback?flow=a&code=b&code=c',
    'masterrl://auth/callback?flow=a b&code=c',
    'masterrl://user:pw@auth/callback?flow=a&code=b',
    'masterrl://open?screen=/../../etc',
    'masterrl://open?screen=https://evil.example',
    'masterrl://open?screen=//evil',
    'masterrl://unknown/thing',
    `masterrl://open?screen=/${'a'.repeat(5000)}`,
    'not a url',
  ])('ignores %s', (raw) => {
    const c = ctx();
    expect(routeDeepLink(raw, c)).toBeNull();
    expect(c.completeSignIn).not.toHaveBeenCalled();
    expect(c.navigate).not.toHaveBeenCalled();
  });

  it('finds the link in program arguments', () => {
    expect(deepLinkIn(['/app/MasterRL', '--flag', 'MasterRL://open?screen=/'])).toBe(
      'MasterRL://open?screen=/',
    );
    expect(parseDeepLink('MASTERRL://Auth/Callback/')?.path).toBe('auth/callback');
  });
});
