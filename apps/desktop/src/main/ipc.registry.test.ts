import { describe, expect, it, vi } from 'vitest';
import { createBridge, ipcRegistry, parseInput, type Channel } from './ipc.registry';

describe('ipc registry', () => {
  it('lists every channel as <area>:<action>', () => {
    const channels = Object.keys(ipcRegistry);
    expect(channels).toContain('app:getVersion');
    expect(channels).toContain('desktop:blockSleep');
    for (const c of channels) expect(c).toMatch(/^[a-z]+:[a-zA-Z]+$/);
  });

  it('accepts no arguments for app:getVersion', () => {
    expect(parseInput('app:getVersion', [])).toEqual([]);
  });

  it.each([[['/etc/passwd']], [['rm -rf /']], [[{ path: '/' }]], ['not a list']])(
    'rejects arguments %j',
    (args) => {
      expect(() => parseInput('app:getVersion', args)).toThrow(/does not accept/);
    },
  );

  // Each channel that takes arguments: what it accepts, and hostile input it refuses.
  const cases: [Channel, unknown[], unknown[][]][] = [
    ['app:showFolder', ['crash-reports'], [['/etc'], ['..'], []]],
    ['core:watch', [3], [['3'], [], [{}]]],
    ['auth:signIn', ['github'], [['https://evil.example'], ['GitHub'], []]],
    ['desktop:blockSleep', [true], [['yes'], [1], []]],
    [
      'crash:read',
      ['mrl-server-0000000000001-42'],
      [['../../etc/passwd'], ['mrl-server-1-1/../../x'], ['a'.repeat(200)], [1]],
    ],
    ['crash:delete', ['desktop-1-1'], [['desktop-1-1.json'], ['C:\\x']]],
    ['crash:openIssue', ['mrl-agent-1-2'], [['x y']]],
    ['settings:set', [{ closeToTray: false }], [[{ closeToTray: 'no' }], ['x']]],
  ];
  it.each(cases)('%s checks its arguments', (channel, good, bad) => {
    expect(parseInput(channel, good)).toEqual(good);
    for (const args of bad) expect(() => parseInput(channel, args)).toThrow(/does not accept/);
  });

  it('settings:set drops keys it does not know', () => {
    expect(parseInput('settings:set', [{ closeToTray: true, evil: 1 }])).toEqual([
      { closeToTray: true },
    ]);
  });

  it('generates one bridge call per channel', async () => {
    const invoke = vi.fn().mockResolvedValue('1.2.3');
    const bridge = createBridge(invoke);

    await expect(bridge.app.getVersion()).resolves.toBe('1.2.3');
    expect(invoke).toHaveBeenCalledWith('app:getVersion', []);
    await bridge.desktop.blockSleep(true);
    expect(invoke).toHaveBeenLastCalledWith('desktop:blockSleep', [true]);
    const calls = Object.values(bridge).flatMap((area) => Object.keys(area));
    expect(calls).toHaveLength(Object.keys(ipcRegistry).length);
  });
});
