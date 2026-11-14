import { describe, expect, it, vi } from 'vitest';
import { createBridge, ipcRegistry, parseInput } from './ipc.registry';

describe('ipc registry', () => {
  it('exposes only app:getVersion', () => {
    expect(Object.keys(ipcRegistry)).toEqual(['app:getVersion']);
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

  it('generates one bridge call per channel', async () => {
    const invoke = vi.fn().mockResolvedValue('1.2.3');
    const bridge = createBridge(invoke);

    await expect(bridge.app.getVersion()).resolves.toBe('1.2.3');
    expect(invoke).toHaveBeenCalledWith('app:getVersion', []);
    expect(Object.keys(bridge)).toEqual(['app']);
    expect(Object.keys(bridge.app)).toEqual(['getVersion']);
  });
});
