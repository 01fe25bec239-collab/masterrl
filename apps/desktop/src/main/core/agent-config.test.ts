import { mkdtempSync, readFileSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { parse } from 'smol-toml';
import { describe, expect, it } from 'vitest';
import { agentConfigPath, linkLocalAgent, withLink } from './agent-config';

const link = {
  serverUrl: 'http://127.0.0.1:50123',
  secretFile: '/home/ada/.config/MasterRL/core/launch-secret',
  gateway: '127.0.0.1:50124',
};

describe('local agent settings', () => {
  it('finds the file where mrl-agent looks', () => {
    expect(agentConfigPath({ HOME: '/Users/ada' }, 'darwin')).toBe(
      '/Users/ada/Library/Application Support/MasterRL/agent.toml',
    );
    expect(agentConfigPath({ HOME: '/home/ada' }, 'linux')).toBe(
      '/home/ada/.config/masterrl/agent.toml',
    );
    expect(agentConfigPath({ HOME: '/h', XDG_CONFIG_HOME: '/x' }, 'linux')).toBe(
      '/x/masterrl/agent.toml',
    );
    expect(agentConfigPath({ APPDATA: 'C:\\Users\\ada\\AppData\\Roaming' }, 'win32')).toBe(
      'C:\\Users\\ada\\AppData\\Roaming\\MasterRL\\agent.toml',
    );
    expect(agentConfigPath({ MRL_AGENT_CONFIG: '/etc/a.toml' }, 'linux')).toBe('/etc/a.toml');
  });

  it('adds the link to an empty file', () => {
    const doc = parse(withLink('', link));
    expect(doc).toEqual({
      transfer: { server_url: link.serverUrl, server_launch_secret_file: link.secretFile },
      gateway: { server: link.gateway },
    });
  });

  it('keeps other settings and comments, and replaces old values', () => {
    const text = [
      '# my agent',
      'state_dir = "/data/agent" # big disk',
      '',
      '[transfer]',
      'server_url = "http://127.0.0.1:1"',
      'retries = 9',
      '',
      '[pool]',
      'pool = "lab"',
    ].join('\n');
    const out = withLink(text, link);
    expect(out).toContain('# my agent');
    expect(out).toContain('state_dir = "/data/agent" # big disk');
    const doc = parse(out) as Record<string, Record<string, unknown>>;
    expect(doc.transfer).toEqual({
      server_url: link.serverUrl,
      server_launch_secret_file: link.secretFile,
      retries: 9,
    });
    expect(doc.pool).toEqual({ pool: 'lab' });
    expect(withLink(out, link)).toBe(out);
  });

  it('leaves the gateway alone when the agent waits for the core or uses a tunnel', () => {
    for (const g of ['[gateway]\nlisten = "0.0.0.0:7000"', '[gateway]\ntransport = "tailscale"']) {
      const doc = parse(withLink(g, link)) as Record<string, Record<string, unknown>>;
      expect(doc.gateway?.server).toBeUndefined();
      expect(doc.transfer?.server_url).toBe(link.serverUrl);
    }
  });

  it('handles layouts it cannot edit line by line', () => {
    const doc = parse(withLink('transfer = { retries = 2 }\n', link)) as Record<
      string,
      Record<string, unknown>
    >;
    expect(doc.transfer).toEqual({
      retries: 2,
      server_url: link.serverUrl,
      server_launch_secret_file: link.secretFile,
    });
  });

  it('refuses to touch a broken file', () => {
    expect(() => withLink('state_dir = [', link)).toThrow(/mistake/);
  });

  it('writes the secret privately and the settings once', async () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'mrl-agent-'));
    const file = path.join(dir, 'MasterRL', 'agent.toml');
    const secretFile = path.join(dir, 'core', 'launch-secret');
    const l = { ...link, secretFile };
    expect(await linkLocalAgent(file, 'the-secret', l)).toBe(true);
    expect(await linkLocalAgent(file, 'the-secret', l)).toBe(false);
    expect(readFileSync(secretFile, 'utf8')).toBe('the-secret');
    if (process.platform !== 'win32') {
      expect(statSync(secretFile).mode & 0o077).toBe(0);
      expect(statSync(file).mode & 0o077).toBe(0);
    }
  });
});
