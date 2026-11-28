import { useEffect, useState } from 'react';
import { Button } from '@mrl/ui';
import { getSession, type UpdateStatus } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';
import { useDesktopSettings } from './desktop-settings';

function describe(s: UpdateStatus): string {
  switch (s.phase) {
    case 'idle':
      return 'Not checked yet.';
    case 'checking':
      return 'Looking for updates…';
    case 'up_to_date':
      return 'You have the newest version.';
    case 'available':
      return `Version ${s.version ?? ''} is available.`;
    case 'downloading':
      return `Downloading version ${s.version ?? ''}… ${String(Math.round(s.percent ?? 0))}%`;
    case 'ready':
      return `Version ${s.version ?? ''} is ready to install.`;
    case 'preparing':
      return 'Saving checkpoints of running training and backing up your data before the update…';
    case 'error':
      return s.detail ?? 'The update did not work.';
  }
}

/** App updates from GitHub Releases: check, then install after checkpoints and a backup. */
export default function UpdateSettings() {
  const bridge = getSession().desktop;
  const settings = useDesktopSettings();
  const [status, setStatus] = useState<UpdateStatus | null>(null);
  const [busy, setBusy] = useState(false);

  const phase = status?.phase;
  useEffect(() => {
    if (!bridge) return;
    let live = true;
    const read = () =>
      void bridge.update.status().then((s) => {
        if (live) setStatus(s);
      });
    if (phase === undefined) read();
    // Progress changes while checking, downloading and preparing.
    const timer =
      phase === 'checking' || phase === 'downloading' || phase === 'preparing'
        ? setInterval(read, 1000)
        : undefined;
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, [bridge, phase]);

  if (!bridge) return <DesktopOnly what="Updates" />;
  const run = (call: () => Promise<UpdateStatus>) => {
    setBusy(true);
    void call()
      .then(setStatus)
      .finally(() => {
        setBusy(false);
      });
  };
  const canInstall = status?.phase === 'available' || status?.phase === 'ready';

  return (
    <div className="flex flex-col gap-3">
      <p role="status">{status ? describe(status) : 'Loading…'}</p>
      {status?.phase === 'error' && status.detail && (
        <p className="text-sm text-fg-muted">Your data and training were not changed.</p>
      )}
      <div className="flex flex-wrap gap-2">
        <Button
          disabled={busy}
          onClick={() => {
            run(() => bridge.update.check());
          }}
        >
          Check for updates
        </Button>
        {canInstall && (
          <Button
            variant="primary"
            disabled={busy}
            onClick={() => {
              run(() => bridge.update.install());
            }}
          >
            Install and restart
          </Button>
        )}
      </div>
      {canInstall && (
        <p className="max-w-prose text-sm text-fg-muted">
          Before it restarts, the app saves a checkpoint of training running on this computer and
          backs up its data. Training on other machines keeps running.
        </p>
      )}
      {settings.data && (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={settings.data.checkForUpdates}
            onChange={(e) => {
              settings.save({ checkForUpdates: e.target.checked });
            }}
          />
          Look for updates when the app starts
        </label>
      )}
    </div>
  );
}
