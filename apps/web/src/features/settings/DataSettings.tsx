import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { describeError } from '@mrl/api-client';
import {
  AsyncView,
  Button,
  ConfirmDialog,
  formatBytes,
  formatDateTime,
  KeyValueList,
  PlainError,
} from '@mrl/ui';
import { getSession, mrl, useDesktopState, type Schemas } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';

/** Query key of `GET /api/v1/desktop/backups`. */
export const BACKUPS_KEY = ['desktop', 'backups'] as const;

const reasons: Record<string, string> = {
  daily: 'Daily',
  manual: 'Made by you',
  'before-update': 'Before an app update',
  'before-restore': 'Before a restore',
};

function reasonText(reason: string): string {
  return reasons[reason] ?? (reason.startsWith('pre-') ? 'Before a database change' : reason);
}

function Backups() {
  const bridge = getSession().desktop;
  const queryClient = useQueryClient();
  const [chosen, setChosen] = useState<Schemas['DesktopBackup'] | null>(null);
  const [said, setSaid] = useState('');
  const list = useQuery({
    queryKey: BACKUPS_KEY,
    queryFn: () => mrl().GET('/api/v1/desktop/backups'),
  });
  const create = useMutation({
    mutationFn: () => mrl().POST('/api/v1/desktop/backups'),
    onSuccess: async (made) => {
      setSaid(`Backup made: ${formatDateTime(new Date(made.created_at ?? Date.now()))}.`);
      await queryClient.invalidateQueries({ queryKey: BACKUPS_KEY });
    },
  });
  const restore = useMutation({
    mutationFn: (name: string) =>
      mrl().POST('/api/v1/desktop/backups/{name}/restore', { path: { name } }),
    onSuccess: async (done) => {
      setSaid(
        `Restored. The data from just before is kept as a backup too (${done.safety_backup}). The core restarts now.`,
      );
      // The core reads the restored database when it starts again.
      if (done.restart_required) await bridge?.core.restart();
      await queryClient.invalidateQueries();
    },
  });
  const failure = create.error ?? restore.error;
  const error = failure ? describeError(failure) : null;

  return (
    <AsyncView
      noun="backups"
      status={list.status}
      data={list.data}
      error={describeError(list.error)}
      onRetry={() => void list.refetch()}
      empty={{ title: '', description: '' }}
    >
      {(data) =>
        !data.available ? (
          <p className="text-fg-muted">
            This core uses a database you run yourself, so the app does not back it up.
          </p>
        ) : (
          <div className="flex flex-col gap-3">
            <div className="flex flex-wrap gap-2">
              <Button
                variant="primary"
                disabled={create.isPending || restore.isPending}
                onClick={() => {
                  setSaid('');
                  create.mutate();
                }}
              >
                {create.isPending ? 'Backing up…' : 'Back up now'}
              </Button>
              {bridge && (
                <Button onClick={() => void bridge.app.showFolder('backups')}>
                  Show the backup folder
                </Button>
              )}
            </div>
            <p role="status" className="min-h-6">
              {restore.isPending ? 'Restoring… this can take a minute.' : said}
            </p>
            {error && (
              <PlainError
                headingLevel={3}
                code={error.code}
                detail={error.detail}
                traceId={error.traceId}
              />
            )}
            {data.items.length === 0 ? (
              <p className="text-fg-muted">No backups yet. The app makes one every day.</p>
            ) : (
              <ul aria-label="Backups" className="flex flex-col divide-y divide-border">
                {data.items.map((b) => (
                  <li key={b.name} className="flex flex-wrap items-center gap-3 py-2">
                    <span className="flex-1">
                      {b.created_at ? formatDateTime(new Date(b.created_at)) : b.name}
                      <span className="ml-2 text-fg-muted">
                        {reasonText(b.reason)} · {formatBytes(b.size_bytes)}
                      </span>
                    </span>
                    <Button
                      size="sm"
                      disabled={restore.isPending || create.isPending}
                      onClick={() => {
                        setChosen(b);
                      }}
                    >
                      Restore
                    </Button>
                  </li>
                ))}
              </ul>
            )}
            <ConfirmDialog
              open={chosen !== null}
              title="Restore this backup?"
              description={
                <>
                  Everything the app keeps (projects, runs, settings) goes back to how it was on{' '}
                  {chosen?.created_at ? formatDateTime(new Date(chosen.created_at)) : chosen?.name}.
                  The data from just before is backed up first, so you can undo this. Training on
                  your machines keeps running.
                </>
              }
              actionLabel="Restore"
              onCancel={() => {
                setChosen(null);
              }}
              onConfirm={() => {
                if (chosen) restore.mutate(chosen.name);
                setChosen(null);
                setSaid('');
              }}
            />
          </div>
        )
      }
    </AsyncView>
  );
}

/** Where the app keeps its data, and its database backups. */
export default function DataSettings() {
  const bridge = getSession().desktop;
  const state = useDesktopState();
  if (!bridge) return <DesktopOnly what="Data and backups" />;
  return (
    <div className="flex flex-col gap-4">
      <AsyncView
        noun="the data folder"
        status={state.status}
        data={state.data}
        error={describeError(state.error)}
        onRetry={() => void state.refetch()}
        empty={{ title: '', description: '' }}
      >
        {(data) => (
          <div className="flex flex-col gap-2">
            <KeyValueList
              items={[
                { key: 'Data folder', value: data.data_dir ?? 'Kept by your own database' },
                { key: 'Core version', value: data.version },
              ]}
            />
            <p className="max-w-prose text-sm text-fg-muted">
              The folder is fixed for this install, because its database password is tied to it.
            </p>
            <div>
              <Button size="sm" onClick={() => void bridge.app.showFolder('data')}>
                Show the data folder
              </Button>
            </div>
          </div>
        )}
      </AsyncView>
      <section aria-labelledby="backups-title">
        <h3 id="backups-title" className="mb-2 font-semibold">
          Backups
        </h3>
        <Backups />
      </section>
    </div>
  );
}
