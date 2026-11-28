import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { describeError } from '@mrl/api-client';
import { AsyncView, Button, formatDateTime } from '@mrl/ui';
import { getSession } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';

/** Query key of the crash report list (`crash:list`). */
export const CRASH_LIST_KEY = ['crash', 'list'] as const;

const programs: Record<string, string> = {
  desktop: 'The app window',
  'mrl-server': 'The core service',
  'mrl-agent': 'The local agent',
};

/** Crash reports: kept on this computer only; view, copy, share or delete them. */
export default function PrivacySettings() {
  const bridge = getSession().desktop;
  const queryClient = useQueryClient();
  const list = useQuery({
    queryKey: CRASH_LIST_KEY,
    queryFn: () => bridge?.crash.list() ?? Promise.resolve([]),
    enabled: bridge !== null,
  });
  const remove = useMutation({
    mutationFn: (id: string) => bridge?.crash.delete(id) ?? Promise.resolve(false),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: CRASH_LIST_KEY }),
  });
  if (!bridge) return <DesktopOnly what="Crash reports" />;
  return (
    <div className="flex flex-col gap-3">
      <p className="max-w-prose">
        When the app, its core or the local agent stops unexpectedly, it writes a crash report on
        this computer. MasterRL never uploads them. Reports have no prompts, data, file paths or
        keys, and only the newest 20 are kept.
      </p>
      <div>
        <Button size="sm" onClick={() => void bridge.app.showFolder('crash-reports')}>
          Show the folder
        </Button>
      </div>
      <AsyncView
        noun="crash reports"
        status={list.status}
        data={list.data}
        error={describeError(list.error)}
        onRetry={() => void list.refetch()}
        isEmpty={(items) => items.length === 0}
        empty={{ icon: 'check', title: 'No crash reports', description: 'Nothing has gone wrong.' }}
      >
        {(items) => (
          <ul aria-label="Crash reports" className="flex flex-col divide-y divide-border">
            {items.map((r) => (
              <li key={r.id} className="flex flex-wrap items-center gap-3 py-2">
                <span className="min-w-0 flex-1">
                  <span className="font-medium">{programs[r.program] ?? r.program}</span>
                  <span className="ml-2 text-fg-muted">{formatDateTime(new Date(r.time))}</span>
                  <span className="block truncate text-sm text-fg-muted">{r.message}</span>
                </span>
                <Link
                  to="/settings/crash/$id"
                  params={{ id: r.id }}
                  className="text-accent underline"
                >
                  View
                </Link>
                <Button
                  size="sm"
                  variant="danger"
                  aria-label={`Delete the report from ${formatDateTime(new Date(r.time))}`}
                  disabled={remove.isPending}
                  onClick={() => {
                    remove.mutate(r.id);
                  }}
                >
                  Delete
                </Button>
              </li>
            ))}
          </ul>
        )}
      </AsyncView>
    </div>
  );
}
