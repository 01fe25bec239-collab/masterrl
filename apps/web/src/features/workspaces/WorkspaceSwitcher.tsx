import { useQueries } from '@tanstack/react-query';
import { useState } from 'react';
import { Button, Dialog, Icon } from '@mrl/ui';
import {
  workspaceSources,
  type WorkspaceEntry,
  type WorkspaceSource,
} from './workspace-sources.registry';

/** Top-bar switcher between the Solo workspace and the teams in your setup. */
export function WorkspaceSwitcher({
  sources = workspaceSources,
}: {
  sources?: readonly WorkspaceSource[];
}) {
  const [open, setOpen] = useState(false);
  const [picked, setPicked] = useState<WorkspaceEntry | null>(null);
  const results = useQueries({
    queries: sources.map((s) => ({
      queryKey: ['workspaces', s.id],
      queryFn: s.list,
      retry: false,
      staleTime: 60_000,
    })),
  });
  const groups = sources.map((s, i) => ({ source: s, result: results[i] }));
  const current = groups.flatMap((g) => g.result?.data ?? []).find((e) => e.current);

  return (
    <>
      <Button
        size="sm"
        aria-haspopup="dialog"
        onClick={() => {
          setPicked(null);
          setOpen(true);
        }}
      >
        <Icon name="grid" />
        {current?.name ?? 'Workspaces'}
      </Button>
      <Dialog
        open={open}
        onClose={() => {
          setOpen(false);
        }}
        title="Switch workspace"
      >
        {picked ? (
          <div className="mt-3 flex flex-col gap-3">
            <p>
              <span className="font-semibold">{picked.name}</span> runs on its team’s core, not on
              this computer. Open it in your browser at:
            </p>
            <code className="rounded bg-surface p-2 text-sm break-all select-all">
              {picked.address}
            </code>
            <div className="flex gap-2">
              <Button
                size="sm"
                onClick={() => void navigator.clipboard.writeText(picked.address ?? '')}
              >
                <Icon name="copy" />
                Copy the address
              </Button>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => {
                  setPicked(null);
                }}
              >
                Back
              </Button>
            </div>
          </div>
        ) : (
          <div className="mt-3 flex flex-col gap-4">
            {groups.map(({ source, result }) => {
              const entries = result?.data ?? [];
              if (result?.isPending) {
                return (
                  <p key={source.id} className="text-fg-muted">
                    Loading {source.label.toLowerCase()}…
                  </p>
                );
              }
              if (result?.isError || entries.length === 0) return null;
              return (
                <section key={source.id} aria-labelledby={`ws-${source.id}`}>
                  <h3 id={`ws-${source.id}`} className="mb-1 text-sm font-semibold text-fg-muted">
                    {source.label}
                  </h3>
                  <ul className="flex flex-col gap-1">
                    {entries.map((e) => (
                      <li key={e.id}>
                        <button
                          type="button"
                          aria-current={e.current ? 'true' : undefined}
                          className="flex w-full items-center gap-2 rounded-md p-2 text-left hover:bg-surface focus-visible:outline-2 focus-visible:outline-accent"
                          onClick={() => {
                            if (e.current) setOpen(false);
                            else setPicked(e);
                          }}
                        >
                          <Icon name={e.current ? 'check' : 'circle'} />
                          <span>
                            <span className="block">{e.name}</span>
                            <span className="block text-sm text-fg-muted">{e.detail}</span>
                          </span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </section>
              );
            })}
            <div>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => {
                  setOpen(false);
                }}
              >
                Close
              </Button>
            </div>
          </div>
        )}
      </Dialog>
    </>
  );
}
