import { useQueryClient } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import { Icon, PlainError } from '@mrl/ui';
import { codeInReason, useCoreStatus } from './core-status';
import { getSession } from './session';

/** The guidance code for "the app is reconnecting to its core" (p30.yaml). */
export const CORE_UNAVAILABLE = 'MRL-ENV-302';

function useSecondsUntil(at: number | undefined): number | undefined {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (at === undefined) return;
    const timer = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => {
      clearInterval(timer);
    };
  }, [at]);
  return at === undefined ? undefined : Math.max(0, Math.ceil((at - now) / 1000));
}

const statusLine =
  'mb-4 flex flex-wrap items-center gap-2 rounded-md border border-border bg-surface px-3 py-2 text-fg';

/**
 * "Reconnecting to MasterRL core…" while the desktop app restarts its core, with Try again
 * after it gave up. When the core is back, every query is fetched again.
 */
export function CoreStatusBanner() {
  const status = useCoreStatus();
  const queryClient = useQueryClient();
  const previous = useRef<string | null>(null);
  const [busy, setBusy] = useState(false);
  const seconds = useSecondsUntil(status?.phase === 'reconnecting' ? status.retryAt : undefined);
  const phase = status?.phase ?? null;

  useEffect(() => {
    if (phase === 'ready' && previous.current !== null && previous.current !== 'ready') {
      void queryClient.invalidateQueries();
    }
    previous.current = phase;
  }, [phase, queryClient]);

  if (!status) return null;
  const restart = () => {
    const bridge = getSession().desktop;
    if (!bridge) return;
    setBusy(true);
    void bridge.core.restart().finally(() => {
      setBusy(false);
    });
  };

  if (status.phase === 'starting' && status.restarts === 0) {
    return (
      <p role="status" className={statusLine}>
        <Icon name="clock" />
        Starting MasterRL core…
      </p>
    );
  }
  if (status.phase === 'reconnecting' || status.phase === 'starting') {
    return (
      <p role="status" className={statusLine}>
        <Icon name="clock" />
        Reconnecting to MasterRL core…
        <span className="text-fg-muted">
          {seconds !== undefined && seconds > 0
            ? `Next try in ${String(seconds)} s. `
            : 'Trying now. '}
          Training on your machines keeps running.
        </span>
      </p>
    );
  }
  if (status.phase === 'failed') {
    return (
      <div className="mb-4">
        <PlainError
          code={codeInReason(status.reason) ?? CORE_UNAVAILABLE}
          detail={status.reason}
          onRetry={busy ? undefined : restart}
        />
      </div>
    );
  }
  return null;
}
