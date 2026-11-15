import { useStreamStatus } from '../hooks/useEventStream';
import { Icon } from './Icon';
import { PlainError } from './States';

/** "Live updates paused — reconnecting" while the shared stream is down. */
export function LiveStatusBanner() {
  const { status, failure } = useStreamStatus();
  if (status === 'failed') {
    return <PlainError className="mb-4" code={failure?.code} detail="Live updates stopped." />;
  }
  if (status !== 'reconnecting') return null;
  return (
    <p
      role="status"
      className="mb-4 flex items-center gap-2 rounded-md border border-border bg-surface px-3 py-2 text-fg"
    >
      <Icon name="clock" />
      Live updates paused — reconnecting. What you see may be out of date.
    </p>
  );
}
