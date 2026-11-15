import { createContext, useContext, useEffect, useEffectEvent, useSyncExternalStore } from 'react';
import type { EventStreamHub, StreamEvent, StreamFailure, StreamStatus } from './event-stream';

const EventStreamContext = createContext<EventStreamHub | null>(null);

/** Put one hub per tab near the root: `<EventStreamProvider value={hub}>`. */
export const EventStreamProvider = EventStreamContext.Provider;

export interface EventStreamState {
  status: StreamStatus;
  /** True while reconnecting: show the stale banner and mark data as not up to date. */
  stale: boolean;
  failure: StreamFailure | null;
}

/** Status of the shared stream, for banners. */
export function useStreamStatus(): EventStreamState {
  const hub = useContext(EventStreamContext);
  const status = useSyncExternalStore(
    (cb) => (hub ? hub.onStatus(cb) : () => undefined),
    () => hub?.status ?? 'idle',
  );
  return { status, stale: status === 'reconnecting', failure: hub?.failure ?? null };
}

/**
 * Subscribe to one topic (e.g. `run:run_123:metrics`) on the shared stream.
 * Pass `null` to pause. `onEvent` may change every render.
 */
export function useEventStream(
  topic: string | null,
  onEvent: (event: StreamEvent) => void,
): EventStreamState {
  const hub = useContext(EventStreamContext);
  if (!hub) throw new Error('useEventStream needs an <EventStreamProvider> above it.');
  const handle = useEffectEvent(onEvent);

  useEffect(() => {
    if (!topic) return;
    return hub.subscribe(topic, (e) => {
      handle(e);
    });
  }, [hub, topic]);

  return useStreamStatus();
}
