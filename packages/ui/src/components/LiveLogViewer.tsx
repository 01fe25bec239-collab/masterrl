import { useVirtualizer } from '@tanstack/react-virtual';
import {
  useDeferredValue,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { cn } from '../lib/cn';
import type { LogBuffer, LogLine } from '../hooks/log-buffer';
import { Button } from './Button';
import { Icon } from './Icon';

export interface LiveLogViewerProps {
  buffer: LogBuffer;
  label?: string;
  /** Link to the complete log when lines were skipped or dropped. */
  onOpenFullLog?: () => void;
  height?: number;
  className?: string;
}

function highlight(text: string, query: string) {
  if (!query) return text;
  const parts: (string | { mark: string })[] = [];
  const lower = text.toLowerCase();
  const q = query.toLowerCase();
  let from = 0;
  for (let i = lower.indexOf(q); i >= 0; i = lower.indexOf(q, from)) {
    parts.push(text.slice(from, i), { mark: text.slice(i, i + q.length) });
    from = i + q.length;
  }
  parts.push(text.slice(from));
  return parts.map((p, i) =>
    typeof p === 'string' ? (
      p
    ) : (
      <mark key={i} className="rounded-sm bg-accent text-accent-fg">
        {p.mark}
      </mark>
    ),
  );
}

/** Live log tail: ring buffer, search, follow mode and a "lines skipped" notice. */
export function LiveLogViewer({
  buffer,
  label = 'Log',
  onOpenFullLog,
  height = 360,
  className,
}: LiveLogViewerProps) {
  useSyncExternalStore(buffer.subscribe, buffer.getVersion);
  const [query, setQuery] = useState('');
  const [follow, setFollow] = useState(true);
  const deferredQuery = useDeferredValue(query);
  const searchId = useId();
  const scrollRef = useRef<HTMLDivElement>(null);

  const all = buffer.lines(); // cached per buffer version
  const shown = useMemo(() => {
    const q = deferredQuery.trim().toLowerCase();
    return q ? all.filter((l) => l.text.toLowerCase().includes(q)) : all;
  }, [all, deferredQuery]);

  // eslint-disable-next-line react-hooks/incompatible-library -- TanStack Virtual returns fresh functions each render by design
  const virtualizer = useVirtualizer({
    count: shown.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 20,
    overscan: 20,
    initialRect: { width: 800, height },
  });

  useEffect(() => {
    if (follow && shown.length > 0) virtualizer.scrollToIndex(shown.length - 1, { align: 'end' });
  }, [follow, shown.length, virtualizer]);

  const missing = buffer.skipped + buffer.evicted;

  return (
    <section aria-label={label} className={cn('rounded-md border border-border', className)}>
      <div className="flex flex-wrap items-center gap-2 border-b border-border bg-surface p-2">
        <label htmlFor={searchId} className="sr-only">
          Search the log
        </label>
        <div className="flex items-center gap-1 rounded-md border border-border bg-bg px-2">
          <Icon name="search" className="text-fg-muted" />
          <input
            id={searchId}
            type="search"
            placeholder="Search the log"
            className="bg-transparent py-1 text-sm text-fg outline-none"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
            }}
          />
        </div>
        <span className="text-sm text-fg-muted" aria-live="polite">
          {deferredQuery ? `${shown.length} matching lines` : `${all.length} lines`}
        </span>
        <Button
          size="sm"
          className="ml-auto"
          aria-pressed={follow}
          onClick={() => {
            setFollow(!follow);
          }}
        >
          <Icon name={follow ? 'pause' : 'play'} />
          {follow ? 'Stop following' : 'Follow new lines'}
        </Button>
      </div>
      {missing > 0 && (
        <p
          role="status"
          className="flex items-center gap-2 border-b border-border px-3 py-1 text-sm text-fg"
        >
          <Icon name="info" />
          {missing.toLocaleString('en-US')} lines skipped to keep up.
          {onOpenFullLog && (
            <button type="button" className="text-accent underline" onClick={onOpenFullLog}>
              Open full log
            </button>
          )}
        </p>
      )}
      <div
        ref={scrollRef}
        role="log"
        aria-label={`${label} lines`}
        // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- scrollable region must be keyboard-scrollable
        tabIndex={0}
        className="overflow-auto bg-bg font-mono text-xs"
        style={{ height }}
        onWheel={(e) => {
          if (e.deltaY < 0 && follow) setFollow(false);
        }}
      >
        <div className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
          {virtualizer.getVirtualItems().map((item) => {
            const line = shown[item.index] as LogLine;
            return (
              <div
                key={line.seq}
                className={cn(
                  'absolute left-0 top-0 w-full whitespace-pre px-3',
                  line.stream === 'stderr' ? 'text-danger' : 'text-fg',
                )}
                style={{ transform: `translateY(${item.start}px)`, height: item.size }}
              >
                {line.rank !== undefined && (
                  <span className="text-fg-muted">[rank {line.rank}] </span>
                )}
                {highlight(line.text, deferredQuery.trim())}
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}
