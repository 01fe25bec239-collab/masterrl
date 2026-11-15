import { useVirtualizer } from '@tanstack/react-virtual';
import { useEffect, useRef, type ReactNode } from 'react';
import { cn } from '../lib/cn';
import { Button } from './Button';
import {
  EmptyState,
  LoadingState,
  PlainError,
  type EmptyStateProps,
  type ErrorLike,
} from './States';

export interface Column<Row> {
  id: string;
  header: string;
  cell: (row: Row) => ReactNode;
  /** CSS width, e.g. `12rem`. */
  width?: string;
  align?: 'left' | 'right';
}

export interface DataTableProps<Row> {
  /** Accessible name, e.g. "Runs". */
  label: string;
  columns: Column<Row>[];
  rows: Row[];
  getRowId: (row: Row) => string;
  status: 'pending' | 'error' | 'success';
  error?: ErrorLike | null;
  onRetry?: () => void;
  empty: Omit<EmptyStateProps, 'className'>;
  /** Cursor pagination: true while the server says there is a next page. */
  hasMore?: boolean;
  loadingMore?: boolean;
  onLoadMore?: () => void;
  onRowClick?: (row: Row) => void;
  /** Visible height of the scroll area in px. */
  height?: number;
  rowHeight?: number;
  className?: string;
}

/** Virtualized table with cursor pagination ("Show more" and auto-load near the end). */
export function DataTable<Row>({
  label,
  columns,
  rows,
  getRowId,
  status,
  error,
  onRetry,
  empty,
  hasMore = false,
  loadingMore = false,
  onLoadMore,
  onRowClick,
  height = 480,
  rowHeight = 40,
  className,
}: DataTableProps<Row>) {
  const scrollRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line react-hooks/incompatible-library -- TanStack Virtual returns fresh functions each render by design
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => rowHeight,
    overscan: 8,
    initialRect: { width: 1024, height },
  });
  const items = virtualizer.getVirtualItems();
  const lastIndex = items.at(-1)?.index ?? -1;

  useEffect(() => {
    if (hasMore && !loadingMore && onLoadMore && lastIndex >= rows.length - 5) onLoadMore();
  }, [hasMore, loadingMore, onLoadMore, lastIndex, rows.length]);

  if (status === 'pending') return <LoadingState label={`Loading ${label.toLowerCase()}`} />;
  if (status === 'error') {
    return (
      <PlainError
        code={error?.code}
        detail={error?.detail}
        traceId={error?.traceId}
        onRetry={onRetry}
      />
    );
  }
  if (rows.length === 0) return <EmptyState {...empty} />;

  const top = items[0]?.start ?? 0;
  const bottom = virtualizer.getTotalSize() - (items.at(-1)?.end ?? 0);
  const align = (c: Column<Row>) => (c.align === 'right' ? 'text-right' : 'text-left');

  return (
    <div className={cn('rounded-md border border-border', className)}>
      <div
        ref={scrollRef}
        className="overflow-auto"
        style={{ maxHeight: height }}
        role="region"
        aria-label={`${label} table`}
        // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- scrollable region must be keyboard-scrollable
        tabIndex={0}
      >
        <table className="w-full border-collapse text-sm" aria-rowcount={rows.length + 1}>
          <caption className="sr-only">{label}</caption>
          <thead className="sticky top-0 bg-surface">
            <tr aria-rowindex={1}>
              {columns.map((c) => (
                <th
                  key={c.id}
                  scope="col"
                  className={cn('border-b border-border px-3 py-2 font-medium text-fg', align(c))}
                  style={c.width ? { width: c.width } : undefined}
                >
                  {c.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {top > 0 && (
              <tr aria-hidden="true">
                <td colSpan={columns.length} style={{ height: top }} />
              </tr>
            )}
            {items.map((item) => {
              const row = rows[item.index] as Row;
              return (
                <tr
                  key={getRowId(row)}
                  aria-rowindex={item.index + 2}
                  style={{ height: rowHeight }}
                  className={cn(
                    'border-b border-border last:border-b-0',
                    onRowClick && 'cursor-pointer hover:bg-surface',
                  )}
                  onClick={
                    onRowClick
                      ? () => {
                          onRowClick(row);
                        }
                      : undefined
                  }
                >
                  {columns.map((c, ci) => (
                    <td key={c.id} className={cn('px-3 py-2 text-fg', align(c))}>
                      {ci === 0 && onRowClick ? (
                        <button
                          type="button"
                          className="text-left text-accent underline-offset-2 hover:underline"
                          onClick={(e) => {
                            e.stopPropagation();
                            onRowClick(row);
                          }}
                        >
                          {c.cell(row)}
                        </button>
                      ) : (
                        c.cell(row)
                      )}
                    </td>
                  ))}
                </tr>
              );
            })}
            {bottom > 0 && (
              <tr aria-hidden="true">
                <td colSpan={columns.length} style={{ height: bottom }} />
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between border-t border-border px-3 py-2 text-sm text-fg-muted">
        <span>
          Showing {rows.length} {rows.length === 1 ? 'row' : 'rows'}
          {hasMore ? ' — more available' : ''}
        </span>
        {hasMore && onLoadMore && (
          <Button size="sm" disabled={loadingMore} onClick={onLoadMore}>
            {loadingMore ? 'Loading more…' : 'Show more'}
          </Button>
        )}
      </div>
    </div>
  );
}
