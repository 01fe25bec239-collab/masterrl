import { useMemo } from 'react';
import { cn } from '../lib/cn';
import { diffLines } from '../lib/diff';

export interface DiffViewerProps {
  before: string;
  after: string;
  /** Names for the two sides, e.g. "Version 3" and "Version 4". */
  beforeLabel?: string;
  afterLabel?: string;
  className?: string;
}

/** Unified line diff. Changes are marked with + / − and words, not only color. */
export function DiffViewer({
  before,
  after,
  beforeLabel = 'Before',
  afterLabel = 'After',
  className,
}: DiffViewerProps) {
  const lines = useMemo(() => diffLines(before, after), [before, after]);
  const added = lines.filter((l) => l.kind === 'added').length;
  const removed = lines.filter((l) => l.kind === 'removed').length;

  return (
    <figure className={cn('overflow-hidden rounded-md border border-border', className)}>
      <figcaption className="border-b border-border bg-surface px-3 py-2 text-sm text-fg">
        {beforeLabel} → {afterLabel}:{' '}
        {added + removed === 0
          ? 'no changes'
          : `${added} line${added === 1 ? '' : 's'} added, ${removed} line${removed === 1 ? '' : 's'} removed`}
      </figcaption>
      <table className="w-full border-collapse font-mono text-xs">
        <thead className="sr-only">
          <tr>
            <th scope="col">{beforeLabel} line</th>
            <th scope="col">{afterLabel} line</th>
            <th scope="col">Change</th>
            <th scope="col">Text</th>
          </tr>
        </thead>
        <tbody>
          {lines.map((line, i) => (
            <tr
              key={i}
              className={cn(
                line.kind === 'added' && 'bg-surface text-accent',
                line.kind === 'removed' && 'bg-surface text-danger',
                line.kind === 'same' && 'text-fg',
              )}
            >
              <td className="w-10 select-none px-2 text-right text-fg-muted">
                {line.oldLine ?? ''}
              </td>
              <td className="w-10 select-none px-2 text-right text-fg-muted">
                {line.newLine ?? ''}
              </td>
              <td className="w-6 select-none text-center">
                <span aria-hidden="true">
                  {line.kind === 'added' ? '+' : line.kind === 'removed' ? '−' : ' '}
                </span>
                <span className="sr-only">
                  {line.kind === 'added' ? 'added' : line.kind === 'removed' ? 'removed' : 'same'}
                </span>
              </td>
              <td className="whitespace-pre-wrap break-all px-2">{line.text}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}
