import { useId, useState, type ReactNode } from 'react';
import { Button } from '../components/Button';
import { Icon } from '../components/Icon';
import { cn } from '../lib/cn';

export interface ChartTable {
  columns: string[];
  rows: (string | number)[][];
  /** e.g. "Sampled to 200 of 12,400 points." */
  note?: string;
}

export interface ChartFrameProps {
  title: string;
  /** Text summary read by everyone: latest, lowest, highest… */
  summary: string;
  /** Built only when the table is open. */
  table: () => ChartTable;
  toolbar?: ReactNode;
  children: ReactNode;
  className?: string;
}

/** Figure with a title, text summary and a "Show as table" alternative for every chart. */
export function ChartFrame({
  title,
  summary,
  table,
  toolbar,
  children,
  className,
}: ChartFrameProps) {
  const [showTable, setShowTable] = useState(false);
  const tableId = useId();
  const data = showTable ? table() : null;

  return (
    <figure className={cn('rounded-md border border-border bg-bg p-3', className)}>
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="font-semibold text-fg">{title}</h3>
        <div className="ml-auto flex flex-wrap items-center gap-2">
          {toolbar}
          <Button
            size="sm"
            aria-expanded={showTable}
            aria-controls={tableId}
            onClick={() => {
              setShowTable(!showTable);
            }}
          >
            <Icon name={showTable ? 'chart' : 'list'} />
            {showTable ? 'Hide table' : 'Show as table'}
          </Button>
        </div>
      </div>
      <div aria-hidden="true" className="mt-2">
        {children}
      </div>
      <figcaption className="mt-2 text-sm text-fg-muted">{summary}</figcaption>
      <div id={tableId}>
        {data && (
          <div className="mt-3 max-h-72 overflow-auto rounded-md border border-border">
            <table className="w-full border-collapse text-sm">
              <caption className="p-2 text-left text-fg-muted">
                {title} as a table.{data.note ? ` ${data.note}` : ''}
              </caption>
              <thead className="sticky top-0 bg-surface">
                <tr>
                  {data.columns.map((c) => (
                    <th key={c} scope="col" className="px-3 py-1 text-left font-medium text-fg">
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => (
                  <tr key={i} className="border-t border-border">
                    {row.map((cell, j) => (
                      <td key={j} className="px-3 py-1 text-fg">
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </figure>
  );
}
