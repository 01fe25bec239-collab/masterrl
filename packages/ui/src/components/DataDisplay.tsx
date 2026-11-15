import type { ReactNode } from 'react';
import { cn } from '../lib/cn';
import { formatDateTime } from '../lib/format';
import { Icon, type IconName } from './Icon';

export interface KeyValueItem {
  key: string;
  value: ReactNode;
}

export function KeyValueList({ items, className }: { items: KeyValueItem[]; className?: string }) {
  return (
    <dl className={cn('grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm', className)}>
      {items.map((item) => (
        <div key={item.key} className="contents">
          <dt className="text-fg-muted">{item.key}</dt>
          <dd className="text-fg">{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

export interface TimelineItem {
  id: string;
  at: Date;
  title: string;
  detail?: ReactNode;
  icon?: IconName;
  /** Marks failures; shown with the alert icon and danger text. */
  problem?: boolean;
}

/** Events in time order, oldest first. */
export function Timeline({ items, className }: { items: TimelineItem[]; className?: string }) {
  return (
    <ol className={cn('relative ml-3 border-l border-border', className)}>
      {items.map((item) => (
        <li key={item.id} className="mb-4 ml-5">
          <span className="absolute -left-2.5 flex size-5 items-center justify-center rounded-full border border-border bg-bg">
            <Icon
              name={item.problem ? 'alert' : (item.icon ?? 'circle')}
              className={cn('size-3', item.problem ? 'text-danger' : 'text-fg-muted')}
            />
          </span>
          <p className={cn('font-medium', item.problem ? 'text-danger' : 'text-fg')}>
            {item.title}
          </p>
          <time className="text-xs text-fg-muted" dateTime={item.at.toISOString()}>
            {formatDateTime(item.at)}
          </time>
          {item.detail && <div className="mt-1 text-sm text-fg-muted">{item.detail}</div>}
        </li>
      ))}
    </ol>
  );
}
