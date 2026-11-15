import { useId, type ReactNode } from 'react';
import { cn } from '../lib/cn';
import { currentPlatform, useGuidance } from '../hooks/useGuidance';
import { Button } from './Button';
import { Icon, type IconName } from './Icon';

export interface PlainErrorProps {
  /** Catalog code, e.g. `MRL-CAP-012`. Unknown or missing codes show the generic explanation. */
  code?: string;
  /** Extra detail from the server, shown under the explanation. */
  detail?: string;
  /** Support reference; shown so people can quote it. */
  traceId?: string;
  onRetry?: () => void;
  className?: string;
  /** Heading level for the title inside a page. */
  headingLevel?: 2 | 3;
}

/** What happened, why, and what to do — from the guidance catalog. Used by every error state. */
export function PlainError({
  code,
  detail,
  traceId,
  onRetry,
  className,
  headingLevel = 2,
}: PlainErrorProps) {
  const guidance = useGuidance(code);
  const titleId = useId();
  const Heading = headingLevel === 2 ? 'h2' : 'h3';
  const platform = currentPlatform();
  const steps = [
    ...guidance.what_to_do.steps,
    ...((platform && guidance.what_to_do[platform]) ?? []),
  ];

  return (
    <section
      role="alert"
      aria-labelledby={titleId}
      className={cn('rounded-md border border-danger bg-bg p-4 text-fg', className)}
    >
      <Heading id={titleId} className="flex items-center gap-2 font-semibold text-danger">
        <Icon name="alert" />
        {guidance.title}
      </Heading>
      <p className="mt-2">
        <span className="font-medium">Why: </span>
        {guidance.why}
      </p>
      {detail && <p className="mt-1 text-fg-muted">{detail}</p>}
      {steps.length > 0 && (
        <div className="mt-2">
          <p className="font-medium">What to do:</p>
          <ol className="ml-5 list-decimal">
            {steps.map((step) => (
              <li key={step}>{step}</li>
            ))}
          </ol>
        </div>
      )}
      <div className="mt-3 flex flex-wrap items-center gap-3 text-sm text-fg-muted">
        {code && (
          <span>
            Error code:{' '}
            {guidance.docs_link ? (
              <a className="text-accent underline" href={guidance.docs_link}>
                {code}
              </a>
            ) : (
              <code>{code}</code>
            )}
          </span>
        )}
        {traceId && (
          <span>
            Reference: <code>{traceId}</code>
          </span>
        )}
        {onRetry && (
          <Button size="sm" onClick={onRetry}>
            Try again
          </Button>
        )}
      </div>
    </section>
  );
}

export interface EmptyStateProps {
  title: string;
  /** Say what to do next, e.g. "Upload a JSONL file or import one from Hugging Face." */
  description: string;
  icon?: IconName;
  action?: ReactNode;
  className?: string;
}

export function EmptyState({
  title,
  description,
  icon = 'info',
  action,
  className,
}: EmptyStateProps) {
  return (
    <div
      className={cn(
        'flex flex-col items-center gap-2 rounded-md border border-dashed border-border p-8 text-center',
        className,
      )}
    >
      <Icon name={icon} className="size-6 text-fg-muted" />
      <p className="font-semibold text-fg">{title}</p>
      <p className="max-w-prose text-fg-muted">{description}</p>
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

export interface LoadingStateProps {
  /** Read out to screen readers, e.g. "Loading runs". */
  label: string;
  /** Number of skeleton lines. */
  lines?: number;
  className?: string;
}

/** Skeleton lines, never a bare spinner. */
export function LoadingState({ label, lines = 3, className }: LoadingStateProps) {
  return (
    <div role="status" aria-live="polite" className={cn('flex flex-col gap-2', className)}>
      <span className="sr-only">{label}</span>
      {Array.from({ length: lines }, (_, i) => (
        <div
          key={i}
          aria-hidden="true"
          className={cn(
            'h-4 animate-pulse rounded bg-surface motion-reduce:animate-none',
            i === lines - 1 ? 'w-2/3' : 'w-full',
          )}
        />
      ))}
    </div>
  );
}

export interface ErrorLike {
  code?: string;
  detail?: string;
  traceId?: string;
}

export interface AsyncViewProps<T> {
  /** What is loading, e.g. "runs" → "Loading runs". */
  noun: string;
  status: 'pending' | 'error' | 'success';
  data: T | undefined;
  error?: ErrorLike | null;
  isEmpty?: (data: T) => boolean;
  empty: Omit<EmptyStateProps, 'className'>;
  onRetry?: () => void;
  children: (data: T) => ReactNode;
}

/** One place that guarantees loading, empty and error states for an async view. */
export function AsyncView<T>({
  noun,
  status,
  data,
  error,
  isEmpty,
  empty,
  onRetry,
  children,
}: AsyncViewProps<T>) {
  if (status === 'pending') return <LoadingState label={`Loading ${noun}`} />;
  if (status === 'error' || data === undefined) {
    return (
      <PlainError
        code={error?.code}
        detail={error?.detail}
        traceId={error?.traceId}
        onRetry={onRetry}
      />
    );
  }
  if (isEmpty?.(data)) return <EmptyState {...empty} />;
  return <>{children(data)}</>;
}
