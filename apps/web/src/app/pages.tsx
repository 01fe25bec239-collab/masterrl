import { Suspense, lazy, useMemo } from 'react';
import { Link } from '@tanstack/react-router';
import { describeError } from '@mrl/api-client';
import {
  AsyncView,
  EmptyState,
  KeyValueList,
  LoadingState,
  PageHeader,
  PlainError,
  SegmentedControl,
  type Level,
  type ThemeChoice,
} from '@mrl/ui';
import type { NavEntry } from './nav.registry';
import { setPrefs, usePrefs } from './prefs';
import { useCurrentScope } from './scopes';
import { settingsSections } from './settings-sections.registry';

export const themeOptions = [
  { value: 'system', label: 'System' },
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
] as const satisfies readonly { value: ThemeChoice; label: string }[];

export const modeOptions = [
  { value: 'beginner', label: 'Beginner' },
  { value: 'advanced', label: 'Advanced' },
] as const satisfies readonly { value: Level; label: string }[];

export function visibleFor<T extends { level?: Level }>(items: readonly T[], mode: Level): T[] {
  return items.filter((i) => mode === 'advanced' || i.level !== 'advanced');
}

/** Home dashboard: where you are, then one card per registered feature. */
export function HomePage({ nav }: { nav: readonly NavEntry[] }) {
  const { mode } = usePrefs();
  const { query, current } = useCurrentScope();
  const cards = useMemo(
    () =>
      visibleFor(nav, mode)
        .filter(
          (e): e is NavEntry & Required<Pick<NavEntry, 'homeCard'>> => e.homeCard !== undefined,
        )
        .map((e) => ({ id: e.id, label: e.label, Card: lazy(e.homeCard) })),
    [nav, mode],
  );

  return (
    <>
      <PageHeader
        title="Welcome to MasterRL"
        description="Train and test language models on your own computer."
      />
      <section aria-labelledby="where" className="mb-6">
        <h2 id="where" className="mb-2 text-lg font-semibold">
          Where you are working
        </h2>
        <AsyncView
          noun="your projects"
          status={query.status}
          data={query.data}
          error={describeError(query.error)}
          onRetry={() => void query.refetch()}
          isEmpty={(tree) => tree.orgs.length === 0}
          empty={{
            title: 'No projects yet',
            description: 'Ask an admin to add you to a workspace, or create your first project.',
          }}
        >
          {() => (
            <KeyValueList
              items={[
                { key: 'Organization', value: current.org?.name ?? 'None' },
                { key: 'Workspace', value: current.workspace?.name ?? 'None' },
                { key: 'Project', value: current.project?.name ?? 'None yet' },
              ]}
            />
          )}
        </AsyncView>
      </section>
      <section aria-labelledby="overview">
        <h2 id="overview" className="mb-2 text-lg font-semibold">
          Overview
        </h2>
        {cards.length === 0 ? (
          <EmptyState
            icon="grid"
            title="Nothing to show yet"
            description="Summaries of your runs, data and machines appear here as those parts of the app are added."
          />
        ) : (
          <div className="grid gap-4 md:grid-cols-2">
            {cards.map(({ id, label, Card }) => (
              <Suspense key={id} fallback={<LoadingState label={`Loading ${label}`} />}>
                <Card />
              </Suspense>
            ))}
          </div>
        )}
      </section>
    </>
  );
}

export function SettingsPage() {
  const { theme, mode } = usePrefs();
  const sections = useMemo(
    () => visibleFor(settingsSections, mode).map((s) => ({ ...s, Body: lazy(s.load) })),
    [mode],
  );
  return (
    <>
      <PageHeader title="Settings" description="Choose how the app looks and how much it shows." />
      <section aria-labelledby="appearance" className="mb-6 flex flex-col gap-4">
        <h2 id="appearance" className="text-lg font-semibold">
          Appearance
        </h2>
        <div className="flex flex-wrap items-center gap-3">
          <span className="w-28 text-fg-muted">Theme</span>
          <SegmentedControl
            label="Theme"
            options={themeOptions}
            value={theme}
            onChange={(v) => {
              setPrefs({ theme: v });
            }}
          />
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <span className="w-28 text-fg-muted">Detail level</span>
          <SegmentedControl
            label="Detail level"
            options={modeOptions}
            value={mode}
            onChange={(v) => {
              setPrefs({ mode: v });
            }}
          />
          <span className="text-sm text-fg-muted">
            Beginner hides expert settings. Your values are kept when you switch.
          </span>
        </div>
      </section>
      {sections.map(({ id, title, description, Body }) => (
        <section key={id} id={id} aria-labelledby={`${id}-title`} className="mb-6">
          <h2 id={`${id}-title`} className="text-lg font-semibold">
            {title}
          </h2>
          <p className="mb-2 text-fg-muted">{description}</p>
          <Suspense fallback={<LoadingState label={`Loading ${title}`} />}>
            <Body />
          </Suspense>
        </section>
      ))}
    </>
  );
}

export function NotFoundPage() {
  return (
    <>
      <PageHeader title="Page not found" />
      <EmptyState
        icon="compass"
        title="We couldn’t find that page"
        description="The link may be old, or the page was moved. Use the menu on the left, or go back to the home page."
        action={
          <Link to="/" className="text-accent underline">
            Go to the home page
          </Link>
        }
      />
    </>
  );
}

export function ForbiddenPage() {
  return (
    <>
      <PageHeader title="No access" />
      <EmptyState
        icon="lock"
        title="You don’t have access to this page"
        description="Ask an admin of this workspace to give you a role that includes it."
        action={
          <Link to="/" className="text-accent underline">
            Go to the home page
          </Link>
        }
      />
    </>
  );
}

/** Route-level error: 403 and 404 get their own pages; anything else gets the plain explanation. */
export function RouteErrorPage({ error, reset }: { error: unknown; reset: () => void }) {
  const status = (error as { status?: unknown } | null)?.status;
  if (status === 403) return <ForbiddenPage />;
  if (status === 404) return <NotFoundPage />;
  const e = describeError(error);
  return <PlainError code={e.code} detail={e.detail} traceId={e.traceId} onRetry={reset} />;
}
