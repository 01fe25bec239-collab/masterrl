import { Link, Outlet, useNavigate, useRouteContext, useRouterState } from '@tanstack/react-router';
import { useMemo, useState } from 'react';
import { describeError } from '@mrl/api-client';
import {
  AppShell,
  Button,
  CommandPalette,
  Icon,
  LeftNav,
  LiveStatusBanner,
  PlainError,
  resolvedTheme,
  ScopeSwitcher,
  SegmentedControl,
  type Command,
  type LinkProps,
  type NavSection,
} from '@mrl/ui';
import { WorkspaceSwitcher } from '../features/workspaces/WorkspaceSwitcher';
import { modeOptions, visibleFor } from './pages';
import { setPrefs, usePrefs } from './prefs';
import { useCurrentScope } from './scopes';
import { CoreStatusBanner, FirstRunRedirect, getSession } from './session';

function RouterLink({ href, children, ...rest }: LinkProps) {
  return (
    <Link to={href} {...rest}>
      {children}
    </Link>
  );
}

/** Root layout: top bar (scope, search, theme, mode), left nav from the registry, page outlet. */
export function Shell() {
  const { nav } = useRouteContext({ from: '__root__' });
  const { mode, theme } = usePrefs();
  const path = useRouterState({ select: (s) => s.location.pathname });
  const navigate = useNavigate();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const { query, current, choose } = useCurrentScope();
  const shown = resolvedTheme(theme);

  const sections = useMemo<NavSection[]>(() => {
    const entries = visibleFor(nav, mode);
    const items = (section: 'project' | 'workspace') =>
      entries
        .filter((e) => e.section === section)
        .map((e) => ({ id: e.id, label: e.label, href: e.path, icon: e.icon }));
    return [
      {
        id: 'project',
        items: [{ id: 'home', label: 'Overview', href: '/', icon: 'home' }, ...items('project')],
      },
      {
        id: 'workspace',
        label: 'Workspace',
        items: [
          ...items('workspace'),
          { id: 'settings', label: 'Settings', href: '/settings', icon: 'settings' },
        ],
      },
    ];
  }, [nav, mode]);

  const commands = useMemo<Command[]>(
    () => [
      ...sections.flatMap((s) =>
        s.items.map((item) => ({
          id: `go-${item.id}`,
          label: item.label,
          group: 'Go to',
          icon: item.icon,
          keywords: nav.find((e) => e.id === item.id)?.keywords,
          run: () => void navigate({ to: item.href }),
        })),
      ),
      {
        id: 'theme',
        label: shown === 'dark' ? 'Use light theme' : 'Use dark theme',
        group: 'Appearance',
        icon: shown === 'dark' ? 'sun' : 'moon',
        keywords: ['theme', 'dark', 'light'],
        run: () => {
          setPrefs({ theme: shown === 'dark' ? 'light' : 'dark' });
        },
      },
      {
        id: 'mode',
        label: mode === 'beginner' ? 'Switch to Advanced mode' : 'Switch to Beginner mode',
        group: 'Appearance',
        icon: 'settings',
        keywords: ['mode', 'beginner', 'advanced', 'expert'],
        run: () => {
          setPrefs({ mode: mode === 'beginner' ? 'advanced' : 'beginner' });
        },
      },
    ],
    [sections, nav, navigate, shown, mode],
  );

  const tree = query.data;
  const topBar = (
    <>
      {getSession().desktop && <WorkspaceSwitcher />}
      {tree && tree.orgs.length > 0 && (
        <ScopeSwitcher
          levels={[
            {
              id: 'org',
              label: 'Organization',
              options: tree.orgs,
              value: current.org?.id,
              onChange: (id) => {
                choose('org', id);
              },
            },
            {
              id: 'workspace',
              label: 'Workspace',
              options: current.org?.workspaces ?? [],
              value: current.workspace?.id,
              onChange: (id) => {
                choose('workspace', id);
              },
            },
            {
              id: 'project',
              label: 'Project',
              options: current.workspace?.projects ?? [],
              value: current.project?.id,
              onChange: (id) => {
                choose('project', id);
              },
            },
          ]}
        />
      )}
      {query.isPending && <span className="text-sm text-fg-muted">Loading your projects…</span>}
      <Button
        size="sm"
        onClick={() => {
          setPaletteOpen(true);
        }}
      >
        <Icon name="search" />
        Search
        <kbd className="text-xs text-fg-muted">Ctrl K</kbd>
      </Button>
      <Button
        size="sm"
        variant="ghost"
        aria-label={shown === 'dark' ? 'Use light theme' : 'Use dark theme'}
        onClick={() => {
          setPrefs({ theme: shown === 'dark' ? 'light' : 'dark' });
        }}
      >
        <Icon name={shown === 'dark' ? 'sun' : 'moon'} />
      </Button>
      <SegmentedControl
        label="Detail level"
        options={modeOptions}
        value={mode}
        onChange={(v) => {
          setPrefs({ mode: v });
        }}
      />
    </>
  );

  const scopeError = query.isError ? describeError(query.error) : null;

  // First-run onboarding gets the whole window.
  if (path === '/welcome') {
    return (
      <main id="main" className="min-h-screen bg-bg p-6 text-fg">
        <CoreStatusBanner />
        <Outlet />
      </main>
    );
  }

  return (
    <AppShell
      brand="MasterRL"
      topBar={topBar}
      linkComponent={RouterLink}
      nav={<LeftNav sections={sections} currentPath={path} linkComponent={RouterLink} />}
      banner={
        <>
          <CoreStatusBanner />
          <FirstRunRedirect />
          <LiveStatusBanner />
          {scopeError && (
            <PlainError
              className="mb-4"
              code={scopeError.code}
              detail={scopeError.detail ?? 'Your organizations and projects could not be loaded.'}
              traceId={scopeError.traceId}
              onRetry={() => void query.refetch()}
            />
          )}
        </>
      }
    >
      <CommandPalette commands={commands} open={paletteOpen} onOpenChange={setPaletteOpen} />
      <Outlet />
    </AppShell>
  );
}
