import { createMemoryHistory } from '@tanstack/react-router';
import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axe from 'axe-core';
import { http, HttpResponse } from 'msw';
import { describe, expect, it } from 'vitest';
import { ProblemError } from '@mrl/api-client';
import { App } from './App';
import type { NavEntry } from './app/nav.registry';
import { navRegistry } from './app/nav.registry';
import { reloadPrefs } from './app/prefs';
import { createAppRouter } from './app/router';
import { settingsSections } from './app/settings-sections.registry';
import { machinePanels } from './features/machines/machine-panels.registry';
import { runTabs } from './features/runs/run-tabs.registry';
import { wizardSteps } from './features/training/wizard-steps.registry';
import { API_URL, createQueryClient } from './lib/api';
import { server } from './mocks/test-setup';

const page = (text: string) => () => Promise.resolve({ default: () => <p>{text}</p> });

const nav: NavEntry[] = [
  {
    id: 'datasets',
    label: 'Datasets',
    path: '/datasets',
    icon: 'file',
    section: 'project',
    load: page('Datasets page'),
  },
  {
    id: 'engines',
    label: 'Engines',
    path: '/engines',
    icon: 'settings',
    section: 'workspace',
    level: 'advanced',
    load: page('Engines page'),
  },
  {
    id: 'secret',
    label: 'Secret',
    path: '/secret',
    icon: 'lock',
    section: 'project',
    load: () =>
      Promise.resolve({
        default: () => {
          throw new ProblemError({
            type: 'about:blank',
            title: 'Forbidden',
            status: 403,
            code: 'MRL-AUTH-003',
          });
        },
      }),
  },
];

async function renderApp(path = '/', entries: readonly NavEntry[] = nav) {
  reloadPrefs();
  const router = createAppRouter(entries, createMemoryHistory({ initialEntries: [path] }));
  const view = render(<App router={router} queryClient={createQueryClient()} />);
  await act(() => router.load());
  return { router, ...view };
}

async function seriousViolations(container: HTMLElement) {
  const results = await axe.run(container);
  return results.violations
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
}

describe('registries', () => {
  it('all start empty and typed; features append one entry each', () => {
    expect(
      [navRegistry, settingsSections, runTabs, wizardSteps, machinePanels].map((r) => r.length),
    ).toEqual([0, 0, 0, 0, 0]);
  });
});

describe('app shell', () => {
  it('shows the home dashboard with scopes from the API and no serious a11y problems', async () => {
    const { container } = await renderApp();

    expect(screen.getByRole('heading', { level: 1, name: 'Welcome to MasterRL' })).toBeDefined();
    expect(await screen.findByRole('combobox', { name: 'Organization' })).toHaveProperty(
      'value',
      'org_home',
    );
    expect(screen.getByRole('combobox', { name: 'Project' })).toHaveProperty(
      'value',
      'prj_support_bot',
    );
    expect(screen.getByText('Support bot', { selector: 'dd' })).toBeDefined();
    expect(screen.getByText('Nothing to show yet')).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
  });

  it('renders nav entries from the registry and routes to them', async () => {
    const user = userEvent.setup();
    const { router } = await renderApp();
    const navEl = screen.getByRole('navigation', { name: 'Main' });

    expect(within(navEl).getByRole('link', { name: 'Overview' }).getAttribute('aria-current')).toBe(
      'page',
    );
    await user.click(within(navEl).getByRole('link', { name: 'Datasets' }));
    expect(await screen.findByText('Datasets page')).toBeDefined();
    expect(router.state.location.pathname).toBe('/datasets');
  });

  it('hides advanced entries in Beginner mode and keeps the choice on this computer', async () => {
    const user = userEvent.setup();
    await renderApp();
    const navEl = screen.getByRole('navigation', { name: 'Main' });
    expect(within(navEl).queryByRole('link', { name: 'Engines' })).toBeNull();

    await user.click(screen.getAllByRole('radio', { name: 'Advanced' })[0] as HTMLElement);
    expect(within(navEl).getByRole('link', { name: 'Engines' })).toBeDefined();
    expect(JSON.parse(localStorage.getItem('mrl.prefs.v1') ?? '{}')).toMatchObject({
      mode: 'advanced',
    });
  });

  it('switches between light and dark themes', async () => {
    const user = userEvent.setup();
    await renderApp();
    await user.click(screen.getByRole('button', { name: 'Use dark theme' }));
    expect(document.documentElement.dataset.theme).toBe('dark');
    expect(document.documentElement.style.getPropertyValue('--mrl-color-bg')).toBe('#0b0f17');
    await user.click(screen.getByRole('button', { name: 'Use light theme' }));
    expect(document.documentElement.dataset.theme).toBe('light');
  });

  it('shows a plain 404 page for unknown addresses', async () => {
    const { container } = await renderApp('/no/such/page');
    expect(await screen.findByRole('heading', { level: 1, name: 'Page not found' })).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
  });

  it('shows the 403 page when the server says no access', async () => {
    const { container } = await renderApp('/secret');
    expect(await screen.findByRole('heading', { level: 1, name: 'No access' })).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
  });

  it('explains a failed scope load and can retry', async () => {
    server.use(
      http.get(`${API_URL}/api/v1/me/scopes`, () =>
        HttpResponse.json(
          { type: 'about:blank', title: 'Bad request', status: 400, code: 'MRL-VAL-001' },
          { status: 400, headers: { 'Content-Type': 'application/problem+json' } },
        ),
      ),
    );
    await renderApp();
    const alerts = await screen.findAllByRole('alert');
    expect(alerts[0]?.textContent).toContain('The app could not read this request.');
    expect(alerts[0]?.textContent).toContain('MRL-VAL-001');
    expect(
      within(alerts[0] as HTMLElement).getByRole('button', { name: 'Try again' }),
    ).toBeDefined();
  });

  it('opens the command palette with Ctrl+K and goes to a page', async () => {
    const user = userEvent.setup();
    const { router } = await renderApp();
    await user.keyboard('{Control>}k{/Control}');
    const input = await screen.findByRole('combobox', { name: 'Search commands' });
    await user.type(input, 'settings');
    await user.keyboard('{Enter}');
    expect(await screen.findByRole('heading', { level: 1, name: 'Settings' })).toBeDefined();
    expect(router.state.location.pathname).toBe('/settings');
  });

  it('settings page has no serious a11y problems', async () => {
    const { container } = await renderApp('/settings');
    expect(await screen.findByRole('heading', { level: 2, name: 'Appearance' })).toBeDefined();
    expect(await seriousViolations(container)).toEqual([]);
  });
});
