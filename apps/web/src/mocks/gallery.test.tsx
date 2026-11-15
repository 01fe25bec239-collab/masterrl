import { createMemoryHistory } from '@tanstack/react-router';
import { act, render, screen } from '@testing-library/react';
import axe from 'axe-core';
import { describe, expect, it, vi } from 'vitest';
import { App } from '../App';
import { reloadPrefs, setPrefs } from '../app/prefs';
import { createAppRouter } from '../app/router';
import { createQueryClient } from '../lib/api';
import { mockNav } from './nav';

// Monaco needs a real browser; the YAML editor's own logic is tested in packages/ui.
vi.mock('../../../../packages/ui/src/forms/monaco-host', () => ({
  default: ({ label, value }: { label: string; value: string }) => (
    <textarea aria-label={label} value={value} readOnly />
  ),
}));

async function renderPage(path: string) {
  reloadPrefs();
  setPrefs({ mode: 'advanced' });
  const router = createAppRouter(mockNav, createMemoryHistory({ initialEntries: [path] }));
  const view = render(<App router={router} queryClient={createQueryClient()} />);
  await act(() => router.load());
  return view;
}

describe.each(mockNav.map((e) => [e.label, e.path] as const))('%s demo page', (label, path) => {
  it('renders with no serious a11y problems', async () => {
    const { container } = await renderPage(path);
    const title = label.replace(' (demo)', '');
    expect(
      await screen.findByRole('heading', { level: 1, name: title }, { timeout: 5000 }),
    ).toBeDefined();
    const results = await axe.run(container);
    const serious = results.violations
      .filter((v) => v.impact === 'serious' || v.impact === 'critical')
      .map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
    expect(serious).toEqual([]);
  }, 20_000);
});

it('home shows registered dashboard cards', async () => {
  await renderPage('/');
  expect(
    await screen.findByRole('heading', { level: 3, name: 'Active runs (demo)' }),
  ).toBeDefined();
});

it('the mock stream feeds the live page through the shared hub', async () => {
  await renderPage('/demo/live');
  // jsdom has no layout, so the virtualized lines themselves are not drawn; the count proves delivery.
  expect(await screen.findByText(/^[1-9]\d* lines$/, {}, { timeout: 5000 })).toBeDefined();
}, 10_000);
