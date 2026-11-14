import { render, screen } from '@testing-library/react';
import axe from 'axe-core';
import { expect, it } from 'vitest';
import { App } from './App';

it('renders the app heading with token colors and no a11y violations', async () => {
  const { container } = render(<App />);

  expect(screen.getByRole('heading', { level: 1, name: 'MasterRL' })).toBeDefined();
  expect(screen.getByRole('main').className).toContain('bg-bg');

  const results = await axe.run(container);
  expect(results.violations).toEqual([]);
});
