import { colors, type Theme } from '../tokens/colors';

export type ThemeChoice = Theme | 'system';

/**
 * Apply a theme by setting the token CSS variables on <html>. `system` removes the
 * overrides so `tokens.css` follows the operating system again.
 * Uses CSSOM (not a style attribute), so it works under the app's strict CSP.
 */
export function applyTheme(
  choice: ThemeChoice,
  root: HTMLElement = document.documentElement,
): void {
  for (const token of Object.keys(colors.light)) {
    const name = `--mrl-color-${token}`;
    if (choice === 'system') root.style.removeProperty(name);
    else root.style.setProperty(name, colors[choice][token as keyof (typeof colors)['light']]);
  }
  if (choice === 'system') {
    root.style.removeProperty('color-scheme');
    delete root.dataset.theme;
  } else {
    root.style.setProperty('color-scheme', choice);
    root.dataset.theme = choice;
  }
}

/** The theme actually showing now. */
export function resolvedTheme(choice: ThemeChoice): Theme {
  if (choice !== 'system') return choice;
  return typeof matchMedia === 'function' && matchMedia('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'light';
}

/** Read a token's current value, e.g. for canvas charts that cannot use CSS classes. */
export function tokenColor(token: keyof (typeof colors)['light']): string {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(`--mrl-color-${token}`)
    .trim();
  return value || colors.light[token];
}
