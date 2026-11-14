/** Semantic color tokens. Keep in sync with `tokens.css` (a test checks this). */
export const colors = {
  light: {
    bg: '#ffffff',
    surface: '#f3f4f6',
    fg: '#111827',
    'fg-muted': '#4b5563',
    border: '#d1d5db',
    accent: '#1d4ed8',
    'accent-fg': '#ffffff',
    danger: '#b91c1c',
  },
  dark: {
    bg: '#0b0f17',
    surface: '#161b26',
    fg: '#f3f4f6',
    'fg-muted': '#9ca3af',
    border: '#374151',
    accent: '#60a5fa',
    'accent-fg': '#0b0f17',
    danger: '#f87171',
  },
} as const;

export type Theme = keyof typeof colors;
export type ColorToken = keyof (typeof colors)['light'];

/** Text-on-background pairs that must meet WCAG AA (4.5:1) in every theme. */
export const textPairs: readonly (readonly [text: ColorToken, background: ColorToken])[] = [
  ['fg', 'bg'],
  ['fg', 'surface'],
  ['fg-muted', 'bg'],
  ['fg-muted', 'surface'],
  ['accent', 'bg'],
  ['accent', 'surface'],
  ['accent-fg', 'accent'],
  ['danger', 'bg'],
  ['danger', 'surface'],
];
