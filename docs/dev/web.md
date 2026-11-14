# Web (renderer)

`apps/web` is the React UI. It runs in a browser for development and inside the desktop app.

## Layout

- `apps/web` — React 19 + Vite + TypeScript (strict). Tailwind v4 for styling.
- `packages/ui` — design tokens and, later, the UI primitives. Use only these in the app.
  - `src/tokens/colors.ts` — color tokens (light and dark) as TS constants.
  - `src/tokens/tokens.css` — the same tokens as `--mrl-color-*` CSS variables.
  - `src/tokens/tailwind-preset.css` — Tailwind preset: `bg-surface`, `text-fg`, `text-fg-muted`, …
    Only token colors exist; Tailwind's default palette is turned off.
- `packages/api-client` — the only way to call the core API (`http://127.0.0.1`, token in the
  `Authorization` header). ESLint blocks `fetch('/api…')` in renderer code.

New features go in `apps/web/src/features/<section>/` and routes in `apps/web/src/routes/<section>/`.

## Commands

```bash
pnpm install
pnpm --filter web dev        # http://localhost:5173
pnpm -r lint                 # ESLint (strict, react-hooks, jsx-a11y) + Prettier
pnpm -r typecheck
pnpm -r test                 # Vitest
pnpm --filter web build
```

## Rules

- Function components, strict TypeScript, no `any`.
- Text colors must pass WCAG AA (4.5:1). Add every new text/background pair to `textPairs` in
  `colors.ts`; the token test checks them in both themes.
- When you change a token, update `colors.ts`, `tokens.css` and `tailwind-preset.css` together.
  The test fails if they drift apart.
