# Desktop (Electron)

`apps/desktop` wraps the web UI in a secure Electron window. Main stays thin: window, menus and
OS integration only. It never handles training data.

## Commands

```bash
pnpm --filter desktop dev          # Vite dev server + Electron window
pnpm --filter 'desktop...' build  # web + unsigned app in apps/desktop/release/
pnpm --filter desktop test         # unit tests
pnpm --filter desktop test:smoke   # Playwright opens the packaged app (build first)
```

## How it fits together

- `src/main/index.ts` — opens the window. Dev loads the Vite server; packaged builds load
  `app://app/`, served from the bundled renderer files (no `file://`).
- `src/main/security.ts` — `webPreferences`, the CSP and the external-link allow-list.
- `src/main/ipc.registry.ts` — every IPC channel with zod schemas for its arguments and result.
  The preload turns it into `window.mrl` (e.g. `window.mrl.app.getVersion()`).
- `src/main/ipc.ts` — handlers. Each call is checked: sender page, then input, then output.

## Adding an IPC channel

1. Add `'<area>:<action>': { input: z.tuple([...]), output: ... }` to `ipc.registry.ts`.
2. Add its handler in `ipc.ts` (TypeScript fails until you do).
3. Never accept raw file paths or shell strings; pass IDs and let main resolve them.

## Security rules

`contextIsolation`, `sandbox` and `webSecurity` stay on; `nodeIntegration` stays off; the CSP allows
no remote or inline scripts. Navigation and `window.open` are blocked; allow-listed https links open
in the system browser. `node scripts/check-security.ts` (part of `pnpm lint`) fails if any of
this is weakened.
