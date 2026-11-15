import { useSyncExternalStore } from 'react';
import { z } from 'zod';
import { applyTheme } from '@mrl/ui';

/** Stored on this computer until user preferences move to the core (P30). */
const prefsSchema = z.object({
  theme: z.enum(['system', 'light', 'dark']).catch('system'),
  mode: z.enum(['beginner', 'advanced']).catch('beginner'),
  scope: z
    .object({
      org: z.string().optional(),
      workspace: z.string().optional(),
      project: z.string().optional(),
    })
    .catch({}),
});

export type Prefs = z.infer<typeof prefsSchema>;

const KEY = 'mrl.prefs.v1';
const listeners = new Set<() => void>();

function read(): Prefs {
  let raw: unknown = {};
  try {
    raw = JSON.parse(localStorage.getItem(KEY) ?? '{}');
  } catch {
    raw = {};
  }
  return prefsSchema.parse(raw && typeof raw === 'object' ? raw : {});
}

let current = read();

export function getPrefs(): Prefs {
  return current;
}

export function setPrefs(patch: Partial<Prefs>): void {
  current = { ...current, ...patch };
  try {
    localStorage.setItem(KEY, JSON.stringify(current));
  } catch {
    // Storage full or blocked: keep the choice for this session only.
  }
  if (patch.theme) applyTheme(patch.theme);
  for (const l of listeners) l();
}

/** Re-read storage, e.g. in tests or after another tab changed it. */
export function reloadPrefs(): void {
  current = read();
  applyTheme(current.theme);
  for (const l of listeners) l();
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function usePrefs(): Prefs {
  return useSyncExternalStore(subscribe, getPrefs);
}

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key === KEY) reloadPrefs();
  });
}
