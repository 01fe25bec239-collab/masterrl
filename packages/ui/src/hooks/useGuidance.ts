import { useMemo } from 'react';
import { explainError, getError, type Explanation } from '../../../../guidance/build/index.js';

export interface Guidance extends Explanation {
  /** The code asked for, if any. */
  code: string | undefined;
  /** False when the catalog has no entry and this is the generic fallback. */
  known: boolean;
}

/** Plain-language explanation (what happened / why / what to do) for an error code. */
export function guidanceFor(code: string | undefined): Guidance {
  const known = code ? getError(code) !== undefined : false;
  return { ...explainError(code ?? ''), code, known };
}

export function useGuidance(code: string | undefined): Guidance {
  return useMemo(() => guidanceFor(code), [code]);
}

export type Platform = 'windows' | 'macos' | 'linux';

/** The platform the app runs on, for OS-specific steps. */
export function currentPlatform(): Platform | undefined {
  if (typeof navigator === 'undefined') return undefined;
  const ua = navigator.userAgent;
  if (/Windows/i.test(ua)) return 'windows';
  if (/Mac OS X|Macintosh/i.test(ua)) return 'macos';
  if (/Linux/i.test(ua)) return 'linux';
  return undefined;
}
