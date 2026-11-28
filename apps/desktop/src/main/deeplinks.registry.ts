import * as z from 'zod/mini';

/** What a deep link handler may do. */
export interface DeepLinkContext {
  /** Finishes a sign-in the app started (`flow` must match it). */
  completeSignIn: (flow: string, code: string) => boolean;
  /** Shows a screen of the app, like `/settings`. */
  navigate: (route: string) => void;
}

/** One `masterrl://<path>?<query>` link the app answers. */
export interface DeepLink<S extends z.ZodMiniType = z.ZodMiniType> {
  /** Host and path without slashes at the ends, like `auth/callback`. */
  path: string;
  /** The query, checked before `handle` sees it. Unknown keys are dropped. */
  params: S;
  handle: (params: z.output<S>, ctx: DeepLinkContext) => void;
}

const entry = <S extends z.ZodMiniType>(link: DeepLink<S>): DeepLink => link;

/** A screen route: lower-case letters, digits, `_`, `-` and `/`. No dots, no `//`. */
export const routePattern = /^\/(?:[a-z0-9_-]+(?:\/[a-z0-9_-]+)*)?$/i;

/**
 * Every deep link the app answers (append-only, one entry per feature).
 * P43 adds `team/join`.
 */
export const deepLinks: readonly DeepLink[] = [
  // P30: the browser comes back from sign-in (P16's PKCE flow).
  entry({
    path: 'auth/callback',
    params: z.object({
      flow: z.string().check(z.regex(/^[A-Za-z0-9_-]{1,128}$/)),
      code: z.string().check(z.regex(/^[A-Za-z0-9._~-]{1,2048}$/)),
    }),
    handle: (p, ctx) => {
      ctx.completeSignIn(p.flow, p.code);
    },
  }),
  // P30: open a screen, like `masterrl://open?screen=/settings`.
  entry({
    path: 'open',
    params: z.object({
      screen: z.string().check(z.maxLength(200), z.regex(routePattern)),
    }),
    handle: (p, ctx) => {
      ctx.navigate(p.screen);
    },
  }),
];
