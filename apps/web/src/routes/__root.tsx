import { createRootRouteWithContext } from '@tanstack/react-router';
import type { NavEntry } from '../app/nav.registry';
import { NotFoundPage, RouteErrorPage } from '../app/pages';
import { Shell } from '../app/Shell';

export interface RouterContext {
  /** Registered pages (nav.registry plus, in dev, the mock nav). */
  nav: readonly NavEntry[];
}

/** Root layout: the app shell around every page. */
export const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: Shell,
  notFoundComponent: NotFoundPage,
  errorComponent: RouteErrorPage,
});
