import {
  createHashHistory,
  createRoute,
  createRouter,
  lazyRouteComponent,
  type RouterHistory,
} from '@tanstack/react-router';
import { indexRoute } from '../routes/index';
import { rootRoute } from '../routes/__root';
import { crashReportRoute } from '../routes/settings/crash';
import { welcomeRoute } from '../routes/welcome';
import type { NavEntry } from './nav.registry';
import { SettingsPage } from './pages';

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
});

/**
 * Routes: home, settings and one per nav entry. Hash history, because the desktop app serves
 * pages from `app://app/index.html`, where path-based URLs do not exist.
 */
export function createAppRouter(
  nav: readonly NavEntry[],
  history: RouterHistory = createHashHistory(),
) {
  const pages = nav.map((entry) =>
    createRoute({
      getParentRoute: () => rootRoute,
      path: entry.path,
      component: lazyRouteComponent(entry.load),
    }),
  );
  return createRouter({
    routeTree: rootRoute.addChildren([
      indexRoute,
      settingsRoute,
      welcomeRoute,
      crashReportRoute,
      ...pages,
    ]),
    history,
    context: { nav },
    defaultPreload: 'intent',
  });
}

export type AppRouter = ReturnType<typeof createAppRouter>;

declare module '@tanstack/react-router' {
  interface Register {
    router: AppRouter;
  }
}
