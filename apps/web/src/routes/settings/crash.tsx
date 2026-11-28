import { createRoute, lazyRouteComponent } from '@tanstack/react-router';
import { rootRoute } from '../__root';

/** "Something went wrong": one local crash report, with Copy and Open a GitHub issue. */
export const crashReportRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings/crash/$id',
  component: lazyRouteComponent(() => import('../../features/settings/CrashReportPage')),
});
