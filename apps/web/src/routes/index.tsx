import { createRoute } from '@tanstack/react-router';
import { HomePage } from '../app/pages';
import { rootRoute } from './__root';

export const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: function Home() {
    const { nav } = indexRoute.useRouteContext();
    return <HomePage nav={nav} />;
  },
});
