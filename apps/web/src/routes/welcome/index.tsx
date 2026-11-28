import { createRoute, lazyRouteComponent } from '@tanstack/react-router';
import { rootRoute } from '../__root';

/** First-run onboarding (desktop): Welcome → Sign in → Restore my setup → Check this computer. */
export const welcomeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/welcome',
  component: lazyRouteComponent(() => import('../../features/onboarding/OnboardingPage')),
});
