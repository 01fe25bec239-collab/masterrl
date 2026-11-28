import { useQuery } from '@tanstack/react-query';
import { getSession, mrl } from './session';

/** Query key of `GET /api/v1/desktop/state`; invalidate it after onboarding changes. */
export const DESKTOP_STATE_KEY = ['desktop', 'state'] as const;

/** The desktop core's state: onboarding step, data folder, version. Off in a browser. */
export function useDesktopState() {
  return useQuery({
    queryKey: DESKTOP_STATE_KEY,
    queryFn: () => mrl().GET('/api/v1/desktop/state'),
    enabled: getSession().desktop !== null,
  });
}
