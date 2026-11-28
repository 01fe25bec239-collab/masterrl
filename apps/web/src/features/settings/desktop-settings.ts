import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getSession, type DesktopSettings } from '../../app/session';

const KEY = ['desktop', 'settings'] as const;

/** The desktop app's own settings (kept by Electron main in `desktop-settings.json`). */
export function useDesktopSettings() {
  const bridge = getSession().desktop;
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: KEY,
    queryFn: () => bridge?.settings.get() ?? Promise.reject(new Error('No desktop app.')),
    enabled: bridge !== null,
  });
  const mutation = useMutation({
    mutationFn: (patch: Partial<DesktopSettings>) =>
      bridge?.settings.set(patch) ?? Promise.reject(new Error('No desktop app.')),
    onSuccess: (next) => {
      queryClient.setQueryData(KEY, next);
    },
  });
  return {
    data: query.data,
    save: (patch: Partial<DesktopSettings>) => {
      mutation.mutate(patch);
    },
  };
}
