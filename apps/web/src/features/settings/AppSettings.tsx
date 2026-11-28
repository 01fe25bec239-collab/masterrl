import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { KeyValueList } from '@mrl/ui';
import { getSession } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';
import { useDesktopSettings } from './desktop-settings';

const SLEEP_KEY = ['desktop', 'sleep'] as const;

/** The desktop app itself: closing to the tray, keeping the computer awake, version. */
export default function AppSettings() {
  const bridge = getSession().desktop;
  const settings = useDesktopSettings();
  const queryClient = useQueryClient();
  // Electron main keeps the blocker across reloads; read it rather than assume "off".
  const sleep = useQuery({
    queryKey: SLEEP_KEY,
    queryFn: () => bridge?.desktop.sleepState() ?? Promise.reject(new Error('No desktop app.')),
    enabled: bridge !== null,
  });
  const keepAwake = useMutation({
    mutationFn: (on: boolean) =>
      bridge?.desktop.blockSleep(on) ?? Promise.reject(new Error('No desktop app.')),
    onSettled: () => queryClient.invalidateQueries({ queryKey: SLEEP_KEY }),
  });
  const info = useQuery({
    queryKey: ['desktop', 'info'],
    queryFn: () => bridge?.app.getInfo() ?? Promise.reject(new Error('No desktop app.')),
    enabled: bridge !== null,
  });
  if (!bridge) return <DesktopOnly what="The app's own settings" />;
  return (
    <div className="flex flex-col gap-3">
      {settings.data && (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={settings.data.closeToTray}
            onChange={(e) => {
              settings.save({ closeToTray: e.target.checked });
            }}
          />
          Closing the window keeps MasterRL running in the tray
        </label>
      )}
      {sleep.data && (
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={sleep.data.manual}
            disabled={keepAwake.isPending}
            onChange={(e) => {
              keepAwake.mutate(e.target.checked);
            }}
          />
          Keep this computer awake until the app closes
        </label>
      )}
      <p className="max-w-prose text-sm text-fg-muted">
        {sleep.data?.training
          ? 'Training is running on this computer, so the app keeps it awake now.'
          : 'While training runs on this computer, the app keeps it awake on its own.'}
      </p>
      {info.data && (
        <KeyValueList
          items={[
            { key: 'App version', value: info.data.version },
            { key: 'System', value: info.data.platform },
          ]}
        />
      )}
    </div>
  );
}
