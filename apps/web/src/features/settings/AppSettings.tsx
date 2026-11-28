import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { KeyValueList } from '@mrl/ui';
import { getSession } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';
import { useDesktopSettings } from './desktop-settings';

/** The desktop app itself: closing to the tray, keeping the computer awake, version. */
export default function AppSettings() {
  const bridge = getSession().desktop;
  const settings = useDesktopSettings();
  const [awake, setAwake] = useState(false);
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
      <label className="flex items-center gap-2">
        <input
          type="checkbox"
          checked={awake}
          onChange={(e) => {
            const on = e.target.checked;
            void bridge.desktop.blockSleep(on).then(() => {
              setAwake(on);
            });
          }}
        />
        Keep this computer awake until the app closes
      </label>
      <p className="max-w-prose text-sm text-fg-muted">
        While training runs on this computer, the app keeps it awake on its own.
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
