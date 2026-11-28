import type { ComponentType } from 'react';
import type { Level } from '@mrl/ui';

/** A section on the Settings page, shown after the built-in "Appearance" section. */
export interface SettingsSection {
  /** Stable kebab-case id; also the URL anchor (`/settings#<id>`). */
  id: string;
  title: string;
  /** One plain sentence under the title. */
  description: string;
  /** `advanced` hides the section in Beginner mode. */
  level?: Level;
  load: () => Promise<{ default: ComponentType }>;
}

/**
 * One entry per feature (append-only, README §3.3). Example:
 *   { id: 'storage', title: 'Storage', description: 'Where models and data are kept.',
 *     load: () => import('../features/storage/StorageSettings') },
 */
export const settingsSections: readonly SettingsSection[] = [
  // P30: setup sync, data and backups, privacy, updates, the desktop app.
  {
    id: 'sync',
    title: 'Sync your setup',
    description: 'Keep your machines, teams and app settings, locked with your passphrase.',
    load: () => import('../features/settings/SyncSettings'),
  },
  {
    id: 'data',
    title: 'Data and backups',
    description: 'Where the app keeps its data, and the backups it makes every day.',
    load: () => import('../features/settings/DataSettings'),
  },
  {
    id: 'privacy',
    title: 'Privacy and crash reports',
    description: 'Crash reports stay on this computer. Nothing is uploaded.',
    load: () => import('../features/settings/PrivacySettings'),
  },
  {
    id: 'updates',
    title: 'Updates',
    description: 'New versions of the app, installed when you choose.',
    load: () => import('../features/settings/UpdateSettings'),
  },
  {
    id: 'app',
    title: 'This app',
    description: 'How the app behaves on this computer.',
    load: () => import('../features/settings/AppSettings'),
  },
];
