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
export const settingsSections: readonly SettingsSection[] = [];
