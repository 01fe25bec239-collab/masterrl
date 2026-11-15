import type { ComponentType } from 'react';
import type { IconName, Level } from '@mrl/ui';

/** A page in the left nav. Its route, nav item, palette command and home card come from here. */
export interface NavEntry {
  /** Stable kebab-case id, e.g. `datasets`. */
  id: string;
  /** Plain words, e.g. "Datasets". */
  label: string;
  /** Route path, e.g. `/datasets`. */
  path: `/${string}`;
  icon: IconName;
  /** `project` pages sit under the project; `workspace` pages under the workspace heading. */
  section: 'project' | 'workspace';
  /** `advanced` hides the entry in Beginner mode. */
  level?: Level;
  /** Extra words the command palette should match. */
  keywords?: string[];
  /** The page, code-split. */
  load: () => Promise<{ default: ComponentType }>;
  /** Optional summary card on the home dashboard. */
  homeCard?: () => Promise<{ default: ComponentType }>;
}

/**
 * One entry per feature (append-only, README §3.3). Example:
 *   { id: 'datasets', label: 'Datasets', path: '/datasets', icon: 'file', section: 'project',
 *     load: () => import('../features/datasets/DatasetsPage') },
 */
export const navRegistry: readonly NavEntry[] = [];
