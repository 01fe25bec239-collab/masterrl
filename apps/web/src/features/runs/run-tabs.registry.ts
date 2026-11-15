import type { ComponentType } from 'react';
import type { Level } from '@mrl/ui';

export interface RunTabProps {
  runId: string;
}

/** A tab on the run detail page (Doc 10 §4.11: Overview, Metrics, Logs, Checkpoints…). */
export interface RunTab {
  /** Stable kebab-case id; also the `?tab=` value. */
  id: string;
  label: string;
  /** Tabs are shown in ascending order. */
  order: number;
  /** `advanced` hides the tab in Beginner mode. */
  level?: Level;
  load: () => Promise<{ default: ComponentType<RunTabProps> }>;
}

/**
 * One entry per feature (append-only, README §3.3). Example:
 *   { id: 'logs', label: 'Logs', order: 30, load: () => import('./tabs/LogsTab') },
 */
export const runTabs: readonly RunTab[] = [];
