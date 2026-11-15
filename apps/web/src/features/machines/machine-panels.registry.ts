import type { ComponentType } from 'react';
import type { Level } from '@mrl/ui';

export interface MachinePanelProps {
  /** `node_…` id. */
  machineId: string;
}

/** A panel on the machine (compute node) detail page: GPUs, health, enrollment… */
export interface MachinePanel {
  /** Stable kebab-case id. */
  id: string;
  title: string;
  /** Panels are shown in ascending order. */
  order: number;
  /** `advanced` hides the panel in Beginner mode. */
  level?: Level;
  load: () => Promise<{ default: ComponentType<MachinePanelProps> }>;
}

/**
 * One entry per feature (append-only, README §3.3). Example:
 *   { id: 'gpus', title: 'GPUs', order: 10, load: () => import('./panels/GpusPanel') },
 */
export const machinePanels: readonly MachinePanel[] = [];
