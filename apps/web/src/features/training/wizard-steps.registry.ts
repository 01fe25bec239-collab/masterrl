import type { ComponentType } from 'react';
import type { JsonObject, Level } from '@mrl/ui';

export interface WizardStepProps {
  /** The run draft being built; each step edits its own keys. */
  draft: JsonObject;
  onChange: (draft: JsonObject) => void;
  level: Level;
}

/** A step of the new-run wizard (Doc 10 §4.5). */
export interface WizardStepEntry {
  /** Stable kebab-case id. */
  id: string;
  title: string;
  /** Steps are shown in ascending order. */
  order: number;
  /** Skip the step for drafts it does not apply to (e.g. reward setup only for RL methods). */
  appliesTo?: (draft: JsonObject) => boolean;
  /** Plain message when the step is not done, or null. */
  validate?: (draft: JsonObject) => string | null;
  load: () => Promise<{ default: ComponentType<WizardStepProps> }>;
}

/**
 * One entry per feature (append-only, README §3.3). Example:
 *   { id: 'dataset', title: 'Choose your data', order: 20, load: () => import('./steps/DatasetStep') },
 */
export const wizardSteps: readonly WizardStepEntry[] = [];
