import type { ScopeTree } from '../app/scopes';

export const scopeTree: ScopeTree = {
  orgs: [
    {
      id: 'org_home',
      name: 'My computer',
      workspaces: [
        {
          id: 'ws_personal',
          name: 'Personal',
          projects: [
            { id: 'prj_support_bot', name: 'Support bot' },
            { id: 'prj_math_tutor', name: 'Math tutor' },
          ],
        },
        { id: 'ws_lab', name: 'Lab', projects: [{ id: 'prj_lab_eval', name: 'Eval sweeps' }] },
      ],
    },
  ],
};

export const jobStates = [
  'QUEUED',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'PAUSED',
  'CANCELLED',
] as const;

export interface MockRun {
  id: string;
  name: string;
  algorithm: string;
  state: (typeof jobStates)[number];
  step: number;
  totalSteps: number;
}

/** 230 runs, so cursor pagination has several pages. */
export const runs: MockRun[] = Array.from({ length: 230 }, (_, i) => ({
  id: `run_${String(i + 1).padStart(4, '0')}`,
  name: `${['sft', 'dpo', 'grpo'][i % 3] ?? 'sft'}-try-${i + 1}`,
  algorithm: ['sft', 'dpo', 'grpo'][i % 3] ?? 'sft',
  state: jobStates[i % jobStates.length] ?? 'QUEUED',
  step: (i * 37) % 5000,
  totalSteps: 5000,
}));

/** Smooth, noisy training loss at `step`. */
export const lossAt = (step: number) =>
  2.4 * Math.exp(-step / 900) + 0.35 + 0.05 * Math.sin(step / 7);

export const logLine = (n: number) =>
  n % 25 === 0
    ? `step ${n * 10}: saved checkpoint ckpt_${n}`
    : `step ${n * 10}: loss=${lossAt(n * 10).toFixed(4)} lr=2.0e-05 tokens/s=${1800 + (n % 9) * 13}`;
