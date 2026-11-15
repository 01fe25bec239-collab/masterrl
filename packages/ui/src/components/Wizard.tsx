import { useId, useRef, useState, type ReactNode } from 'react';
import { cn } from '../lib/cn';
import { Button, focusRing } from './Button';
import { Icon } from './Icon';

export type StepStatus = 'todo' | 'current' | 'complete' | 'error';

export interface StepperStep {
  id: string;
  title: string;
  status: StepStatus;
}

export interface StepperProps {
  steps: StepperStep[];
  /** Lets people jump back to a finished step. */
  onSelect?: (id: string) => void;
  label?: string;
}

const statusText: Record<StepStatus, string> = {
  todo: 'not started',
  current: 'current step',
  complete: 'done',
  error: 'needs attention',
};

/** Left-rail list of steps with their state in words and icons. */
export function Stepper({ steps, onSelect, label = 'Steps' }: StepperProps) {
  return (
    <nav aria-label={label}>
      <ol className="flex flex-col gap-1">
        {steps.map((step, i) => {
          const icon =
            step.status === 'complete' ? 'check' : step.status === 'error' ? 'alert' : 'circle';
          const body = (
            <>
              <span
                className={cn(
                  'flex size-6 items-center justify-center rounded-full border text-xs',
                  step.status === 'current' ? 'border-accent text-accent' : 'border-border',
                  step.status === 'error' && 'border-danger text-danger',
                )}
              >
                {step.status === 'todo' || step.status === 'current' ? (
                  i + 1
                ) : (
                  <Icon name={icon} className="size-3.5" />
                )}
              </span>
              <span className="flex flex-col text-left">
                <span className={cn('text-sm', step.status === 'current' && 'font-semibold')}>
                  {step.title}
                </span>
                <span className="text-xs text-fg-muted">{statusText[step.status]}</span>
              </span>
            </>
          );
          const canJump = onSelect && step.status !== 'todo' && step.status !== 'current';
          return (
            <li key={step.id} aria-current={step.status === 'current' ? 'step' : undefined}>
              {canJump ? (
                <button
                  type="button"
                  className={cn(
                    'flex w-full items-center gap-2 rounded-md p-2 text-fg hover:bg-surface',
                    focusRing,
                  )}
                  onClick={() => {
                    onSelect(step.id);
                  }}
                >
                  {body}
                </button>
              ) : (
                <div className="flex items-center gap-2 p-2 text-fg">{body}</div>
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}

export interface WizardStep {
  id: string;
  title: string;
  render: () => ReactNode;
  /** Return a plain message when the step is not ready, or null to continue. */
  validate?: () => string | null;
}

export interface WizardProps {
  title: string;
  steps: WizardStep[];
  onFinish: () => void;
  finishLabel?: string;
  onCancel?: () => void;
}

/** Step-by-step flow with a Stepper rail, Back/Next and per-step checks. */
export function Wizard({ title, steps, onFinish, finishLabel = 'Finish', onCancel }: WizardProps) {
  const [index, setIndex] = useState(0);
  const [done, setDone] = useState<ReadonlySet<string>>(new Set());
  const [problem, setProblem] = useState<{ step: string; message: string } | null>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const headingId = useId();
  const step = steps[index];
  if (!step) return null;
  const last = index === steps.length - 1;

  const go = (next: number) => {
    setIndex(next);
    setProblem(null);
    requestAnimationFrame(() => headingRef.current?.focus());
  };
  const next = () => {
    const message = step.validate?.() ?? null;
    if (message) {
      setProblem({ step: step.id, message });
      return;
    }
    setDone(new Set(done).add(step.id));
    if (last) onFinish();
    else go(index + 1);
  };

  return (
    <section aria-label={title} className="grid gap-6 md:grid-cols-[14rem_1fr]">
      <Stepper
        label={`${title} steps`}
        steps={steps.map((s, i) => ({
          id: s.id,
          title: s.title,
          status:
            problem?.step === s.id
              ? 'error'
              : i === index
                ? 'current'
                : done.has(s.id)
                  ? 'complete'
                  : 'todo',
        }))}
        onSelect={(id) => {
          const target = steps.findIndex((s) => s.id === id);
          if (target >= 0) go(target);
        }}
      />
      <div aria-labelledby={headingId} role="group">
        <h2
          id={headingId}
          ref={headingRef}
          tabIndex={-1}
          className="text-xl font-semibold outline-none"
        >
          Step {index + 1} of {steps.length}: {step.title}
        </h2>
        <div className="mt-4">{step.render()}</div>
        {problem?.step === step.id && (
          <p role="alert" className="mt-4 flex items-center gap-2 text-danger">
            <Icon name="alert" />
            {problem.message}
          </p>
        )}
        <div className="mt-6 flex gap-2">
          {onCancel && (
            <Button variant="ghost" onClick={onCancel}>
              Cancel
            </Button>
          )}
          <Button
            disabled={index === 0}
            onClick={() => {
              go(index - 1);
            }}
          >
            Back
          </Button>
          <Button variant="primary" onClick={next}>
            {last ? finishLabel : 'Next'}
          </Button>
        </div>
      </div>
    </section>
  );
}
