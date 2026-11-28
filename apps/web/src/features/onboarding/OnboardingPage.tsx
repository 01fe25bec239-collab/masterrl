import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useRef, useState } from 'react';
import { describeError } from '@mrl/api-client';
import { AsyncView, EmptyState, PageHeader, PlainError, Stepper } from '@mrl/ui';
import {
  DESKTOP_STATE_KEY,
  getSession,
  mrl,
  useDesktopState,
  type Schemas,
} from '../../app/session';
import { CheckStep } from './CheckStep';
import { RestoreStep } from './RestoreStep';
import { SignInStep } from './SignInStep';
import type { StepProps } from './StepFooter';
import { StepFooter } from './StepFooter';

/** The onboarding steps, in order (the core keeps the one to show next). */
export const STEPS = [
  { id: 'welcome', title: 'Welcome' },
  { id: 'sign_in', title: 'Sign in' },
  { id: 'restore', title: 'Restore my setup' },
  { id: 'check', title: 'Check this computer' },
] as const;

type StepId = (typeof STEPS)[number]['id'];

function stepFrom(saved: string): StepId {
  return STEPS.find((s) => s.id === saved)?.id ?? 'welcome';
}

function WelcomeStep({ onNext }: StepProps) {
  return (
    <>
      <p className="max-w-prose">
        MasterRL trains and tests language models on this computer and on machines you connect. Your
        data, models and prompts stay on your machines.
      </p>
      <p className="mt-2 max-w-prose text-fg-muted">
        Setting up takes about two minutes: sign in, bring back a setup you saved on another
        computer, and see what this computer can do.
      </p>
      <StepFooter onNext={onNext} nextLabel="Get started" />
    </>
  );
}

/** First run: Welcome → Sign in → Restore my setup → Check this computer. */
export default function OnboardingPage() {
  const state = useDesktopState();
  if (!getSession().desktop) {
    return (
      <>
        <PageHeader title="Set up MasterRL" />
        <EmptyState
          icon="info"
          title="Setting up happens in the desktop app"
          description="Open the MasterRL app on your computer to sign in and check what it can do."
          action={
            <Link to="/" className="text-accent underline">
              Go to the home page
            </Link>
          }
        />
      </>
    );
  }
  return (
    <AsyncView
      noun="your setup"
      status={state.status}
      data={state.data}
      error={describeError(state.error)}
      onRetry={() => void state.refetch()}
      empty={{ title: '', description: '' }}
    >
      {(data) => <Onboarding saved={data.onboarding} />}
    </AsyncView>
  );
}

function Onboarding({ saved }: { saved: Schemas['DesktopOnboarding'] }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [step, setStep] = useState<StepId>(() =>
    saved.step === 'done' ? 'check' : stepFrom(saved.step),
  );
  const [reached, setReached] = useState(() =>
    saved.step === 'done'
      ? STEPS.length - 1
      : STEPS.findIndex((s) => s.id === stepFrom(saved.step)),
  );
  const heading = useRef<HTMLHeadingElement>(null);
  const save = useMutation({
    mutationFn: (next: StepId | 'done') =>
      mrl().PUT('/api/v1/desktop/onboarding', { body: { step: next } }),
    onSuccess: (onboarding) => {
      queryClient.setQueryData<Schemas['DesktopState']>(DESKTOP_STATE_KEY, (old) =>
        old ? { ...old, onboarding } : old,
      );
    },
  });
  const index = STEPS.findIndex((s) => s.id === step);

  const go = (next: StepId) => {
    setStep(next);
    const at = STEPS.findIndex((s) => s.id === next);
    if (at > reached) {
      setReached(at);
      // Saved so a restart (or a crash) comes back to this step.
      save.mutate(next);
    }
    requestAnimationFrame(() => heading.current?.focus());
  };
  const back =
    index > 0
      ? () => {
          go(STEPS[index - 1]?.id ?? 'welcome');
        }
      : undefined;
  const nextStep = STEPS[index + 1];
  const next = nextStep
    ? () => {
        go(nextStep.id);
      }
    : undefined;
  const finish = async () => {
    await save.mutateAsync('done');
    await navigate({ to: '/' });
  };
  const failed = save.isError ? describeError(save.error) : null;
  const title = STEPS[index]?.title ?? '';

  return (
    <div className="mx-auto max-w-4xl">
      <PageHeader title="Set up MasterRL" description="A few steps before you start." />
      <div className="grid gap-6 md:grid-cols-[14rem_1fr]">
        <Stepper
          label="Setup steps"
          steps={STEPS.map((s, i) => ({
            id: s.id,
            title: s.title,
            status: i === index ? 'current' : i <= reached ? 'complete' : 'todo',
          }))}
          onSelect={(id) => {
            go(stepFrom(id));
          }}
        />
        <section aria-labelledby="onboarding-step">
          <h2
            id="onboarding-step"
            ref={heading}
            tabIndex={-1}
            className="mb-4 text-xl font-semibold outline-none"
          >
            Step {index + 1} of {STEPS.length}: {title}
          </h2>
          {step === 'welcome' && <WelcomeStep onNext={next} />}
          {step === 'sign_in' && <SignInStep onBack={back} onNext={next} />}
          {step === 'restore' && <RestoreStep onBack={back} onNext={next} />}
          {step === 'check' && (
            <CheckStep
              onBack={back}
              onNext={() => void finish().catch(() => undefined)}
              nextLabel="Start using MasterRL"
            />
          )}
          {failed && (
            <PlainError
              className="mt-4"
              code={failed.code}
              detail={failed.detail ?? 'Your progress could not be saved.'}
              traceId={failed.traceId}
            />
          )}
          {step === 'check' && save.isPending && (
            <p role="status" className="mt-2 text-fg-muted">
              Saving…
            </p>
          )}
        </section>
      </div>
    </div>
  );
}
