import { describeError } from '@mrl/api-client';
import { AsyncView } from '@mrl/ui';
import { SignInButtons, useMe } from '../account/sign-in';
import { StepFooter, type StepProps } from './StepFooter';

/** Sign in once; after that the app also starts without a network. */
export function SignInStep({ onBack, onNext }: StepProps) {
  const me = useMe();
  return (
    <AsyncView
      noun="your sign-in"
      status={me.status}
      data={me.data}
      error={describeError(me.error)}
      onRetry={() => void me.refetch()}
      empty={{ title: '', description: '' }}
    >
      {(data) => {
        if (data.sign_in === 'active') {
          const who = data.user
            ? `${data.user.display_name}${data.user.email ? ` (${data.user.email})` : ''}`
            : 'your account';
          return (
            <>
              <p>You are signed in as {who}.</p>
              <p className="mt-2 text-fg-muted">
                From now on the app starts even without a network.
              </p>
              <StepFooter onBack={onBack} onNext={onNext} />
            </>
          );
        }
        if (data.sign_in === 'not_configured') {
          return (
            <>
              <p className="max-w-prose">
                Sign-in is not set up for this copy of MasterRL. Everything on this computer works;
                saving your setup and joining teams need sign-in.
              </p>
              <StepFooter onBack={onBack} onNext={onNext} />
            </>
          );
        }
        return (
          <>
            <p className="mb-4 max-w-prose">
              {data.sign_in === 'expired'
                ? 'Your sign-in has expired. Sign in again to keep your setup in sync.'
                : 'Sign in to save your setup, bring it to another computer and join teams. Your browser opens to finish.'}
            </p>
            <SignInButtons onSignedIn={onNext} />
            <p className="mt-4 text-sm text-fg-muted">
              You can also skip this and sign in later from Account.
            </p>
            <StepFooter onBack={onBack} onNext={onNext} nextLabel="Skip for now" skip />
          </>
        );
      }}
    </AsyncView>
  );
}
