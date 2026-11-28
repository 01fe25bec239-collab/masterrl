import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { describeError } from '@mrl/api-client';
import { Button, PlainError } from '@mrl/ui';
import { getSession, mrl } from '../../app/session';

/** Query key of `GET /api/v1/auth/me`. */
export const ME_KEY = ['auth', 'me'] as const;

/** Who is signed in on this install. */
export function useMe() {
  return useQuery({ queryKey: ME_KEY, queryFn: () => mrl().GET('/api/v1/auth/me') });
}

type Provider = 'google' | 'github';

interface Outcome {
  /** Plain words for the person. */
  message?: string;
  error?: { code?: string; detail?: string; traceId?: string };
}

/**
 * "Continue with Google / GitHub": opens the system browser through the desktop app and waits
 * for it to come back (`masterrl://auth/callback`, or a loopback page on Linux).
 */
export function SignInButtons({ onSignedIn }: { onSignedIn?: () => void }) {
  const queryClient = useQueryClient();
  const bridge = getSession().desktop;
  const [waiting, setWaiting] = useState<Provider | null>(null);
  const [outcome, setOutcome] = useState<Outcome>({});

  if (!bridge) {
    return <p className="text-fg-muted">Sign in from the MasterRL desktop app on your computer.</p>;
  }
  const start = async (provider: Provider) => {
    setWaiting(provider);
    setOutcome({});
    try {
      const result = await bridge.auth.signIn(provider);
      if (result.status === 'signed_in') {
        await queryClient.invalidateQueries({ queryKey: ME_KEY });
        await queryClient.invalidateQueries({ queryKey: ['sync'] });
        onSignedIn?.();
      } else if (result.status === 'cancelled') {
        setOutcome({ message: 'Sign-in was cancelled. You can try again.' });
      } else {
        setOutcome({ error: { code: result.code, detail: result.detail } });
      }
    } catch (e) {
      setOutcome({ error: describeError(e) });
    } finally {
      setWaiting(null);
    }
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap gap-2">
        <Button variant="primary" disabled={waiting !== null} onClick={() => void start('google')}>
          Continue with Google
        </Button>
        <Button variant="primary" disabled={waiting !== null} onClick={() => void start('github')}>
          Continue with GitHub
        </Button>
      </div>
      {waiting && (
        <p role="status" className="flex flex-wrap items-center gap-2">
          Finish signing in in your browser. This window updates when you are done.
          <Button size="sm" onClick={() => void bridge.auth.cancelSignIn()}>
            Cancel
          </Button>
        </p>
      )}
      {outcome.message && <p role="status">{outcome.message}</p>}
      {outcome.error && (
        <PlainError
          headingLevel={3}
          code={outcome.error.code}
          detail={outcome.error.detail}
          traceId={outcome.error.traceId}
        />
      )}
    </div>
  );
}
