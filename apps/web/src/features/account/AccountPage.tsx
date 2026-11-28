import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { describeError } from '@mrl/api-client';
import { AsyncView, Button, ConfirmDialog, KeyValueList, PageHeader, PlainError } from '@mrl/ui';
import { mrl, type Schemas } from '../../app/session';
import { CliTokens } from './CliTokens';
import { SignInButtons, useMe } from './sign-in';

const signInWords: Record<Schemas['SignInState'], string> = {
  active: 'Signed in',
  signed_out: 'Signed out',
  expired: 'Sign-in expired',
  not_configured: 'Sign-in is not set up for this install',
};

const principalWords: Record<string, string> = {
  local_owner: 'Owner of this computer',
  user: 'Person',
  service: 'Service account',
};

function Profile({ me }: { me: Schemas['Me'] }) {
  const queryClient = useQueryClient();
  const [confirm, setConfirm] = useState(false);
  const signOut = useMutation({
    mutationFn: () => mrl().POST('/api/v1/auth/logout'),
    onSuccess: () => queryClient.invalidateQueries(),
  });
  const error = signOut.isError ? describeError(signOut.error) : null;
  return (
    <div className="flex flex-col gap-3">
      <KeyValueList
        items={[
          { key: 'Name', value: me.user?.display_name ?? 'You' },
          ...(me.user?.email ? [{ key: 'Email', value: me.user.email }] : []),
          { key: 'Account', value: principalWords[me.principal_kind] ?? me.principal_kind },
          { key: 'Sign-in', value: signInWords[me.sign_in] },
        ]}
      />
      {me.sign_in === 'active' ? (
        <div>
          <Button
            onClick={() => {
              setConfirm(true);
            }}
          >
            Sign out
          </Button>
        </div>
      ) : me.sign_in !== 'not_configured' ? (
        <SignInButtons />
      ) : null}
      {error && (
        <PlainError
          headingLevel={3}
          code={error.code}
          detail={error.detail}
          traceId={error.traceId}
        />
      )}
      <ConfirmDialog
        open={confirm}
        title="Sign out?"
        description="Everything on this computer keeps working, including training. Syncing your setup and team workspaces pause until you sign in again."
        actionLabel="Sign out"
        danger={false}
        onCancel={() => {
          setConfirm(false);
        }}
        onConfirm={() => {
          setConfirm(false);
          signOut.mutate();
        }}
      />
    </div>
  );
}

/** Your profile, sign-in, and access tokens for the command line. */
export default function AccountPage() {
  const me = useMe();
  return (
    <>
      <PageHeader title="Account" description="Who you are signed in as, and tokens for scripts." />
      <section aria-labelledby="profile-title" className="mb-8">
        <h2 id="profile-title" className="mb-2 text-lg font-semibold">
          Profile
        </h2>
        <AsyncView
          noun="your profile"
          status={me.status}
          data={me.data}
          error={describeError(me.error)}
          onRetry={() => void me.refetch()}
          empty={{ title: '', description: '' }}
        >
          {(data) => <Profile me={data} />}
        </AsyncView>
      </section>
      <section aria-labelledby="tokens-title">
        <h2 id="tokens-title" className="mb-2 text-lg font-semibold">
          Command-line tokens
        </h2>
        <p className="mb-3 max-w-prose text-fg-muted">
          The <code>mrl</code> command and your scripts sign in with a token. Treat it like a
          password; remove it when you no longer use it.
        </p>
        <CliTokens />
      </section>
    </>
  );
}
