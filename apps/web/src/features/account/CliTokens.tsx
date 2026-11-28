import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useId, useState } from 'react';
import { describeError, ProblemError } from '@mrl/api-client';
import { AsyncView, Button, ConfirmDialog, formatDateTime, Icon, PlainError } from '@mrl/ui';
import { usePrefs } from '../../app/prefs';
import { mrl, type Schemas } from '../../app/session';

const TOKENS_KEY = ['auth', 'tokens'] as const;
type Token = Schemas['TokenInfo'];

function NewTokenForm({ onCreated }: { onCreated: (t: Schemas['NewToken']) => void }) {
  const queryClient = useQueryClient();
  const { mode } = usePrefs();
  const [name, setName] = useState('');
  const [days, setDays] = useState('90');
  const [scopes, setScopes] = useState('*');
  const nameId = useId();
  const daysId = useId();
  const scopesId = useId();
  const create = useMutation({
    mutationFn: () =>
      mrl().POST('/api/v1/auth/tokens', {
        body: {
          name: name.trim(),
          days: Number(days),
          scopes: scopes
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean),
        },
      }),
    onSuccess: async (made) => {
      setName('');
      await queryClient.invalidateQueries({ queryKey: TOKENS_KEY });
      onCreated(made);
    },
  });
  const error = create.isError ? describeError(create.error) : null;
  const fields = create.error instanceof ProblemError ? create.error.fieldErrors() : {};
  const daysNumber = Number(days);
  const valid =
    name.trim() !== '' && Number.isInteger(daysNumber) && daysNumber >= 1 && daysNumber <= 365;

  return (
    <form
      className="flex max-w-md flex-col gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (valid) create.mutate();
      }}
    >
      <label htmlFor={nameId} className="font-medium">
        Name
      </label>
      <input
        id={nameId}
        className="rounded-md border border-border bg-bg px-3 py-2"
        placeholder="Laptop scripts"
        maxLength={100}
        value={name}
        aria-invalid={fields['/name'] ? true : undefined}
        onChange={(e) => {
          setName(e.target.value);
        }}
      />
      <label htmlFor={daysId} className="font-medium">
        Days until it stops working (1–365)
      </label>
      <input
        id={daysId}
        type="number"
        min={1}
        max={365}
        className="w-32 rounded-md border border-border bg-bg px-3 py-2"
        value={days}
        onChange={(e) => {
          setDays(e.target.value);
        }}
      />
      {mode === 'advanced' && (
        <>
          <label htmlFor={scopesId} className="font-medium">
            Access (comma-separated, <code>*</code> for everything)
          </label>
          <input
            id={scopesId}
            className="rounded-md border border-border bg-bg px-3 py-2"
            value={scopes}
            onChange={(e) => {
              setScopes(e.target.value);
            }}
          />
        </>
      )}
      <div>
        <Button type="submit" variant="primary" disabled={!valid || create.isPending}>
          {create.isPending ? 'Making…' : 'Make a token'}
        </Button>
      </div>
      {error && (
        <PlainError
          headingLevel={3}
          code={error.code}
          detail={Object.values(fields)[0] ?? error.detail}
          traceId={error.traceId}
        />
      )}
    </form>
  );
}

function Shown({ token, onDone }: { token: Schemas['NewToken']; onDone: () => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex max-w-xl flex-col gap-2 rounded-md border border-accent p-4">
      <p className="font-semibold">Your new token</p>
      <p>Copy it now: it is shown only once.</p>
      <code className="rounded bg-surface p-2 text-sm break-all select-all">{token.token}</code>
      <div className="flex gap-2">
        <Button
          size="sm"
          onClick={() =>
            void navigator.clipboard.writeText(token.token).then(() => {
              setCopied(true);
            })
          }
        >
          <Icon name="copy" />
          {copied ? 'Copied' : 'Copy'}
        </Button>
        <Button size="sm" variant="primary" onClick={onDone}>
          Done
        </Button>
      </div>
    </div>
  );
}

/** Your access tokens: list, make one, remove one. */
export function CliTokens() {
  const queryClient = useQueryClient();
  const [made, setMade] = useState<Schemas['NewToken'] | null>(null);
  const [revoking, setRevoking] = useState<Token | null>(null);
  const tokens = useQuery({
    queryKey: TOKENS_KEY,
    queryFn: () => mrl().GET('/api/v1/auth/tokens', { query: { limit: '200' } }),
  });
  const revoke = useMutation({
    mutationFn: (id: string) => mrl().DELETE('/api/v1/auth/tokens/{id}', { path: { id } }),
    onSettled: () => queryClient.invalidateQueries({ queryKey: TOKENS_KEY }),
  });
  const revokeError = revoke.isError ? describeError(revoke.error) : null;

  return (
    <div className="flex flex-col gap-4">
      {made ? (
        <Shown
          token={made}
          onDone={() => {
            setMade(null);
          }}
        />
      ) : (
        <NewTokenForm onCreated={setMade} />
      )}
      {revokeError && (
        <PlainError
          headingLevel={3}
          code={revokeError.code}
          detail={revokeError.detail}
          traceId={revokeError.traceId}
        />
      )}
      <AsyncView
        noun="your tokens"
        status={tokens.status}
        data={tokens.data}
        error={describeError(tokens.error)}
        onRetry={() => void tokens.refetch()}
        isEmpty={(page) => page.items.length === 0}
        empty={{
          icon: 'lock',
          title: 'No tokens',
          description: 'Make one above for the mrl command.',
        }}
      >
        {(page) => (
          <ul aria-label="Your tokens" className="flex flex-col divide-y divide-border">
            {page.items.map((t) => (
              <li key={t.id} className="flex flex-wrap items-center gap-3 py-2">
                <span className="min-w-0 flex-1">
                  <span className="font-medium">{t.name}</span>{' '}
                  <code className="text-sm text-fg-muted">{t.prefix}…</code>
                  <span className="block text-sm text-fg-muted">
                    Works until {formatDateTime(new Date(t.expires_at))} ·{' '}
                    {t.last_used_at
                      ? `last used ${formatDateTime(new Date(t.last_used_at))}`
                      : 'never used'}
                  </span>
                </span>
                <Button
                  size="sm"
                  variant="danger"
                  aria-label={`Remove ${t.name}`}
                  disabled={revoke.isPending}
                  onClick={() => {
                    setRevoking(t);
                  }}
                >
                  Remove
                </Button>
              </li>
            ))}
          </ul>
        )}
      </AsyncView>
      <ConfirmDialog
        open={revoking !== null}
        title="Remove this token?"
        description={`Scripts using “${revoking?.name ?? ''}” stop working right away. This can’t be undone.`}
        actionLabel="Remove token"
        onCancel={() => {
          setRevoking(null);
        }}
        onConfirm={() => {
          if (revoking) revoke.mutate(revoking.id);
          setRevoking(null);
        }}
      />
    </div>
  );
}
