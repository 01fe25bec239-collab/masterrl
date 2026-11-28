import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useId, useState } from 'react';
import { describeError, ProblemError } from '@mrl/api-client';
import { AsyncView, Button, formatDateTime, Icon, PlainError } from '@mrl/ui';
import { mrl, type Schemas } from '../../app/session';
import { SYNC_STATUS_KEY } from '../onboarding/RestoreStep';
import { RestoreForm } from './RestoreForm';

const MIN_PASSPHRASE = 12;

function PassphraseForm({ onDone, cta }: { onDone: (key: string) => void; cta: string }) {
  const queryClient = useQueryClient();
  const [first, setFirst] = useState('');
  const [second, setSecond] = useState('');
  const firstId = useId();
  const secondId = useId();
  const hintId = useId();
  const setup = useMutation({
    mutationFn: () => mrl().POST('/api/v1/sync/setup', { body: { passphrase: first } }),
    onSuccess: async (done) => {
      setFirst('');
      setSecond('');
      await queryClient.invalidateQueries({ queryKey: SYNC_STATUS_KEY });
      onDone(done.recovery_key);
    },
  });
  const tooShort = Array.from(first).length < MIN_PASSPHRASE;
  const mismatch = second !== '' && first !== second;
  const error = setup.isError ? describeError(setup.error) : null;
  const fieldError =
    setup.error instanceof ProblemError ? Object.values(setup.error.fieldErrors())[0] : undefined;

  return (
    <form
      className="flex max-w-md flex-col gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (!tooShort && first === second) setup.mutate();
      }}
    >
      <label htmlFor={firstId} className="font-medium">
        Sync passphrase
      </label>
      <input
        id={firstId}
        type="password"
        autoComplete="new-password"
        aria-describedby={hintId}
        className="rounded-md border border-border bg-bg px-3 py-2"
        value={first}
        onChange={(e) => {
          setFirst(e.target.value);
        }}
      />
      <p id={hintId} className="text-sm text-fg-muted">
        At least 12 characters; a few unrelated words work well. Your setup is locked with it before
        it leaves this computer, so nobody else can read it, not even MasterRL.
      </p>
      <label htmlFor={secondId} className="font-medium">
        Type it again
      </label>
      <input
        id={secondId}
        type="password"
        autoComplete="new-password"
        className="rounded-md border border-border bg-bg px-3 py-2"
        value={second}
        aria-invalid={mismatch ? true : undefined}
        onChange={(e) => {
          setSecond(e.target.value);
        }}
      />
      {mismatch && <p className="text-danger">The two passphrases are not the same.</p>}
      <div>
        <Button
          type="submit"
          variant="primary"
          disabled={tooShort || first !== second || setup.isPending}
        >
          {setup.isPending ? 'Saving…' : cta}
        </Button>
      </div>
      {error && (
        <PlainError
          headingLevel={3}
          code={error.code}
          detail={fieldError ?? error.detail}
          traceId={error.traceId}
        />
      )}
    </form>
  );
}

function RecoveryKey({ value, onSaved }: { value: string; onSaved: () => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex max-w-xl flex-col gap-2 rounded-md border border-accent p-4">
      <p className="font-semibold">Your recovery key</p>
      <p>
        If you forget your passphrase, this key opens your saved setup. It is shown only now: write
        it down or keep it in a password manager.
      </p>
      <code className="rounded bg-surface p-2 text-sm break-all select-all">{value}</code>
      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          onClick={() =>
            void navigator.clipboard.writeText(value).then(() => {
              setCopied(true);
            })
          }
        >
          <Icon name="copy" />
          {copied ? 'Copied' : 'Copy'}
        </Button>
        <Button size="sm" variant="primary" onClick={onSaved}>
          I saved it
        </Button>
      </div>
    </div>
  );
}

function Copies({ status }: { status: Schemas['SyncStatus'] }) {
  const [restoring, setRestoring] = useState<number | null>(null);
  if (!status.copies) {
    return status.copies_unavailable ? (
      <p className="text-fg-muted">{status.copies_unavailable}</p>
    ) : null;
  }
  if (status.copies.length === 0) {
    return <p className="text-fg-muted">No copies saved yet.</p>;
  }
  return (
    <div>
      <h3 className="font-semibold">Saved copies</h3>
      <p className="mb-2 text-sm text-fg-muted">The newest five are kept.</p>
      <ul aria-label="Saved copies" className="flex flex-col divide-y divide-border">
        {status.copies.map((c, i) => (
          <li key={c.version} className="py-2">
            <div className="flex flex-wrap items-center gap-3">
              <span className="flex-1">
                {formatDateTime(new Date(c.at))}
                {i === 0 && <span className="ml-2 text-fg-muted">(newest)</span>}
              </span>
              <Button
                size="sm"
                aria-expanded={restoring === c.version}
                onClick={() => {
                  setRestoring(restoring === c.version ? null : c.version);
                }}
              >
                Restore this copy
              </Button>
            </div>
            {restoring === c.version && (
              <div className="mt-2">
                <p className="mb-2 text-sm">
                  Your machines, teams and app settings on this computer are replaced by this copy.
                </p>
                <RestoreForm version={c.version} />
              </div>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}

/** Setup sync: turn it on, save now, change the passphrase, restore an older copy. */
export default function SyncSettings() {
  const queryClient = useQueryClient();
  const [recoveryKey, setRecoveryKey] = useState<string | null>(null);
  const [changing, setChanging] = useState(false);
  // One id per save, reused when the person tries the same save again.
  const [opId, setOpId] = useState<string | null>(null);
  const status = useQuery({
    queryKey: SYNC_STATUS_KEY,
    queryFn: () => mrl().GET('/api/v1/sync/status'),
  });
  const push = useMutation({
    mutationFn: (op: string) => mrl().POST('/api/v1/sync/push', { body: { op_id: op } }),
    onSuccess: async () => {
      setOpId(null);
      await queryClient.invalidateQueries({ queryKey: SYNC_STATUS_KEY });
    },
  });
  const pushError = push.isError ? describeError(push.error) : null;

  if (recoveryKey) {
    return (
      <RecoveryKey
        value={recoveryKey}
        onSaved={() => {
          setRecoveryKey(null);
          setChanging(false);
        }}
      />
    );
  }
  return (
    <AsyncView
      noun="sync"
      status={status.status}
      data={status.data}
      error={describeError(status.error)}
      onRetry={() => void status.refetch()}
      empty={{ title: '', description: '' }}
    >
      {(data) => {
        if (data.sign_in === 'not_configured') {
          return <p>Sign-in is not set up for this copy of MasterRL, so sync is off.</p>;
        }
        if (data.sign_in !== 'active') {
          return (
            <p>
              <Link to="/account" className="text-accent underline">
                Sign in
              </Link>{' '}
              to save your setup and bring it to another computer.
            </p>
          );
        }
        if (!data.set_up) {
          return (
            <div className="flex flex-col gap-3">
              <p className="max-w-prose">
                Sync saves your machines, teams and app settings, locked with a passphrase only you
                know. Datasets, models and prompts are never synced.
              </p>
              <PassphraseForm cta="Turn on sync" onDone={setRecoveryKey} />
            </div>
          );
        }
        const save = () => {
          const op = opId ?? crypto.randomUUID();
          setOpId(op);
          push.mutate(op);
        };
        return (
          <div className="flex flex-col gap-4">
            <p>
              Sync is on.{' '}
              {data.last_push_at
                ? `Last saved ${formatDateTime(new Date(data.last_push_at))}.`
                : 'Nothing saved yet.'}
            </p>
            <div className="flex flex-wrap gap-2">
              <Button variant="primary" disabled={push.isPending} onClick={save}>
                {push.isPending ? 'Saving…' : 'Save my setup now'}
              </Button>
              <Button
                aria-expanded={changing}
                onClick={() => {
                  setChanging(!changing);
                }}
              >
                Change passphrase
              </Button>
            </div>
            {push.isSuccess && <p role="status">Saved.</p>}
            {pushError && (
              <PlainError
                headingLevel={3}
                code={pushError.code}
                detail={pushError.detail}
                traceId={pushError.traceId}
                onRetry={save}
              />
            )}
            {changing && (
              <div>
                <p className="mb-2 max-w-prose text-sm">
                  You get a new recovery key. Save your setup again afterwards so the newest copy
                  uses the new passphrase.
                </p>
                <PassphraseForm cta="Change passphrase" onDone={setRecoveryKey} />
              </div>
            )}
            <Copies status={data} />
          </div>
        );
      }}
    </AsyncView>
  );
}
