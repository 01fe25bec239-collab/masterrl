import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useId, useState } from 'react';
import { describeError, ProblemError } from '@mrl/api-client';
import { Button, PlainError } from '@mrl/ui';
import { mrl, type Schemas } from '../../app/session';

/** Plain summary of what a restore brought back. */
export function restoredSummary(r: Schemas['Restored']): string {
  const machines = r.profile.machines?.length ?? 0;
  const teams = r.profile.team_memberships?.length ?? 0;
  const n = (count: number, one: string, many: string) =>
    `${String(count)} ${count === 1 ? one : many}`;
  return `Your setup is back: ${n(machines, 'machine', 'machines')} and ${n(teams, 'team', 'teams')}.`;
}

export interface RestoreFormProps {
  /** An older copy; the newest when absent. */
  version?: number;
  onRestored?: (restored: Schemas['Restored']) => void;
}

/** Opens a saved setup with the sync passphrase, or the recovery key. */
export function RestoreForm({ version, onRestored }: RestoreFormProps) {
  const queryClient = useQueryClient();
  const [useKey, setUseKey] = useState(false);
  const [secret, setSecret] = useState('');
  const inputId = useId();
  const restore = useMutation({
    mutationFn: () =>
      mrl().POST('/api/v1/sync/restore', {
        body: useKey ? { recovery_key: secret.trim(), version } : { passphrase: secret, version },
      }),
    onSuccess: async (restored) => {
      setSecret('');
      await queryClient.invalidateQueries({ queryKey: ['sync'] });
      await queryClient.invalidateQueries({ queryKey: ['workspaces'] });
      onRestored?.(restored);
    },
  });
  const error = restore.isError ? describeError(restore.error) : null;
  const fieldError =
    restore.error instanceof ProblemError
      ? Object.values(restore.error.fieldErrors())[0]
      : undefined;

  return (
    <form
      className="flex max-w-md flex-col gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (secret.trim()) restore.mutate();
      }}
    >
      <label htmlFor={inputId} className="font-medium">
        {useKey ? 'Recovery key' : 'Sync passphrase'}
      </label>
      <input
        id={inputId}
        type="password"
        autoComplete="off"
        spellCheck={false}
        className="rounded-md border border-border bg-bg px-3 py-2"
        value={secret}
        aria-invalid={fieldError ? true : undefined}
        onChange={(e) => {
          setSecret(e.target.value);
        }}
      />
      <div className="flex flex-wrap gap-2">
        <Button type="submit" variant="primary" disabled={!secret.trim() || restore.isPending}>
          {restore.isPending ? 'Restoring…' : 'Restore'}
        </Button>
        <Button
          variant="ghost"
          onClick={() => {
            setUseKey(!useKey);
            setSecret('');
            restore.reset();
          }}
        >
          {useKey ? 'Use my passphrase instead' : 'I forgot it: use my recovery key'}
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
      {restore.data && <p role="status">{restoredSummary(restore.data)}</p>}
    </form>
  );
}
