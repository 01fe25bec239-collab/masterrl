import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { describeError } from '@mrl/api-client';
import { AsyncView, formatDateTime } from '@mrl/ui';
import { mrl } from '../../app/session';
import { RestoreForm } from '../settings/RestoreForm';
import { StepFooter, type StepProps } from './StepFooter';

/** Query key of `GET /api/v1/sync/status`. */
export const SYNC_STATUS_KEY = ['sync', 'status'] as const;

/** Optional: bring back the setup saved from another computer. */
export function RestoreStep({ onBack, onNext }: StepProps) {
  const [restored, setRestored] = useState(false);
  const status = useQuery({
    queryKey: SYNC_STATUS_KEY,
    queryFn: () => mrl().GET('/api/v1/sync/status'),
  });
  return (
    <AsyncView
      noun="your saved setup"
      status={status.status}
      data={status.data}
      error={describeError(status.error)}
      onRetry={() => void status.refetch()}
      empty={{ title: '', description: '' }}
    >
      {(data) => {
        const newest = data.copies?.[0];
        if (data.sign_in !== 'active') {
          return (
            <>
              <p className="max-w-prose">
                Sign in to bring back a setup you saved on another computer. You can do this later
                in Settings.
              </p>
              <StepFooter onBack={onBack} onNext={onNext} />
            </>
          );
        }
        if (!newest) {
          return (
            <>
              <p className="max-w-prose">
                {data.copies_unavailable ??
                  'There is no saved setup for your account yet. You can turn on sync in Settings.'}
              </p>
              <StepFooter onBack={onBack} onNext={onNext} />
            </>
          );
        }
        return (
          <>
            <p className="mb-4 max-w-prose">
              We found a setup you saved on {formatDateTime(new Date(newest.at))}. It has your
              machines, teams and app settings. Enter your sync passphrase to bring it to this
              computer.
            </p>
            <RestoreForm
              onRestored={() => {
                setRestored(true);
              }}
            />
            <StepFooter
              onBack={onBack}
              onNext={onNext}
              nextLabel={restored ? 'Next' : 'Skip'}
              skip={!restored}
            />
          </>
        );
      }}
    </AsyncView>
  );
}
