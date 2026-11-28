import { Button } from '@mrl/ui';

/** What each onboarding step gets: how to move on. */
export interface StepProps {
  onBack?: () => void;
  onNext?: () => void;
  nextLabel?: string;
}

/** Back and Next (or Skip) under a step. */
export function StepFooter({
  onBack,
  onNext,
  nextLabel = 'Next',
  skip = false,
}: StepProps & { skip?: boolean }) {
  return (
    <div className="mt-6 flex gap-2">
      {onBack && <Button onClick={onBack}>Back</Button>}
      {onNext && (
        <Button variant={skip ? 'secondary' : 'primary'} onClick={onNext}>
          {nextLabel}
        </Button>
      )}
    </div>
  );
}
