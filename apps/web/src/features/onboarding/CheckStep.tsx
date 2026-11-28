import { useQuery } from '@tanstack/react-query';
import { describeError } from '@mrl/api-client';
import { AsyncView, Button, formatRange, Icon } from '@mrl/ui';
import { mrl, type Schemas } from '../../app/session';
import { StepFooter, type StepProps } from './StepFooter';

/** Query key of `POST /api/v1/desktop/doctor` (it only reads this computer). */
export const DOCTOR_KEY = ['desktop', 'doctor'] as const;

const gb = (low: number, high: number) => formatRange({ min: low, max: high }, 'GB');

/** "This computer can … / It can't … / To unlock more, install …" from the Environment Doctor. */
export function DoctorReport({ report }: { report: Schemas['DesktopDoctorReport'] }) {
  const problems = report.checks.filter((c) => c.outcome === 'warn' || c.outcome === 'fail');
  return (
    <div className="flex flex-col gap-4">
      {report.fits && <p>{report.fits}</p>}
      <section aria-labelledby="doctor-can">
        <h3 id="doctor-can" className="font-semibold">
          This computer can
        </h3>
        {report.can.length === 0 ? (
          <p className="text-fg-muted">Nothing yet. See what to install below.</p>
        ) : (
          <ul className="ml-5 list-disc">
            {report.can.map((t) => (
              <li key={t.task}>
                {t.name}
                {t.download_first_gb_high ? (
                  <span className="text-fg-muted">
                    {' '}
                    (first downloads {gb(t.download_first_gb_low ?? 0, t.download_first_gb_high)})
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>
      {report.cannot.length > 0 && (
        <section aria-labelledby="doctor-cannot">
          <h3 id="doctor-cannot" className="font-semibold">
            It can’t
          </h3>
          <ul className="ml-5 list-disc">
            {report.cannot.map((t) => (
              <li key={t.task}>
                {t.name}
                {t.blocked_by && <span className="text-fg-muted">: {t.blocked_by}</span>}
              </li>
            ))}
          </ul>
        </section>
      )}
      {report.to_install.length > 0 && (
        <section aria-labelledby="doctor-install">
          <h3 id="doctor-install" className="font-semibold">
            To unlock more, install
          </h3>
          <ul className="ml-5 list-disc">
            {report.to_install.map((c) => (
              <li key={c.id}>
                {c.name}: {c.summary}{' '}
                <span className="text-fg-muted">({gb(c.download_gb_low, c.download_gb_high)})</span>
              </li>
            ))}
          </ul>
        </section>
      )}
      {problems.length > 0 && (
        <section aria-labelledby="doctor-problems">
          <h3 id="doctor-problems" className="font-semibold">
            Worth a look
          </h3>
          <ul className="flex flex-col gap-1">
            {problems.map((c) => (
              <li key={c.check} className="flex items-start gap-2">
                <Icon
                  name="alert"
                  label={c.outcome === 'fail' ? 'Problem' : 'Warning'}
                  className={c.outcome === 'fail' ? 'text-danger' : 'text-fg-muted'}
                />
                <span>
                  {c.summary}
                  {c.code && <code className="ml-2 text-sm text-fg-muted">{c.code}</code>}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}

/** Runs the Environment Doctor through the core and shows what this computer can do. */
export function CheckStep({ onBack, onNext, nextLabel }: StepProps) {
  const report = useQuery({
    queryKey: DOCTOR_KEY,
    queryFn: () => mrl().POST('/api/v1/desktop/doctor'),
    staleTime: Infinity,
  });
  return (
    <>
      <AsyncView
        noun="what this computer can do"
        status={report.status}
        data={report.data}
        error={describeError(report.error)}
        onRetry={() => void report.refetch()}
        empty={{ title: '', description: '' }}
      >
        {(data) => <DoctorReport report={data} />}
      </AsyncView>
      <div className="mt-4">
        <Button size="sm" disabled={report.isFetching} onClick={() => void report.refetch()}>
          {report.isFetching ? 'Checking…' : 'Check again'}
        </Button>
      </div>
      <StepFooter onBack={onBack} onNext={onNext} nextLabel={nextLabel} />
    </>
  );
}
