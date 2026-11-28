import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useParams } from '@tanstack/react-router';
import { useState } from 'react';
import { describeError } from '@mrl/api-client';
import { AsyncView, Button, Icon, PageHeader } from '@mrl/ui';
import { getSession } from '../../app/session';
import { DesktopOnly } from './DesktopOnly';
import { CRASH_LIST_KEY } from './PrivacySettings';

/** "Something went wrong": a local crash report, ready to copy or to open as a GitHub issue. */
export default function CrashReportPage() {
  const { id } = useParams({ from: '/settings/crash/$id' });
  const bridge = getSession().desktop;
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [said, setSaid] = useState('');
  const report = useQuery({
    queryKey: ['crash', 'report', id],
    queryFn: () => {
      if (!bridge) throw new Error('Crash reports are kept by the desktop app.');
      return bridge.crash.read(id);
    },
    enabled: bridge !== null,
    retry: false,
  });
  const remove = useMutation({
    mutationFn: async () => {
      await bridge?.crash.delete(id);
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: CRASH_LIST_KEY });
      await navigate({ to: '/settings', hash: 'privacy' });
    },
  });

  const header = (
    <PageHeader
      title="Something went wrong"
      description="MasterRL wrote a crash report. Nothing was sent: it stays on this computer until you share it."
    />
  );
  if (!bridge) {
    return (
      <>
        {header}
        <DesktopOnly what="Crash reports" />
      </>
    );
  }
  return (
    <>
      {header}
      <p className="mb-4 max-w-prose">
        The report says where the app stopped. It has no prompts, data, file paths or keys. Read it
        before you share it.
      </p>
      <AsyncView
        noun="the crash report"
        status={report.status}
        data={report.data}
        error={describeError(report.error)}
        onRetry={() => void report.refetch()}
        empty={{ title: '', description: '' }}
      >
        {(text) => (
          <>
            <div className="mb-4 flex flex-wrap gap-2">
              <Button
                variant="primary"
                onClick={() =>
                  void bridge.crash.copy(id).then((ok) => {
                    setSaid(
                      ok
                        ? 'Copied. Paste it wherever you ask for help.'
                        : 'It could not be copied.',
                    );
                  })
                }
              >
                <Icon name="copy" />
                Copy report
              </Button>
              <Button
                onClick={() =>
                  void bridge.crash.openIssue(id).then((ok) => {
                    setSaid(
                      ok
                        ? 'GitHub opened in your browser with the report filled in. Nothing is sent until you submit it there.'
                        : 'GitHub could not be opened. Copy the report instead.',
                    );
                  })
                }
              >
                Open a GitHub issue
              </Button>
              <Button
                variant="danger"
                disabled={remove.isPending}
                onClick={() => {
                  remove.mutate();
                }}
              >
                Delete report
              </Button>
            </div>
            <p role="status" className="mb-2 min-h-6">
              {said}
            </p>
            <textarea
              readOnly
              aria-label="Crash report"
              rows={18}
              spellCheck={false}
              className="w-full rounded-md border border-border bg-surface p-3 font-mono text-sm"
              value={text}
            />
          </>
        )}
      </AsyncView>
      <p className="mt-6">
        <Link to="/settings" hash="privacy" className="text-accent underline">
          All crash reports
        </Link>
      </p>
    </>
  );
}
