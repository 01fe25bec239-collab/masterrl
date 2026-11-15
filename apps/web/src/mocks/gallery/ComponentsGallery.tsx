import { useInfiniteQuery } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import { z } from 'zod';
import { describeError } from '@mrl/api-client';
import {
  Button,
  ConfirmDialog,
  DataTable,
  DiffViewer,
  EmptyState,
  FileUploader,
  FreshnessBadge,
  HashLabel,
  KeyValueList,
  LicenseChip,
  LoadingState,
  MaturityBadge,
  PageHeader,
  PlainError,
  StateChip,
  stateMachines,
  Timeline,
  Wizard,
  type StateMachine,
  type StateOf,
  type UploadTransport,
} from '@mrl/ui';
import { requireApi } from '../../lib/api';
import { jobStates } from '../data';

const pageSchema = z.object({
  items: z.array(
    z.object({
      id: z.string(),
      name: z.string(),
      algorithm: z.string(),
      state: z.enum(jobStates),
      step: z.number(),
      totalSteps: z.number(),
    }),
  ),
  next_cursor: z.string().nullable(),
});

/** Demo times are relative to when this page module loaded. */
const loadedAt = Date.now();

/** In-memory ticket transport: 256 KB parts, every third part fails once to show retry. */
function memoryTransport(): UploadTransport {
  const tickets = new Map<string, Set<number>>();
  const failedOnce = new Set<string>();
  const wait = (ms: number, signal: AbortSignal) =>
    new Promise<void>((resolve, reject) => {
      const t = setTimeout(resolve, ms);
      signal.addEventListener('abort', () => {
        clearTimeout(t);
        reject(new DOMException('Stopped', 'AbortError'));
      });
    });
  return {
    open(file) {
      const done = tickets.get(file.resumeKey) ?? new Set<number>();
      tickets.set(file.resumeKey, done);
      return Promise.resolve({
        uploadId: file.resumeKey,
        partSize: 256 * 1024,
        completedParts: [...done].map((n) => ({ partNumber: n, etag: `etag-${n}` })),
      });
    },
    async putPart(ticket, partNumber, _body, signal) {
      await wait(150, signal);
      const key = `${ticket.uploadId}:${partNumber}`;
      if (partNumber % 3 === 0 && !failedOnce.has(key)) {
        failedOnce.add(key);
        throw new Error('The connection dropped.');
      }
      tickets.get(ticket.uploadId)?.add(partNumber);
      return { partNumber, etag: `etag-${partNumber}` };
    },
    complete: () => Promise.resolve(),
  };
}

function Demo({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mb-8" aria-labelledby={title}>
      <h2 id={title} className="mb-3 text-lg font-semibold">
        {title}
      </h2>
      {children}
    </section>
  );
}

function RunsTable() {
  const query = useInfiniteQuery({
    queryKey: ['demo-runs'],
    initialPageParam: '0',
    queryFn: ({ pageParam }) =>
      requireApi().get(
        `/api/v1/projects/prj_support_bot/runs?limit=50&cursor=${pageParam}`,
        pageSchema,
      ),
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
  const rows = query.data?.pages.flatMap((p) => p.items) ?? [];
  return (
    <DataTable
      label="Runs"
      status={query.status}
      error={describeError(query.error)}
      onRetry={() => void query.refetch()}
      rows={rows}
      getRowId={(r) => r.id}
      hasMore={query.hasNextPage}
      loadingMore={query.isFetchingNextPage}
      onLoadMore={() => void query.fetchNextPage()}
      empty={{ title: 'No runs yet', description: 'Start a training run to see it here.' }}
      height={320}
      columns={[
        { id: 'name', header: 'Name', cell: (r) => r.name },
        { id: 'algorithm', header: 'Method', cell: (r) => r.algorithm.toUpperCase() },
        { id: 'state', header: 'State', cell: (r) => <StateChip machine="job" state={r.state} /> },
        {
          id: 'progress',
          header: 'Progress',
          align: 'right',
          cell: (r) =>
            `${r.step.toLocaleString('en-US')} / ${r.totalSteps.toLocaleString('en-US')}`,
        },
      ]}
    />
  );
}

export default function ComponentsGallery() {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [deleted, setDeleted] = useState(false);
  const [wizardDone, setWizardDone] = useState(false);
  const [name, setName] = useState('');
  const [transport] = useState(memoryTransport);

  return (
    <>
      <PageHeader
        title="Components"
        description="Every building block, working against mock data. Shown only in development."
        meta={<MaturityBadge level="beta" />}
      />

      <Demo title="Data table (virtualized, cursor pages)">
        <RunsTable />
      </Demo>

      <Demo title="State chips">
        <div className="flex flex-col gap-3">
          {(Object.keys(stateMachines) as StateMachine[]).map((machine) => (
            <div key={machine} className="flex flex-wrap items-center gap-2">
              <span className="w-44 text-sm text-fg-muted">{machine}</span>
              {(Object.keys(stateMachines[machine]) as StateOf<typeof machine>[]).map((state) => (
                <StateChip key={state} machine={machine} state={state} />
              ))}
            </div>
          ))}
        </div>
      </Demo>

      <Demo title="Badges and labels">
        <div className="flex flex-wrap items-center gap-2">
          <FreshnessBadge freshness="live" />
          <FreshnessBadge freshness="sampled" />
          <FreshnessBadge freshness="delayed" asOf={new Date(loadedAt - 5 * 60_000)} />
          <FreshnessBadge freshness="stale" asOf={new Date(loadedAt - 90 * 60_000)} />
          <MaturityBadge level="stable" />
          <MaturityBadge level="experimental" />
          <LicenseChip license="Apache-2.0" kind="permissive" />
          <LicenseChip license="Llama 3 Community" kind="restricted" />
          <LicenseChip kind="unknown" />
          <HashLabel
            hash="sha256:9f2c4e1ab37d55c0e8a1f0b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f6"
            label="checkpoint hash"
          />
        </div>
      </Demo>

      <Demo title="Key-value list and timeline">
        <div className="grid gap-6 md:grid-cols-2">
          <KeyValueList
            items={[
              { key: 'Base model', value: 'Qwen2.5-0.5B-Instruct' },
              { key: 'Method', value: 'Supervised fine-tuning (LoRA)' },
              { key: 'State', value: <StateChip machine="job" state="RUNNING" /> },
              {
                key: 'Config hash',
                value: <HashLabel hash="a1b2c3d4e5f60718" label="config hash" />,
              },
            ]}
          />
          <Timeline
            items={[
              {
                id: '1',
                at: new Date(loadedAt - 3_600_000),
                title: 'Run created',
                icon: 'circle',
              },
              {
                id: '2',
                at: new Date(loadedAt - 3_300_000),
                title: 'Started on GPU 0',
                icon: 'play',
              },
              {
                id: '3',
                at: new Date(loadedAt - 1_200_000),
                title: 'Attempt 1 ran out of GPU memory',
                detail: 'Retrying from the last saved checkpoint with a smaller batch.',
                problem: true,
              },
              {
                id: '4',
                at: new Date(loadedAt - 600_000),
                title: 'Checkpoint saved',
                icon: 'check',
              },
            ]}
          />
        </div>
      </Demo>

      <Demo title="Diff">
        <DiffViewer
          beforeLabel="Version 3"
          afterLabel="Version 4"
          before={'learning_rate: 2.0e-5\nnum_train_epochs: 3\nlora:\n  r: 16\n  alpha: 32'}
          after={
            'learning_rate: 1.0e-5\nnum_train_epochs: 3\nlora:\n  r: 32\n  alpha: 32\n  dropout: 0.05'
          }
        />
      </Demo>

      <Demo title="Typed confirmation">
        {deleted ? (
          <p>Deleted “support-bot-v2”. (Demo only — nothing was removed.)</p>
        ) : (
          <Button
            variant="danger"
            onClick={() => {
              setConfirmOpen(true);
            }}
          >
            Delete run…
          </Button>
        )}
        <ConfirmDialog
          open={confirmOpen}
          title="Delete this run?"
          description="Its checkpoints and logs are removed too. This can’t be undone."
          confirmPhrase="support-bot-v2"
          actionLabel="Delete run"
          onCancel={() => {
            setConfirmOpen(false);
          }}
          onConfirm={() => {
            setConfirmOpen(false);
            setDeleted(true);
          }}
        />
      </Demo>

      <Demo title="Wizard">
        {wizardDone ? (
          <p>All steps done for “{name}”.</p>
        ) : (
          <Wizard
            title="New project"
            onFinish={() => {
              setWizardDone(true);
            }}
            steps={[
              {
                id: 'name',
                title: 'Name it',
                validate: () => (name.trim() ? null : 'Give the project a name first.'),
                render: () => (
                  <label className="flex flex-col gap-1">
                    Project name
                    <input
                      className="rounded-md border border-border bg-bg px-3 py-2"
                      value={name}
                      onChange={(e) => {
                        setName(e.target.value);
                      }}
                    />
                  </label>
                ),
              },
              {
                id: 'data',
                title: 'Pick data',
                render: () => <p>Data choices come from the Datasets feature.</p>,
              },
              { id: 'review', title: 'Review', render: () => <p>Everything looks ready.</p> },
            ]}
          />
        )}
      </Demo>

      <Demo title="File upload (resumable parts)">
        <FileUploader
          transport={transport}
          hint="Any file works here. Every third part fails once to show retry."
        />
      </Demo>

      <Demo title="Empty, loading and error states">
        <div className="grid gap-4 md:grid-cols-3">
          <EmptyState
            title="No datasets yet"
            description="Upload a JSONL file or import one from Hugging Face."
          />
          <LoadingState label="Loading datasets" />
          <PlainError code="OOM/OOM_GPU" traceId="tr_demo_123" onRetry={() => undefined} />
        </div>
      </Demo>
    </>
  );
}
