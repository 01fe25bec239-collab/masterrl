import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axe from 'axe-core';
import { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { EventStreamHub } from '../hooks/event-stream';
import { LogBuffer } from '../hooks/log-buffer';
import { EventStreamProvider } from '../hooks/useEventStream';
import type { UploadTransport } from '../lib/uploader';
import { FreshnessBadge, HashLabel, LicenseChip, MaturityBadge } from './Badges';
import { CommandPalette } from './CommandPalette';
import { KeyValueList, Timeline } from './DataDisplay';
import { DataTable } from './DataTable';
import { ConfirmDialog } from './Dialog';
import { DiffViewer } from './DiffViewer';
import { FileUploader } from './FileUploader';
import { AppShell, LeftNav, PageHeader, ScopeSwitcher } from './Layout';
import { LiveLogViewer } from './LiveLogViewer';
import { LiveStatusBanner } from './LiveStatusBanner';
import { StateChip, stateLabel, stateMachines, type StateMachine } from './StateChip';
import { AsyncView, EmptyState, LoadingState, PlainError } from './States';
import { Stepper, Wizard } from './Wizard';

async function serious(container: HTMLElement) {
  const r = await axe.run(container);
  return r.violations
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => v.id);
}

describe('StateChip', () => {
  it.each(Object.keys(stateMachines) as StateMachine[])(
    '%s: every state has an icon and words',
    (machine) => {
      for (const state of Object.keys(stateMachines[machine])) {
        const { container, unmount } = render(
          <StateChip machine={machine} state={state as never} />,
        );
        const chip = container.querySelector(`[data-state="${state}"]`);
        expect(chip?.querySelector('svg')).not.toBeNull();
        expect(chip?.textContent.trim()).toBe(stateLabel(machine, state as never));
        expect(chip?.textContent).not.toBe(state);
        unmount();
      }
    },
  );

  it('covers the canonical state lists', () => {
    expect(Object.keys(stateMachines.job)).toHaveLength(16);
    expect(Object.keys(stateMachines.checkpoint)).toEqual([
      'WRITING',
      'UPLOADED',
      'VERIFYING',
      'COMMITTED',
      'CORRUPT',
      'TOMBSTONED',
    ]);
    expect(Object.keys(stateMachines)).toHaveLength(12);
  });
});

describe('badges', () => {
  it('say what they mean in words', () => {
    render(
      <>
        <FreshnessBadge freshness="delayed" asOf={new Date(Date.now() - 120_000)} />
        <MaturityBadge level="experimental" />
        <LicenseChip license="MIT" kind="permissive" />
        <LicenseChip kind="unknown" />
      </>,
    );
    expect(screen.getByText('Delayed')).toBeDefined();
    expect(screen.getByText('· 2 minutes ago')).toBeDefined();
    expect(screen.getByText('Experimental')).toBeDefined();
    expect(screen.getByText('MIT · Permissive')).toBeDefined();
    expect(screen.getByText('License unknown')).toBeDefined();
  });

  it('HashLabel shows a short hash and copies the full one', async () => {
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, 'writeText');
    render(<HashLabel hash="sha256:abcdef0123456789" label="config hash" />);
    expect(screen.getByText('abcdef01').getAttribute('title')).toBe('sha256:abcdef0123456789');
    await user.click(screen.getByRole('button', { name: 'Copy full config hash' }));
    expect(writeText).toHaveBeenCalledWith('sha256:abcdef0123456789');
    expect(await screen.findByText('Copied')).toBeDefined();
  });
});

describe('data display', () => {
  it('KeyValueList, Timeline and DiffViewer are readable', async () => {
    const { container } = render(
      <>
        <KeyValueList items={[{ key: 'Model', value: 'Qwen' }]} />
        <Timeline
          items={[
            { id: '1', at: new Date('2026-01-01T10:00:00Z'), title: 'Started' },
            {
              id: '2',
              at: new Date('2026-01-01T11:00:00Z'),
              title: 'Ran out of memory',
              problem: true,
            },
          ]}
        />
        <DiffViewer before={'a: 1\nb: 2'} after={'a: 1\nb: 3'} />
      </>,
    );
    expect(screen.getByText('Model').tagName).toBe('DT');
    expect(screen.getAllByRole('listitem').map((li) => li.querySelector('p')?.textContent)).toEqual(
      ['Started', 'Ran out of memory'],
    );
    expect(screen.getByText(/1 line added, 1 line removed/)).toBeDefined();
    expect(screen.getAllByText('added', { selector: '.sr-only' })).toHaveLength(1);
    expect(await serious(container)).toEqual([]);
  });
});

describe('ConfirmDialog', () => {
  it('needs the exact phrase before the action works', async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn();
    render(
      <ConfirmDialog
        open
        title="Delete run?"
        description="This can’t be undone."
        confirmPhrase="my-run"
        actionLabel="Delete run"
        onConfirm={onConfirm}
        onCancel={() => undefined}
      />,
    );
    const action = screen.getByRole('button', { name: 'Delete run' });
    expect((action as HTMLButtonElement).disabled).toBe(true);
    await user.type(screen.getByLabelText(/To confirm, type/), 'my-ru');
    expect((action as HTMLButtonElement).disabled).toBe(true);
    await user.type(screen.getByLabelText(/To confirm, type/), 'n');
    await user.click(action);
    expect(onConfirm).toHaveBeenCalledOnce();
  });
});

describe('CommandPalette', () => {
  function Palette({ run }: { run: () => void }) {
    const [open, setOpen] = useState(false);
    return (
      <CommandPalette
        open={open}
        onOpenChange={setOpen}
        commands={[
          { id: 'a', label: 'Datasets', group: 'Go to', run: () => undefined },
          { id: 'b', label: 'Runs', group: 'Go to', keywords: ['training'], run },
        ]}
      />
    );
  }

  it('opens with Ctrl+K, filters by keywords and runs with Enter', async () => {
    const user = userEvent.setup();
    const run = vi.fn();
    render(<Palette run={run} />);
    await user.keyboard('{Control>}k{/Control}');
    const input = screen.getByRole('combobox', { name: 'Search commands' });
    await user.type(input, 'train');
    expect(screen.getAllByRole('option').map((o) => o.textContent)).toEqual(['RunsGo to']);
    expect(input.getAttribute('aria-activedescendant')).toContain('-b');
    await user.keyboard('{Enter}');
    expect(run).toHaveBeenCalledOnce();
    expect(screen.queryByRole('combobox')).toBeNull();
  });

  it('says when nothing matches', async () => {
    const user = userEvent.setup();
    render(<Palette run={() => undefined} />);
    await user.keyboard('{Control>}k{/Control}');
    await user.type(screen.getByRole('combobox'), 'zzz');
    expect(screen.getByText(/Nothing matches/)).toBeDefined();
  });
});

describe('layout', () => {
  it('AppShell, LeftNav, ScopeSwitcher and PageHeader work together accessibly', async () => {
    const onChange = vi.fn();
    const { container } = render(
      <AppShell
        brand="MasterRL"
        topBar={
          <ScopeSwitcher
            levels={[
              {
                id: 'org',
                label: 'Organization',
                options: [{ id: 'o1', name: 'Home' }],
                value: 'o1',
                onChange,
              },
              {
                id: 'project',
                label: 'Project',
                options: [
                  { id: 'p1', name: 'One' },
                  { id: 'p2', name: 'Two' },
                ],
                value: 'p1',
                onChange,
              },
            ]}
          />
        }
        nav={
          <LeftNav
            currentPath="/runs/run_1"
            sections={[
              {
                id: 'main',
                items: [
                  { id: 'home', label: 'Overview', href: '/' },
                  { id: 'runs', label: 'Runs', href: '/runs', icon: 'chart' },
                ],
              },
              { id: 'empty', label: 'Nothing', items: [] },
            ]}
          />
        }
      >
        <PageHeader
          title="Runs"
          description="All runs."
          actions={<button type="button">New run</button>}
        />
      </AppShell>,
    );
    expect(screen.getByRole('link', { name: 'Skip to main content' }).getAttribute('href')).toBe(
      '#main',
    );
    expect(screen.getByRole('link', { name: 'Runs' }).getAttribute('aria-current')).toBe('page');
    expect(screen.getByRole('link', { name: 'Overview' }).getAttribute('aria-current')).toBeNull();
    expect(screen.queryByText('Nothing')).toBeNull();
    expect(screen.getByLabelText<HTMLSelectElement>('Organization').disabled).toBe(true);
    await userEvent.setup().selectOptions(screen.getByLabelText('Project'), 'p2');
    expect(onChange).toHaveBeenCalledWith('p2');
    expect(screen.getByRole('heading', { level: 1, name: 'Runs' })).toBeDefined();
    expect(await serious(container)).toEqual([]);
  });
});

describe('DataTable', () => {
  const rows = Array.from({ length: 30 }, (_, i) => ({ id: `r${i}`, name: `Run ${i}` }));
  const columns = [{ id: 'name', header: 'Name', cell: (r: (typeof rows)[number]) => r.name }];
  const empty = { title: 'No runs yet', description: 'Start one to see it here.' };

  it('renders a virtual window of rows and asks for the next cursor page', async () => {
    const user = userEvent.setup();
    const onLoadMore = vi.fn();
    const { container } = render(
      <DataTable
        label="Runs"
        status="success"
        rows={rows}
        getRowId={(r) => r.id}
        columns={columns}
        empty={empty}
        hasMore
        onLoadMore={onLoadMore}
        height={200}
      />,
    );
    const table = screen.getByRole('table', { name: 'Runs' });
    expect(table.getAttribute('aria-rowcount')).toBe('31');
    const shown = within(table)
      .getAllByRole('row')
      .filter((r) => r.getAttribute('aria-rowindex'));
    expect(shown.length).toBeLessThan(31);
    expect(screen.getByText(/Showing 30 rows — more available/)).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Show more' }));
    expect(onLoadMore).toHaveBeenCalled();
    expect(await serious(container)).toEqual([]);
  });

  it('has loading, empty and error states', () => {
    const { rerender } = render(
      <DataTable
        label="Runs"
        status="pending"
        rows={[]}
        getRowId={(r: { id: string }) => r.id}
        columns={[]}
        empty={empty}
      />,
    );
    expect(screen.getByRole('status').textContent).toContain('Loading runs');
    rerender(
      <DataTable
        label="Runs"
        status="success"
        rows={[]}
        getRowId={(r: { id: string }) => r.id}
        columns={[]}
        empty={empty}
      />,
    );
    expect(screen.getByText('No runs yet')).toBeDefined();
    rerender(
      <DataTable
        label="Runs"
        status="error"
        error={{ code: 'MRL-VAL-001' }}
        rows={[]}
        getRowId={(r: { id: string }) => r.id}
        columns={[]}
        empty={empty}
      />,
    );
    expect(screen.getByRole('alert').textContent).toContain('The app could not read this request.');
  });
});

describe('states', () => {
  it('PlainError explains a code: what happened, why, what to do', async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    const { container } = render(
      <PlainError code="OOM/OOM_GPU" traceId="tr_9" onRetry={onRetry} />,
    );
    const alert = screen.getByRole('alert');
    expect(within(alert).getByRole('heading', { level: 2 }).textContent.length).toBeGreaterThan(5);
    expect(alert.textContent).toContain('Why:');
    expect(alert.textContent).toContain('What to do:');
    expect(alert.textContent).toContain('OOM/OOM_GPU');
    expect(alert.textContent).toContain('tr_9');
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    expect(onRetry).toHaveBeenCalled();
    expect(await serious(container)).toEqual([]);
  });

  it('AsyncView picks loading, empty, error or content', () => {
    const props = {
      noun: 'datasets',
      empty: { title: 'No datasets yet', description: 'Upload one.' },
      isEmpty: (d: string[]) => d.length === 0,
    };
    const { rerender } = render(
      <AsyncView {...props} status="pending" data={undefined}>
        {(d) => d.join()}
      </AsyncView>,
    );
    expect(screen.getByText('Loading datasets')).toBeDefined();
    rerender(
      <AsyncView {...props} status="success" data={[]}>
        {(d) => d.join()}
      </AsyncView>,
    );
    expect(screen.getByText('No datasets yet')).toBeDefined();
    rerender(
      <AsyncView {...props} status="error" data={undefined} error={{ detail: 'Nope.' }}>
        {(d) => d.join()}
      </AsyncView>,
    );
    expect(screen.getByText('Nope.')).toBeDefined();
    rerender(
      <AsyncView {...props} status="success" data={['a', 'b']}>
        {(d) => d.join()}
      </AsyncView>,
    );
    expect(screen.getByText('a,b')).toBeDefined();
    render(<EmptyState title="T" description="D" />);
    render(<LoadingState label="Loading things" lines={2} />);
  });
});

describe('Wizard', () => {
  it('checks each step, shows problems in words and lets people go back', async () => {
    const user = userEvent.setup();
    const onFinish = vi.fn();
    let ready = false;
    render(
      <Wizard
        title="New run"
        onFinish={onFinish}
        steps={[
          {
            id: 'a',
            title: 'Pick a model',
            render: () => <p>model</p>,
            validate: () => (ready ? null : 'Pick a model first.'),
          },
          { id: 'b', title: 'Review', render: () => <p>review</p> },
        ]}
      />,
    );
    expect(screen.getByRole('heading', { name: 'Step 1 of 2: Pick a model' })).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Next' }));
    expect(screen.getByRole('alert').textContent).toContain('Pick a model first.');
    expect(screen.getByText('needs attention')).toBeDefined();
    ready = true;
    await user.click(screen.getByRole('button', { name: 'Next' }));
    expect(screen.getByRole('heading', { name: 'Step 2 of 2: Review' })).toBeDefined();
    await user.click(screen.getByRole('button', { name: /Pick a model/ }));
    expect(screen.getByRole('heading', { name: 'Step 1 of 2: Pick a model' })).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Next' }));
    await user.click(screen.getByRole('button', { name: 'Finish' }));
    expect(onFinish).toHaveBeenCalledOnce();
  });

  it('Stepper marks the current step', () => {
    render(
      <Stepper
        steps={[
          { id: 'a', title: 'One', status: 'complete' },
          { id: 'b', title: 'Two', status: 'current' },
        ]}
      />,
    );
    expect(screen.getAllByRole('listitem')[1]?.getAttribute('aria-current')).toBe('step');
  });
});

describe('FileUploader', () => {
  it('uploads picked files in parts and shows progress', async () => {
    const user = userEvent.setup();
    const putPart = vi.fn<UploadTransport['putPart']>((_t, n) =>
      Promise.resolve({ partNumber: n, etag: 'e' }),
    );
    const onUploaded = vi.fn();
    const transport: UploadTransport = {
      open: () => Promise.resolve({ uploadId: 'u1', partSize: 3, completedParts: [] }),
      putPart,
      complete: () => Promise.resolve(),
    };
    render(<FileUploader transport={transport} hint="Any file." onUploaded={onUploaded} />);
    const input = document.querySelector<HTMLInputElement>(
      'input[type="file"]',
    ) as HTMLInputElement;
    await user.upload(input, new File(['abcdefgh'], 'train.jsonl'));
    expect(await screen.findByText('Uploaded')).toBeDefined();
    expect(putPart).toHaveBeenCalledTimes(3);
    expect(onUploaded).toHaveBeenCalledOnce();
    expect(
      screen
        .getByRole('progressbar', { name: 'train.jsonl upload progress' })
        .getAttribute('value'),
    ).toBe('100');
    expect(
      screen.getByRole('button', { name: 'Upload files' }).getAttribute('aria-describedby'),
    ).toBeTruthy();
  });

  it('explains a failed upload and offers to try again', async () => {
    const user = userEvent.setup();
    const transport: UploadTransport = {
      open: () =>
        Promise.reject(
          Object.assign(new Error('No space left.'), { code: 'INFRA_TRANSIENT/DISK_FULL' }),
        ),
      putPart: () => Promise.reject(new Error('unused')),
      complete: () => Promise.resolve(),
    };
    render(<FileUploader transport={transport} />);
    await user.upload(
      document.querySelector<HTMLInputElement>('input[type="file"]') as HTMLInputElement,
      new File(['x'], 'a.txt'),
    );
    expect(await screen.findByRole('alert')).toBeDefined();
    expect(screen.getByText('No space left.')).toBeDefined();
    expect(screen.getByRole('button', { name: 'Try again' })).toBeDefined();
  });
});

describe('LiveLogViewer', () => {
  it('counts lines, searches and says how many lines were skipped', async () => {
    const user = userEvent.setup();
    const buffer = new LogBuffer({ capacity: 3, maxLinesPerSecond: 1000 });
    const onOpenFullLog = vi.fn();
    const { container } = render(<LiveLogViewer buffer={buffer} onOpenFullLog={onOpenFullLog} />);
    act(() => {
      buffer.append([
        { text: 'loss=1' },
        { text: 'loss=2' },
        { text: 'saved checkpoint' },
        { text: 'loss=3' },
      ]);
    });
    expect(screen.getByText('3 lines')).toBeDefined();
    expect(screen.getByText(/1 lines skipped/)).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Open full log' }));
    expect(onOpenFullLog).toHaveBeenCalled();
    await user.type(screen.getByRole('searchbox', { name: 'Search the log' }), 'loss');
    expect(await screen.findByText('2 matching lines')).toBeDefined();
    expect(
      screen.getByRole('button', { name: /Stop following/ }).getAttribute('aria-pressed'),
    ).toBe('true');
    expect(await serious(container)).toEqual([]);
  });
});

describe('LiveStatusBanner', () => {
  it('shows the paused banner while reconnecting', async () => {
    const hub = new EventStreamHub(() => Promise.reject(new TypeError('down')), {
      minDelayMs: 10_000,
    });
    render(
      <EventStreamProvider value={hub}>
        <LiveStatusBanner />
      </EventStreamProvider>,
    );
    expect(screen.queryByRole('status')).toBeNull();
    act(() => {
      hub.subscribe('t', () => undefined);
    });
    expect(await screen.findByText(/Live updates paused — reconnecting/)).toBeDefined();
    hub.close();
  });
});
