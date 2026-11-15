import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axe from 'axe-core';
import { describe, expect, it, vi } from 'vitest';
import { LiveMetricsStore } from '../hooks/live-metrics';
import { binValues, GpuGauge, Histogram, ResourceBreakdownBar, Sparkline } from './BasicCharts';
import { TimeSeriesChart } from './TimeSeriesChart';

const plots: {
  setData: ReturnType<typeof vi.fn>;
  opts: { series: { label?: string }[]; scales: { y: { distr: number } } };
}[] = [];
vi.mock('uplot', () => ({
  default: class {
    setData = vi.fn();
    setSize = vi.fn();
    destroy = vi.fn();
    constructor(opts: (typeof plots)[number]['opts']) {
      plots.push({ setData: this.setData, opts });
    }
  },
}));

async function serious(container: HTMLElement) {
  const r = await axe.run(container);
  return r.violations
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => v.id);
}

function store() {
  const s = new LiveMetricsStore();
  const x = Array.from({ length: 5000 }, (_, i) => i);
  s.load(x, {
    r0: x.map((i) => 5 - i / 1000),
    r1: x.map((i) => 6 - i / 1000),
    base: x.map(() => 4),
  });
  return s;
}
const series = [
  { id: 'r0', label: 'Loss GPU 0', rank: 0 },
  { id: 'r1', label: 'Loss GPU 1', rank: 1 },
  { id: 'base', label: 'Target', overlay: true },
];

describe('TimeSeriesChart', () => {
  it('gives a text summary and an accessible table alternative', async () => {
    const user = userEvent.setup();
    const { container } = render(<TimeSeriesChart title="Loss" series={series} store={store()} />);
    expect(screen.getByText(/Loss GPU 0: latest 0\.001, lowest 0\.001, highest 5/)).toBeDefined();

    const toggle = screen.getByRole('button', { name: 'Show as table' });
    expect(toggle.getAttribute('aria-expanded')).toBe('false');
    await user.click(toggle);
    const table = screen.getByRole('table', { name: /Loss as a table/ });
    expect(
      within(table)
        .getAllByRole('columnheader')
        .map((h) => h.textContent),
    ).toEqual(['Step', 'Loss GPU 0', 'Loss GPU 1', 'Target']);
    expect(within(table).getAllByRole('row').length).toBeGreaterThan(50);
    expect(table.textContent).toContain('Sampled to');
    expect(await serious(container)).toEqual([]);
  });

  it('filters to one GPU rank and keeps overlays', async () => {
    const user = userEvent.setup();
    render(<TimeSeriesChart title="Loss" series={series} store={store()} />);
    await user.selectOptions(screen.getByLabelText('Show'), 'GPU rank 1');
    expect(screen.queryByText(/Loss GPU 0:/)).toBeNull();
    expect(screen.getByText(/Loss GPU 1:/)).toBeDefined();
    expect(screen.getByText(/Target:/)).toBeDefined();
  });

  it('draws with uPlot when canvas works: log scale, overlays dashed, streaming setData', async () => {
    const spy = vi
      .spyOn(HTMLCanvasElement.prototype, 'getContext')
      .mockReturnValue({} as CanvasRenderingContext2D);
    try {
      plots.length = 0;
      const user = userEvent.setup();
      const s = store();
      render(<TimeSeriesChart title="Loss" series={series} store={s} />);
      expect(plots[0]?.opts.series.map((x) => x.label)).toEqual([
        'Step',
        'Loss GPU 0',
        'Loss GPU 1',
        'Target',
      ]);
      expect((plots[0]?.opts.series[3] as { dash?: number[] }).dash).toEqual([6, 4]);

      await user.click(screen.getByRole('radio', { name: 'Log' }));
      expect(plots.at(-1)?.opts.scales.y.distr).toBe(3);

      const plot = plots.at(-1);
      act(() => {
        s.appendPoints('r0', [[5000, 0.5]]);
      });
      await vi.waitFor(() => {
        expect(plot?.setData).toHaveBeenCalled();
      });
    } finally {
      spy.mockRestore();
    }
  });
});

describe('small charts', () => {
  it('histogram has a summary and a table', async () => {
    const user = userEvent.setup();
    const { container } = render(
      <Histogram title="Lengths" valueLabel="Tokens" bins={binValues([1, 2, 2, 3, 3, 3, 10], 3)} />,
    );
    expect(screen.getByText(/7 values from 1 to 10/)).toBeDefined();
    await user.click(screen.getByRole('button', { name: 'Show as table' }));
    expect(screen.getAllByRole('row')).toHaveLength(4);
    expect(await serious(container)).toEqual([]);
  });

  it('binValues spreads values into equal buckets', () => {
    expect(binValues([0, 1, 2, 3], 2).map((b) => b.count)).toEqual([2, 2]);
    expect(binValues([], 5)).toEqual([]);
  });

  it('sparkline names itself with latest, lowest and highest', () => {
    render(<Sparkline label="Reward" values={[0.1, null, 0.5, 0.3]} />);
    expect(
      screen.getByRole('img', { name: 'Reward: latest 0.3, lowest 0.1, highest 0.5' }),
    ).toBeDefined();
  });

  it('GPU gauge exposes meters with readable values', () => {
    render(
      <GpuGauge
        name="RTX 3060"
        utilizationPct={87}
        memoryUsedGb={5}
        memoryTotalGb={6}
        temperatureC={70}
      />,
    );
    const mem = screen.getByRole('meter', { name: 'Memory' });
    expect(mem.getAttribute('aria-valuetext')).toBe('5 of 6 GB');
    expect(screen.getByRole('meter', { name: 'Busy' }).getAttribute('aria-valuenow')).toBe('87');
  });

  it('resource bar shows ranges and says whether it fits', () => {
    const { rerender } = render(
      <ResourceBreakdownBar
        title="GPU memory"
        unit="GB"
        capacity={6}
        parts={[
          { label: 'Weights', range: { min: 1, max: 1 } },
          { label: 'Activations', range: { min: 2, max: 4 } },
        ]}
      />,
    );
    expect(screen.getByText('GPU memory: 3–5 GB')).toBeDefined();
    expect(screen.getByText('Activations: 2–4 GB')).toBeDefined();
    expect(screen.getByText(/^Fits/)).toBeDefined();
    rerender(
      <ResourceBreakdownBar
        title="GPU memory"
        unit="GB"
        capacity={4}
        parts={[{ label: 'All', range: { min: 3, max: 5 } }]}
      />,
    );
    expect(screen.getByText(/^Might not fit/)).toBeDefined();
    rerender(
      <ResourceBreakdownBar
        title="GPU memory"
        unit="GB"
        capacity={2}
        parts={[{ label: 'All', range: { min: 3, max: 5 } }]}
      />,
    );
    expect(screen.getByText(/^Does not fit/)).toBeDefined();
  });
});
