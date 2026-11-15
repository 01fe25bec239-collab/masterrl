import { useEffect, useId, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { SegmentedControl } from '../components/Button';
import { useLiveMetrics, type LiveMetricsStore } from '../hooks/live-metrics';
import { formatNumber } from '../lib/format';
import { tokenColor } from '../lib/theme';
import { ChartFrame } from './ChartFrame';

export interface ChartSeries {
  /** Column id in the store. */
  id: string;
  label: string;
  /** GPU rank this series belongs to; enables the per-rank view. */
  rank?: number;
  /** Drawn dashed, e.g. a comparison run or a target line. */
  overlay?: boolean;
}

export interface TimeSeriesChartProps {
  title: string;
  series: ChartSeries[];
  /** Data source: load history with `store.load`, stream with `store.appendPoints`. */
  store: LiveMetricsStore;
  xLabel?: string;
  unit?: string;
  /** Points drawn per series; history is min/max downsampled to this. */
  maxPoints?: number;
  logScale?: boolean;
  height?: number;
}

const dashes: (number[] | undefined)[] = [undefined, undefined, [6, 4], [2, 3], [10, 3, 2, 3]];
const strokes = ['accent', 'fg', 'accent', 'fg-muted', 'danger'] as const;

/** False where canvas cannot draw (some test or locked-down environments): the chart shows text and table only. */
const canDraw = () =>
  typeof document !== 'undefined' && document.createElement('canvas').getContext('2d') !== null;

function seriesStyle(s: ChartSeries, i: number): Pick<uPlot.Series, 'stroke' | 'dash' | 'width'> {
  return {
    stroke: tokenColor(strokes[i % strokes.length] ?? 'accent'),
    dash: s.overlay ? [6, 4] : dashes[Math.floor(i / strokes.length) % dashes.length],
    width: s.overlay ? 1.5 : 2,
  };
}

/** uPlot line chart: streaming append, min/max downsampled history, overlays, per-rank view, log scale. */
export function TimeSeriesChart({
  title,
  series,
  store,
  xLabel = 'Step',
  unit,
  maxPoints = 2000,
  logScale: initialLog = false,
  height = 260,
}: TimeSeriesChartProps) {
  const version = useLiveMetrics(store);
  const [log, setLog] = useState(initialLog);
  const ranks = useMemo(
    () =>
      [...new Set(series.flatMap((s) => (s.rank === undefined ? [] : [s.rank])))].sort(
        (a, b) => a - b,
      ),
    [series],
  );
  const [rank, setRank] = useState<'all' | `${number}`>('all');
  const visible = useMemo(
    () =>
      rank === 'all'
        ? series
        : series.filter((s) => s.rank === undefined || s.rank === Number(rank)),
    [series, rank],
  );
  const ids = useMemo(() => visible.map((s) => s.id), [visible]);
  const hostRef = useRef<HTMLDivElement>(null);
  const plotRef = useRef<uPlot | null>(null);
  const rankId = useId();
  const [drawable] = useState(canDraw);

  const aligned = (points: number): uPlot.AlignedData => {
    const snap = store.snapshot(ids, points);
    const ys = log
      ? snap.ys.map((col) => col.map((v) => (v !== null && v > 0 ? v : null)))
      : snap.ys;
    return [snap.x, ...ys];
  };

  // Build the plot when what is drawn changes (series, scale, size).
  useEffect(() => {
    const host = hostRef.current;
    if (!host || !drawable) return;
    const plot = new uPlot(
      {
        width: host.clientWidth || 600,
        height,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-enum-assignment -- uPlot's Distr is an ambient const enum (3 = log, 1 = linear), unusable with isolatedModules
        scales: { x: { time: false }, y: { distr: log ? 3 : 1 } },
        axes: [
          { label: xLabel, stroke: tokenColor('fg-muted'), grid: { stroke: tokenColor('border') } },
          { label: unit, stroke: tokenColor('fg-muted'), grid: { stroke: tokenColor('border') } },
        ],
        legend: { show: true },
        series: [
          { label: xLabel },
          ...visible.map((s, i) => ({ label: s.label, ...seriesStyle(s, i) })),
        ],
      },
      aligned(maxPoints),
      host,
    );
    plotRef.current = plot;
    const resize =
      typeof ResizeObserver === 'undefined'
        ? null
        : new ResizeObserver(([entry]) => {
            if (entry) plot.setSize({ width: entry.contentRect.width, height });
          });
    resize?.observe(host);
    return () => {
      resize?.disconnect();
      plot.destroy();
      plotRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- data updates go through setData below
  }, [visible, log, height, xLabel, unit, drawable]);

  // Streaming: push new data at most once per frame.
  useEffect(() => {
    const frame = requestAnimationFrame(() => {
      plotRef.current?.setData(aligned(maxPoints));
    });
    return () => {
      cancelAnimationFrame(frame);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `version` is the change signal
  }, [version, maxPoints]);

  const summary =
    store.size === 0
      ? 'No data yet.'
      : visible
          .map((s) => {
            const { latest, min, max } = store.summary(s.id);
            return `${s.label}: latest ${formatNumber(latest, unit)}, lowest ${formatNumber(min, unit)}, highest ${formatNumber(max, unit)}`;
          })
          .join('. ') + '.';

  return (
    <ChartFrame
      title={title}
      summary={summary}
      toolbar={
        <>
          {ranks.length > 0 && (
            <>
              <label htmlFor={rankId} className="text-sm text-fg-muted">
                Show
              </label>
              <select
                id={rankId}
                className="rounded-md border border-border bg-bg px-2 py-1 text-sm text-fg"
                value={rank}
                onChange={(e) => {
                  setRank(e.target.value as 'all' | `${number}`);
                }}
              >
                <option value="all">All GPUs</option>
                {ranks.map((r) => (
                  <option key={r} value={String(r)}>
                    GPU rank {r}
                  </option>
                ))}
              </select>
            </>
          )}
          <SegmentedControl
            label={`${title} scale`}
            options={[
              { value: 'linear', label: 'Linear' },
              { value: 'log', label: 'Log' },
            ]}
            value={log ? 'log' : 'linear'}
            onChange={(v) => {
              setLog(v === 'log');
            }}
          />
        </>
      }
      table={() => {
        const data = aligned(200);
        const [x, ...ys] = data;
        return {
          columns: [xLabel, ...visible.map((s) => s.label)],
          rows: Array.from(x, (step, i) => [
            step,
            ...ys.map((col) => formatNumber(col[i] ?? null, unit)),
          ]),
          note:
            store.size > x.length
              ? `Sampled to ${x.length} of ${store.size.toLocaleString('en-US')} points, keeping highs and lows.`
              : undefined,
        };
      }}
    >
      {drawable ? (
        <div ref={hostRef} style={{ minHeight: height }} data-version={version} />
      ) : (
        <p className="text-sm text-fg-muted">
          This screen can’t draw charts. Open the table to see the numbers.
        </p>
      )}
    </ChartFrame>
  );
}
