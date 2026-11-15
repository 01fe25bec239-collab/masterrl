import { cn } from '../lib/cn';
import { formatNumber, formatRange, type Range } from '../lib/format';
import { ChartFrame } from './ChartFrame';

export interface HistogramBin {
  start: number;
  end: number;
  count: number;
}

/** Bin raw values into `bins` equal-width buckets. */
export function binValues(values: readonly number[], bins = 20): HistogramBin[] {
  if (values.length === 0) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = (max - min) / bins || 1;
  const out = Array.from({ length: bins }, (_, i) => ({
    start: min + i * width,
    end: min + (i + 1) * width,
    count: 0,
  }));
  for (const v of values) {
    const i = Math.min(bins - 1, Math.floor((v - min) / width));
    (out[i] as HistogramBin).count += 1;
  }
  return out;
}

export interface HistogramProps {
  title: string;
  bins: HistogramBin[];
  /** What is counted, e.g. "Tokens per sample". */
  valueLabel: string;
  height?: number;
}

export function Histogram({ title, bins, valueLabel, height = 160 }: HistogramProps) {
  const total = bins.reduce((sum, b) => sum + b.count, 0);
  const peak = Math.max(1, ...bins.map((b) => b.count));
  const top = bins.reduce<HistogramBin | undefined>(
    (best, b) => (!best || b.count > best.count ? b : best),
    undefined,
  );
  const summary =
    total === 0
      ? 'No data yet.'
      : `${total.toLocaleString('en-US')} values from ${formatNumber(bins[0]?.start)} to ${formatNumber(bins.at(-1)?.end)}. Most common range: ${formatNumber(top?.start)}–${formatNumber(top?.end)} (${top?.count ?? 0}).`;
  const w = 100 / Math.max(1, bins.length);

  return (
    <ChartFrame
      title={title}
      summary={summary}
      table={() => ({
        columns: [valueLabel, 'Count'],
        rows: bins.map((b) => [`${formatNumber(b.start)}–${formatNumber(b.end)}`, b.count]),
      })}
    >
      <svg
        viewBox={`0 0 100 ${height}`}
        preserveAspectRatio="none"
        className="w-full"
        style={{ height }}
      >
        {bins.map((b, i) => {
          const h = (b.count / peak) * (height - 2);
          return (
            <rect
              key={i}
              x={i * w + w * 0.05}
              y={height - h}
              width={w * 0.9}
              height={h}
              className="fill-accent"
            />
          );
        })}
      </svg>
    </ChartFrame>
  );
}

export interface SparklineProps {
  values: readonly (number | null)[];
  /** e.g. "Loss" → "Loss: latest 0.41, lowest 0.40, highest 2.1". */
  label: string;
  unit?: string;
  width?: number;
  height?: number;
  className?: string;
}

/** Tiny inline trend line with its summary as the accessible name. */
export function Sparkline({
  values,
  label,
  unit,
  width = 96,
  height = 24,
  className,
}: SparklineProps) {
  const nums = values.filter((v): v is number => v !== null && !Number.isNaN(v));
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const span = max - min || 1;
  const step = values.length > 1 ? width / (values.length - 1) : 0;
  const points = values
    .map((v, i) =>
      v === null || Number.isNaN(v)
        ? null
        : `${i * step},${height - 1 - ((v - min) / span) * (height - 2)}`,
    )
    .filter(Boolean)
    .join(' ');
  const name =
    nums.length === 0
      ? `${label}: no data yet`
      : `${label}: latest ${formatNumber(nums.at(-1), unit)}, lowest ${formatNumber(min, unit)}, highest ${formatNumber(max, unit)}`;

  return (
    <svg
      role="img"
      aria-label={name}
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      className={cn('inline-block text-accent', className)}
    >
      <title>{name}</title>
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth={1.5} />
    </svg>
  );
}

function Meter({
  label,
  value,
  max,
  text,
}: {
  label: string;
  value: number;
  max: number;
  text: string;
}) {
  const pct = Math.max(0, Math.min(100, (value / (max || 1)) * 100));
  return (
    <div>
      <div className="flex justify-between text-xs">
        <span className="text-fg-muted">{label}</span>
        <span className="text-fg">{text}</span>
      </div>
      <div
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={max}
        aria-valuenow={value}
        aria-valuetext={text}
        className="mt-1 h-2 overflow-hidden rounded-full bg-surface"
      >
        <div
          className={cn('h-full', pct >= 90 ? 'bg-danger' : 'bg-accent')}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

export interface GpuGaugeProps {
  name: string;
  utilizationPct: number;
  memoryUsedGb: number;
  memoryTotalGb: number;
  temperatureC?: number;
  className?: string;
}

export function GpuGauge({
  name,
  utilizationPct,
  memoryUsedGb,
  memoryTotalGb,
  temperatureC,
  className,
}: GpuGaugeProps) {
  return (
    <section
      aria-label={`${name} usage`}
      className={cn('flex flex-col gap-2 rounded-md border border-border p-3', className)}
    >
      <p className="text-sm font-medium text-fg">{name}</p>
      <Meter
        label="Busy"
        value={utilizationPct}
        max={100}
        text={`${Math.round(utilizationPct)}%`}
      />
      <Meter
        label="Memory"
        value={memoryUsedGb}
        max={memoryTotalGb}
        text={`${formatNumber(memoryUsedGb)} of ${formatNumber(memoryTotalGb)} GB`}
      />
      {temperatureC !== undefined && (
        <p className="text-xs text-fg-muted">Temperature: {Math.round(temperatureC)} °C</p>
      )}
    </section>
  );
}

export interface ResourcePart {
  label: string;
  /** Estimates are ranges: low and high guess. */
  range: Range;
}

export interface ResourceBreakdownBarProps {
  title: string;
  parts: ResourcePart[];
  unit: string;
  /** What is available, e.g. GPU memory. Draws a limit line and says whether it fits. */
  capacity?: number;
  className?: string;
}

/** Stacked estimate bar: solid = low estimate, striped = how much higher it may go. */
export function ResourceBreakdownBar({
  title,
  parts,
  unit,
  capacity,
  className,
}: ResourceBreakdownBarProps) {
  const total = parts.reduce((t, p) => ({ min: t.min + p.range.min, max: t.max + p.range.max }), {
    min: 0,
    max: 0,
  });
  const scale = Math.max(total.max, capacity ?? 0) || 1;
  const pct = (v: number) => `${(v / scale) * 100}%`;
  const fit =
    capacity === undefined
      ? null
      : total.max <= capacity
        ? `Fits: needs ${formatRange(total, unit)} of ${formatNumber(capacity, unit)}.`
        : total.min > capacity
          ? `Does not fit: needs ${formatRange(total, unit)}, but only ${formatNumber(capacity, unit)} is available.`
          : `Might not fit: needs ${formatRange(total, unit)}, and ${formatNumber(capacity, unit)} is available.`;

  return (
    <section aria-label={title} className={cn('flex flex-col gap-2', className)}>
      <p className="text-sm font-medium text-fg">
        {title}: {formatRange(total, unit)}
      </p>
      <div aria-hidden="true" className="relative flex h-4 overflow-hidden rounded bg-surface">
        {parts.map((p, i) => (
          <div key={p.label} className="flex h-full">
            <div
              className={cn('h-full', i % 2 ? 'bg-fg-muted' : 'bg-accent')}
              style={{ width: pct(p.range.min) }}
            />
            <div
              className={cn(
                'h-full opacity-60',
                i % 2
                  ? 'bg-[repeating-linear-gradient(45deg,var(--color-fg-muted)_0_3px,transparent_3px_6px)]'
                  : 'bg-[repeating-linear-gradient(45deg,var(--color-accent)_0_3px,transparent_3px_6px)]',
              )}
              style={{ width: pct(p.range.max - p.range.min) }}
            />
          </div>
        ))}
        {capacity !== undefined && (
          <div className="absolute inset-y-0 w-0.5 bg-danger" style={{ left: pct(capacity) }} />
        )}
      </div>
      <ul className="flex flex-wrap gap-x-4 gap-y-1 text-sm text-fg">
        {parts.map((p) => (
          <li key={p.label}>
            {p.label}: {formatRange(p.range, unit)}
          </li>
        ))}
      </ul>
      {fit && (
        <p
          className={cn('text-sm', total.max > (capacity ?? Infinity) ? 'text-danger' : 'text-fg')}
        >
          {fit}
        </p>
      )}
      <p className="text-xs text-fg-muted">
        Estimates. Solid shows the low guess; stripes show how much higher it may go.
      </p>
    </section>
  );
}
