import { useSyncExternalStore } from 'react';
import { minMaxDownsample, type DownsampledColumns } from '../lib/downsample';

export interface SeriesSummary {
  latest: number | null;
  min: number | null;
  max: number | null;
}

/**
 * Per-run, in-memory, columnar ring buffer feeding uPlot.
 * Rows are keyed by step (x, ascending); each series is one Float64Array column (NaN = no value).
 * When full, the oldest rows drop off.
 */
export class LiveMetricsStore {
  private x: Float64Array;
  private cols = new Map<string, Float64Array>();
  private start = 0;
  private length = 0;
  private listeners = new Set<() => void>();
  /** Bumps on every change; use it as a cheap snapshot key. */
  version = 0;

  constructor(readonly capacity = 100_000) {
    this.x = new Float64Array(capacity);
  }

  get size(): number {
    return this.length;
  }

  get seriesIds(): string[] {
    return [...this.cols.keys()];
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = (): number => this.version;

  private phys(i: number) {
    return (this.start + i) % this.capacity;
  }

  private col(id: string): Float64Array {
    let c = this.cols.get(id);
    if (!c) {
      c = new Float64Array(this.capacity).fill(Number.NaN);
      this.cols.set(id, c);
    }
    return c;
  }

  private changed() {
    this.version += 1;
    for (const l of this.listeners) l();
  }

  /** Logical row index for step `x`, or the insertion point as `-(i + 1)`. */
  private find(x: number): number {
    let lo = 0;
    let hi = this.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const v = this.x[this.phys(mid)] as number;
      if (v === x) return mid;
      if (v < x) lo = mid + 1;
      else hi = mid - 1;
    }
    return -(lo + 1);
  }

  private pushRow(x: number) {
    if (this.length === this.capacity) {
      const p = this.start;
      for (const c of this.cols.values()) c[p] = Number.NaN;
      this.start = (this.start + 1) % this.capacity;
      this.length -= 1;
    }
    const p = this.phys(this.length);
    this.x[p] = x;
    for (const c of this.cols.values()) c[p] = Number.NaN;
    this.length += 1;
  }

  /**
   * Insert a row in the middle.
   * ponytail: O(n) shift; only out-of-order points pay it, in-order appends are O(1).
   */
  private insertRow(at: number, x: number) {
    if (this.length === this.capacity) {
      if (at === 0) return false;
      this.start = (this.start + 1) % this.capacity;
      this.length -= 1;
      at -= 1;
    }
    for (let i = this.length; i > at; i -= 1) {
      const to = this.phys(i);
      const from = this.phys(i - 1);
      this.x[to] = this.x[from] as number;
      for (const c of this.cols.values()) c[to] = c[from] as number;
    }
    const p = this.phys(at);
    this.x[p] = x;
    for (const c of this.cols.values()) c[p] = Number.NaN;
    this.length += 1;
    return true;
  }

  private setPoint(seriesId: string, x: number, value: number) {
    const c = this.col(seriesId);
    const last = this.length > 0 ? (this.x[this.phys(this.length - 1)] as number) : -Infinity;
    if (x > last) {
      this.pushRow(x);
      c[this.phys(this.length - 1)] = value;
      return;
    }
    let i = this.find(x);
    if (i < 0) {
      const at = -(i + 1);
      if (!this.insertRow(at, x)) return;
      i = this.find(x);
    }
    c[this.phys(i)] = value;
  }

  /** Points for one series, e.g. from a `metrics` event: `[[step, value, ts?], …]`. */
  appendPoints(seriesId: string, points: readonly (readonly number[])[]): void {
    if (points.length === 0) return;
    for (const [x, value] of points) {
      if (x === undefined || value === undefined) continue;
      this.setPoint(seriesId, x, value);
    }
    this.changed();
  }

  /** Replace everything with history from REST (already downsampled by the server). */
  load(x: readonly number[], series: Record<string, readonly (number | null)[]>): void {
    this.start = 0;
    this.length = 0;
    this.cols.clear();
    const from = Math.max(0, x.length - this.capacity);
    for (const id of Object.keys(series)) this.col(id);
    for (let i = from; i < x.length; i += 1) {
      this.pushRow(x[i] as number);
      const p = this.phys(this.length - 1);
      for (const [id, values] of Object.entries(series)) {
        const v = values[i];
        (this.cols.get(id) as Float64Array)[p] = v ?? Number.NaN;
      }
    }
    this.changed();
  }

  clear(): void {
    this.start = 0;
    this.length = 0;
    this.cols.clear();
    this.changed();
  }

  /** All rows in step order, downsampled (min/max) to about `maxPoints` rows. */
  snapshot(seriesIds: readonly string[] = this.seriesIds, maxPoints = 2000): DownsampledColumns {
    const x = new Float64Array(this.length);
    const ys = seriesIds.map(() => new Float64Array(this.length));
    for (let i = 0; i < this.length; i += 1) {
      const p = this.phys(i);
      x[i] = this.x[p] as number;
      seriesIds.forEach((id, s) => {
        const c = this.cols.get(id);
        (ys[s] as Float64Array)[i] = c ? (c[p] as number) : Number.NaN;
      });
    }
    return minMaxDownsample({ x, ys }, Math.max(1, Math.floor(maxPoints / 2)));
  }

  summary(seriesId: string): SeriesSummary {
    const c = this.cols.get(seriesId);
    let latest: number | null = null;
    let min: number | null = null;
    let max: number | null = null;
    if (c) {
      for (let i = 0; i < this.length; i += 1) {
        const v = c[this.phys(i)] as number;
        if (Number.isNaN(v)) continue;
        latest = v;
        min = min === null ? v : Math.min(min, v);
        max = max === null ? v : Math.max(max, v);
      }
    }
    return { latest, min, max };
  }
}

/** Re-render when the store changes; returns its version. */
export function useLiveMetrics(store: LiveMetricsStore): number {
  return useSyncExternalStore(store.subscribe, store.getVersion);
}
