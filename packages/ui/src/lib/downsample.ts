export interface Columns {
  x: ArrayLike<number>;
  /** One column per series, same length as `x`. `null` or NaN means no value at that x. */
  ys: ArrayLike<number | null>[];
}

export interface DownsampledColumns {
  x: number[];
  ys: (number | null)[][];
}

const isValue = (v: number | null | undefined): v is number => v != null && !Number.isNaN(v);

/**
 * Min/max downsampling: splits x into `buckets` equal index ranges and keeps, per bucket,
 * the points where any series hits its min or max. Spikes stay visible; order is kept.
 * Returns the input unchanged (copied) when it already fits in 2 × buckets points.
 */
export function minMaxDownsample({ x, ys }: Columns, buckets: number): DownsampledColumns {
  const n = x.length;
  const copy = (): DownsampledColumns => ({
    x: Array.from(x),
    ys: ys.map((col) => Array.from(col, (v) => (isValue(v) ? v : null))),
  });
  if (buckets < 1 || n <= buckets * 2) return copy();

  const keep = new Set<number>();
  const size = n / buckets;
  for (let b = 0; b < buckets; b += 1) {
    const from = Math.floor(b * size);
    const to = Math.min(n, Math.floor((b + 1) * size));
    if (from >= to) continue;
    keep.add(from);
    for (const col of ys) {
      let minI = -1;
      let maxI = -1;
      for (let i = from; i < to; i += 1) {
        const v = col[i];
        if (!isValue(v)) continue;
        if (minI < 0 || v < (col[minI] as number)) minI = i;
        if (maxI < 0 || v > (col[maxI] as number)) maxI = i;
      }
      if (minI >= 0) keep.add(minI);
      if (maxI >= 0) keep.add(maxI);
    }
  }
  keep.add(n - 1);

  const idx = [...keep].sort((a, b) => a - b);
  return {
    x: idx.map((i) => x[i] as number),
    ys: ys.map((col) =>
      idx.map((i) => {
        const v = col[i];
        return isValue(v) ? v : null;
      }),
    ),
  };
}
