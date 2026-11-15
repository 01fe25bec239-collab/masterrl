export interface DiffLine {
  kind: 'same' | 'added' | 'removed';
  text: string;
  /** 1-based line numbers in the old and new text. */
  oldLine?: number;
  newLine?: number;
}

/**
 * Line diff via longest common subsequence.
 * ponytail: O(n·m) memory; fine for configs and manifests (a few thousand lines).
 * Switch to Myers' O(ND) if we diff large files.
 */
export function diffLines(before: string, after: string): DiffLine[] {
  const a = before.split('\n');
  const b = after.split('\n');
  const n = a.length;
  const m = b.length;
  const lcs = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i -= 1) {
    const row = lcs[i] as Uint32Array;
    const next = lcs[i + 1] as Uint32Array;
    for (let j = m - 1; j >= 0; j -= 1) {
      row[j] =
        a[i] === b[j]
          ? (next[j + 1] as number) + 1
          : Math.max(next[j] as number, row[j + 1] as number);
    }
  }

  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n || j < m) {
    if (i < n && j < m && a[i] === b[j]) {
      out.push({ kind: 'same', text: a[i] as string, oldLine: i + 1, newLine: j + 1 });
      i += 1;
      j += 1;
    } else if (
      j < m &&
      (i >= n ||
        ((lcs[i] as Uint32Array)[j + 1] as number) >= ((lcs[i + 1] as Uint32Array)[j] as number))
    ) {
      out.push({ kind: 'added', text: b[j] as string, newLine: j + 1 });
      j += 1;
    } else {
      out.push({ kind: 'removed', text: a[i] as string, oldLine: i + 1 });
      i += 1;
    }
  }
  return out;
}
