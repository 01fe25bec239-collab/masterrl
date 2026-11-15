import { RingBuffer } from '../lib/ring-buffer';

export interface LogLine {
  /** Increasing number, unique within the buffer. */
  seq: number;
  text: string;
  rank?: number;
  stream?: 'stdout' | 'stderr';
}

export interface LogBufferOptions {
  capacity?: number;
  /** Above this rate, extra lines are skipped and counted (backpressure). */
  maxLinesPerSecond?: number;
  now?: () => number;
}

/** Ring buffer of log lines with a rate limit that counts what it skips. */
export class LogBuffer {
  private ring: RingBuffer<LogLine>;
  private listeners = new Set<() => void>();
  private windowStart = 0;
  private windowCount = 0;
  private seq = 0;
  private readonly maxPerSecond: number;
  private readonly now: () => number;
  /** Lines dropped because they came in too fast. */
  skipped = 0;
  version = 0;

  constructor({ capacity = 5000, maxLinesPerSecond = 200, now = Date.now }: LogBufferOptions = {}) {
    this.ring = new RingBuffer(capacity);
    this.maxPerSecond = maxLinesPerSecond;
    this.now = now;
  }

  /** Older lines that fell out of the buffer. */
  get evicted(): number {
    return this.ring.evicted;
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = (): number => this.version;

  append(lines: readonly Omit<LogLine, 'seq'>[]): void {
    const t = this.now();
    if (t - this.windowStart >= 1000) {
      this.windowStart = t;
      this.windowCount = 0;
    }
    for (const line of lines) {
      for (const text of line.text.split('\n')) {
        if (this.windowCount >= this.maxPerSecond) {
          this.skipped += 1;
          continue;
        }
        this.windowCount += 1;
        this.seq += 1;
        this.ring.push({ ...line, text, seq: this.seq });
      }
    }
    this.version += 1;
    for (const l of this.listeners) l();
  }

  private cache: { version: number; lines: LogLine[] } | null = null;

  /** Oldest first. Same array until the next change. */
  lines(): LogLine[] {
    if (this.cache?.version !== this.version)
      this.cache = { version: this.version, lines: this.ring.toArray() };
    return this.cache.lines;
  }

  clear(): void {
    this.ring.clear();
    this.skipped = 0;
    this.version += 1;
    for (const l of this.listeners) l();
  }
}
