export interface StreamEvent {
  /** `<source>:<cursor>`; sent back as Last-Event-ID on reconnect. */
  id: string | undefined;
  /** SSE `event:` field: `state`, `metrics`, `log`, `telemetry`, `alert`, `resync`… */
  type: string;
  data: string;
  /** `data` parsed as JSON, or undefined when it is not JSON. */
  json: unknown;
}

export interface OpenStreamArgs {
  topics: string[];
  lastEventId: string | undefined;
  signal: AbortSignal;
}

/** Opens `GET /api/v1/streams?topics=…`; supplied by the API client so auth stays in one place. */
export type OpenStream = (args: OpenStreamArgs) => Promise<Response>;

export type StreamStatus = 'idle' | 'connecting' | 'live' | 'reconnecting' | 'failed';

export interface StreamFailure {
  status: number;
  code: string | undefined;
}

export interface EventStreamOptions {
  minDelayMs?: number;
  maxDelayMs?: number;
  /** No bytes (not even a heartbeat) for this long → treat the connection as dead. */
  staleAfterMs?: number;
  random?: () => number;
}

type Handler = (event: StreamEvent) => void;

/** Retrying cannot fix these: bad topics, not signed in, not allowed. */
const FATAL_STATUS = new Set([400, 401, 403]);

/** Splits an SSE byte stream into events (WHATWG "event stream interpretation"). */
export class SseParser {
  private buffer = '';
  private data: string[] = [];
  private type = '';
  lastEventId: string | undefined;
  retry: number | undefined;

  constructor(private readonly emit: (event: StreamEvent) => void) {}

  push(chunk: string): void {
    this.buffer += chunk;
    const lines = this.buffer.split(/\r\n|\r|\n/);
    this.buffer = lines.pop() ?? '';
    for (const line of lines) this.line(line);
  }

  private line(line: string): void {
    if (line === '') {
      if (this.data.length > 0) {
        const data = this.data.join('\n');
        let json: unknown;
        try {
          json = JSON.parse(data);
        } catch {
          json = undefined;
        }
        this.emit({ id: this.lastEventId, type: this.type || 'message', data, json });
      }
      this.data = [];
      this.type = '';
      return;
    }
    if (line.startsWith(':')) return;
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'data') this.data.push(value);
    else if (field === 'event') this.type = value;
    else if (field === 'id' && !value.includes('\0')) this.lastEventId = value;
    else if (field === 'retry' && /^\d+$/.test(value)) this.retry = Number(value);
  }
}

const topicOf = (event: StreamEvent): string | undefined => {
  const json = event.json;
  if (json && typeof json === 'object' && 'topic' in json) {
    const topic = json.topic;
    return typeof topic === 'string' ? topic : undefined;
  }
  return undefined;
};

/**
 * One SSE connection per tab, shared by every subscriber (multiplexed topics).
 * Reconnects with exponential backoff and `Last-Event-ID`, and reconnects when the topic set changes.
 * Events whose JSON has a `topic` go to that topic's subscribers; others (e.g. `resync`) go to all.
 */
export class EventStreamHub {
  private subs = new Map<string, Set<Handler>>();
  private statusListeners = new Set<() => void>();
  private controller: AbortController | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private watchdog: ReturnType<typeof setTimeout> | null = null;
  private attempt = 0;
  private openTopics = '';
  private closed = false;
  private scheduled = false;
  status: StreamStatus = 'idle';
  failure: StreamFailure | null = null;
  lastEventId: string | undefined;
  private serverRetry: number | undefined;
  private readonly opts: Required<EventStreamOptions>;

  constructor(
    private readonly open: OpenStream,
    options: EventStreamOptions = {},
  ) {
    this.opts = {
      minDelayMs: 500,
      maxDelayMs: 30_000,
      staleAfterMs: 45_000,
      random: Math.random,
      ...options,
    };
  }

  subscribe(topic: string, handler: Handler): () => void {
    let set = this.subs.get(topic);
    if (!set) {
      set = new Set();
      this.subs.set(topic, set);
    }
    set.add(handler);
    this.topicsChanged();
    return () => {
      const s = this.subs.get(topic);
      s?.delete(handler);
      if (s?.size === 0) this.subs.delete(topic);
      this.topicsChanged();
    };
  }

  onStatus(listener: () => void): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  close(): void {
    this.closed = true;
    this.stop();
    this.setStatus('idle');
  }

  private topics(): string[] {
    return [...this.subs.keys()].sort();
  }

  private setStatus(status: StreamStatus) {
    if (this.status === status) return;
    this.status = status;
    for (const l of this.statusListeners) l();
  }

  private stop() {
    this.controller?.abort();
    this.controller = null;
    if (this.timer) clearTimeout(this.timer);
    if (this.watchdog) clearTimeout(this.watchdog);
    this.timer = null;
    this.watchdog = null;
  }

  /** Batch subscribe/unsubscribe calls from one render into one reconnect. */
  private topicsChanged() {
    if (this.scheduled || this.closed) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      const key = this.topics().join(',');
      if (key === this.openTopics && this.status !== 'idle' && this.status !== 'failed') return;
      this.stop();
      this.attempt = 0;
      if (!key) {
        this.openTopics = '';
        this.setStatus('idle');
        return;
      }
      void this.connect();
    });
  }

  private resetWatchdog(controller: AbortController) {
    if (this.watchdog) clearTimeout(this.watchdog);
    this.watchdog = setTimeout(() => {
      controller.abort();
    }, this.opts.staleAfterMs);
  }

  private dispatch(event: StreamEvent) {
    if (event.id !== undefined) this.lastEventId = event.id;
    if (event.type === 'heartbeat') return;
    const topic = topicOf(event);
    const targets = topic ? [this.subs.get(topic)] : [...this.subs.values()];
    for (const set of targets) for (const h of set ?? []) h(event);
  }

  private async connect(): Promise<void> {
    const topics = this.topics();
    this.openTopics = topics.join(',');
    const controller = new AbortController();
    this.controller = controller;
    this.setStatus(
      this.attempt === 0 && this.lastEventId === undefined ? 'connecting' : 'reconnecting',
    );
    try {
      const response = await this.open({
        topics,
        lastEventId: this.lastEventId,
        signal: controller.signal,
      });
      if (!response.ok || !response.body) {
        if (FATAL_STATUS.has(response.status)) {
          let code: string | undefined;
          try {
            const body = (await response.json()) as { code?: unknown };
            code = typeof body.code === 'string' ? body.code : undefined;
          } catch {
            code = undefined;
          }
          this.failure = { status: response.status, code };
          this.setStatus('failed');
          return;
        }
        throw new Error(`stream answered ${response.status}`);
      }
      this.failure = null;
      this.attempt = 0;
      this.setStatus('live');
      this.resetWatchdog(controller);
      const parser = new SseParser((e) => {
        this.dispatch(e);
      });
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        this.resetWatchdog(controller);
        parser.push(value);
        this.serverRetry = parser.retry ?? this.serverRetry;
      }
    } catch (err) {
      // Clients that throw on error statuses (like @mrl/api-client) carry `status` and `code`.
      const status = (err as { status?: unknown } | null)?.status;
      if (typeof status === 'number' && FATAL_STATUS.has(status)) {
        const code = (err as { code?: unknown }).code;
        this.failure = { status, code: typeof code === 'string' ? code : undefined };
        this.setStatus('failed');
        return;
      }
      // Otherwise: network error, watchdog abort, or a retryable status. Retry below.
    }
    if (this.controller !== controller || this.closed) return;
    this.scheduleReconnect();
  }

  private scheduleReconnect() {
    if (this.watchdog) clearTimeout(this.watchdog);
    this.setStatus('reconnecting');
    const base = Math.max(this.opts.minDelayMs, this.serverRetry ?? 0);
    const delay = Math.min(this.opts.maxDelayMs, base * 2 ** this.attempt);
    this.attempt += 1;
    this.timer = setTimeout(
      () => {
        void this.connect();
      },
      delay * (0.5 + this.opts.random() * 0.5),
    );
  }
}
