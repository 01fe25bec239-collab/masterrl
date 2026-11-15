/** Ticket protocol: open (or resume) a ticket, PUT each part, then complete. */
export interface UploadTicket {
  uploadId: string;
  partSize: number;
  /** Parts the server already has; resuming skips them. */
  completedParts: UploadedPart[];
}

export interface UploadedPart {
  partNumber: number;
  etag: string;
}

export interface UploadFileInfo {
  name: string;
  size: number;
  type: string;
  /** Same file picked again → same key → the server can hand back the same ticket. */
  resumeKey: string;
}

export interface UploadTransport {
  open(file: UploadFileInfo): Promise<UploadTicket>;
  putPart(
    ticket: UploadTicket,
    partNumber: number,
    body: Blob,
    signal: AbortSignal,
  ): Promise<UploadedPart>;
  complete(ticket: UploadTicket, parts: UploadedPart[]): Promise<void>;
  abort?(ticket: UploadTicket): Promise<void>;
}

export type UploadStatus = 'waiting' | 'uploading' | 'paused' | 'failed' | 'done' | 'cancelled';

export interface UploadState {
  status: UploadStatus;
  sentBytes: number;
  totalBytes: number;
  /** Error code from the transport, for PlainError. */
  errorCode?: string;
  errorDetail?: string;
}

export const resumeKeyOf = (file: File) => `${file.name}:${file.size}:${file.lastModified}`;

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(t);
      reject(signal.reason as Error);
    });
  });

function errorInfo(err: unknown): { code?: string; detail?: string } {
  if (err && typeof err === 'object') {
    const e = err as { code?: unknown; message?: unknown };
    return {
      code: typeof e.code === 'string' ? e.code : undefined,
      detail: typeof e.message === 'string' ? e.message : undefined,
    };
  }
  return {};
}

/**
 * One file's upload. Parts go one at a time and each is retried 3 times.
 * ponytail: sequential parts; add a small worker pool if uploads are network-latency bound.
 */
export class FileUpload {
  state: UploadState;
  private ticket: UploadTicket | null = null;
  private parts = new Map<number, UploadedPart>();
  private controller: AbortController | null = null;
  private listeners = new Set<() => void>();

  constructor(
    readonly file: File,
    private readonly transport: UploadTransport,
    private readonly retryDelayMs = 500,
  ) {
    this.state = { status: 'waiting', sentBytes: 0, totalBytes: file.size };
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private set(patch: Partial<UploadState>) {
    this.state = { ...this.state, ...patch };
    for (const l of this.listeners) l();
  }

  private partCount(partSize: number) {
    return Math.max(1, Math.ceil(this.file.size / partSize));
  }

  private sentBytes(partSize: number) {
    let sent = 0;
    for (const n of this.parts.keys()) {
      sent += Math.min(partSize, this.file.size - (n - 1) * partSize);
    }
    return sent;
  }

  /** Start, or continue after pause/failure. */
  async start(): Promise<void> {
    if (this.state.status === 'uploading' || this.state.status === 'done') return;
    const controller = new AbortController();
    this.controller = controller;
    this.set({ status: 'uploading', errorCode: undefined, errorDetail: undefined });
    try {
      this.ticket ??= await this.transport.open({
        name: this.file.name,
        size: this.file.size,
        type: this.file.type,
        resumeKey: resumeKeyOf(this.file),
      });
      const ticket = this.ticket;
      for (const p of ticket.completedParts) this.parts.set(p.partNumber, p);
      this.set({ sentBytes: this.sentBytes(ticket.partSize) });

      const count = this.partCount(ticket.partSize);
      for (let n = 1; n <= count; n += 1) {
        if (this.parts.has(n)) continue;
        const body = this.file.slice((n - 1) * ticket.partSize, n * ticket.partSize);
        let attempt = 0;
        for (;;) {
          try {
            this.parts.set(n, await this.transport.putPart(ticket, n, body, controller.signal));
            break;
          } catch (err) {
            if (controller.signal.aborted || attempt >= 2) throw err;
            attempt += 1;
            await sleep(this.retryDelayMs * 2 ** attempt, controller.signal);
          }
        }
        this.set({ sentBytes: this.sentBytes(ticket.partSize) });
        // Paused or cancelled while this part was in flight: keep it, stop before the next.
        if (controller.signal.aborted) return;
      }
      await this.transport.complete(
        ticket,
        [...this.parts.values()].sort((a, b) => a.partNumber - b.partNumber),
      );
      this.set({ status: 'done', sentBytes: this.file.size });
    } catch (err) {
      if (controller.signal.aborted) return;
      const info = errorInfo(err);
      this.set({ status: 'failed', errorCode: info.code, errorDetail: info.detail });
    }
  }

  pause(): void {
    if (this.state.status !== 'uploading') return;
    this.controller?.abort(new DOMException('Paused', 'AbortError'));
    this.set({ status: 'paused' });
  }

  async cancel(): Promise<void> {
    this.controller?.abort(new DOMException('Cancelled', 'AbortError'));
    this.set({ status: 'cancelled' });
    if (this.ticket) await this.transport.abort?.(this.ticket);
  }
}
