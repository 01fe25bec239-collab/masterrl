import { useId, useRef, useState, useSyncExternalStore } from 'react';
import { cn } from '../lib/cn';
import { formatBytes } from '../lib/format';
import { FileUpload, type UploadTransport } from '../lib/uploader';
import { Button } from './Button';
import { Icon } from './Icon';
import { PlainError } from './States';

export interface FileUploaderProps {
  transport: UploadTransport;
  /** e.g. `.jsonl,.parquet` */
  accept?: string;
  multiple?: boolean;
  /** Plain hint under the drop area, e.g. "JSONL or Parquet, up to 50 GB". */
  hint?: string;
  onUploaded?: (file: File) => void;
  label?: string;
}

function UploadRow({ upload, onRemove }: { upload: FileUpload; onRemove: () => void }) {
  const state = useSyncExternalStore(
    (cb) => upload.subscribe(cb),
    () => upload.state,
  );
  const pct = state.totalBytes === 0 ? 100 : Math.round((state.sentBytes / state.totalBytes) * 100);
  const statusText = {
    waiting: 'Waiting',
    uploading: `Uploading — ${pct}%`,
    paused: `Paused at ${pct}%`,
    failed: `Stopped at ${pct}%`,
    done: 'Uploaded',
    cancelled: 'Cancelled',
  }[state.status];

  return (
    <li className="rounded-md border border-border p-3">
      <div className="flex flex-wrap items-center gap-2">
        <Icon name={state.status === 'done' ? 'check' : 'file'} />
        <span className="font-medium text-fg">{upload.file.name}</span>
        <span className="text-sm text-fg-muted">{formatBytes(upload.file.size)}</span>
        <span className="ml-auto text-sm text-fg" aria-live="polite">
          {statusText}
        </span>
      </div>
      <progress
        className="mt-2 h-2 w-full accent-accent"
        max={100}
        value={pct}
        aria-label={`${upload.file.name} upload progress`}
      />
      <div className="mt-2 flex gap-2">
        {state.status === 'uploading' && (
          <Button
            size="sm"
            onClick={() => {
              upload.pause();
            }}
          >
            Pause
          </Button>
        )}
        {(state.status === 'paused' || state.status === 'failed') && (
          <Button size="sm" onClick={() => void upload.start()}>
            {state.status === 'failed' ? 'Try again' : 'Resume'}
          </Button>
        )}
        {state.status !== 'done' && state.status !== 'cancelled' && (
          <Button size="sm" variant="ghost" onClick={() => void upload.cancel()}>
            Cancel
          </Button>
        )}
        {(state.status === 'done' || state.status === 'cancelled') && (
          <Button size="sm" variant="ghost" onClick={onRemove}>
            Remove from list
          </Button>
        )}
      </div>
      {state.status === 'failed' && (
        <PlainError
          className="mt-2"
          headingLevel={3}
          code={state.errorCode}
          detail={state.errorDetail}
        />
      )}
    </li>
  );
}

/** Pick or drop files; each uploads in parts and can pause, resume and retry. */
export function FileUploader({
  transport,
  accept,
  multiple = true,
  hint,
  onUploaded,
  label = 'Upload files',
}: FileUploaderProps) {
  const [uploads, setUploads] = useState<FileUpload[]>([]);
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const hintId = useId();

  const add = (files: FileList | null) => {
    if (!files) return;
    const next = [...files].map((file) => {
      const upload = new FileUpload(file, transport);
      upload.subscribe(() => {
        if (upload.state.status === 'done') onUploaded?.(file);
      });
      void upload.start();
      return upload;
    });
    setUploads((prev) => [...prev, ...next]);
  };

  return (
    <div>
      {/* eslint-disable-next-line jsx-a11y/no-static-element-interactions -- drop target only; the button below is the keyboard path */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => {
          setDragging(false);
        }}
        onDrop={(e) => {
          e.preventDefault();
          setDragging(false);
          add(e.dataTransfer.files);
        }}
        className={cn(
          'flex flex-col items-center gap-2 rounded-md border-2 border-dashed p-6 text-center',
          dragging ? 'border-accent bg-surface' : 'border-border',
        )}
      >
        <Icon name="upload" className="size-6 text-fg-muted" />
        <p className="text-fg">Drop files here, or</p>
        <Button
          variant="primary"
          aria-describedby={hint ? hintId : undefined}
          onClick={() => inputRef.current?.click()}
        >
          {label}
        </Button>
        <input
          ref={inputRef}
          type="file"
          className="sr-only"
          tabIndex={-1}
          aria-hidden="true"
          accept={accept}
          multiple={multiple}
          onChange={(e) => {
            add(e.target.files);
            e.target.value = '';
          }}
        />
        {hint && (
          <p id={hintId} className="text-sm text-fg-muted">
            {hint}
          </p>
        )}
      </div>
      {uploads.length > 0 && (
        <ul className="mt-4 flex flex-col gap-2" aria-label="Uploads">
          {uploads.map((upload, i) => (
            <UploadRow
              key={`${upload.file.name}-${i}`}
              upload={upload}
              onRemove={() => {
                setUploads((prev) => prev.filter((u) => u !== upload));
              }}
            />
          ))}
        </ul>
      )}
    </div>
  );
}
