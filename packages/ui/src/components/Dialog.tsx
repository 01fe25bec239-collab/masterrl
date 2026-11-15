import { useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { cn } from '../lib/cn';
import { Button } from './Button';

export interface DialogProps {
  open: boolean;
  onClose: () => void;
  title: string;
  /** Hide the visible title (it stays the accessible name). */
  hideTitle?: boolean;
  children: ReactNode;
  className?: string;
}

/** Modal built on the native <dialog>: focus trap, Escape and backdrop come from the browser. */
export function Dialog({ open, onClose, title, hideTitle, children, className }: DialogProps) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) {
      if (typeof el.showModal === 'function') el.showModal();
      else el.setAttribute('open', '');
    } else if (!open && el.open) {
      if (typeof el.close === 'function') el.close();
      else el.removeAttribute('open');
    }
  }, [open]);

  return (
    <dialog
      ref={ref}
      aria-labelledby={titleId}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
      className={cn(
        'm-auto w-[min(32rem,calc(100vw-2rem))] rounded-lg border border-border bg-bg p-0 text-fg shadow-xl',
        // The token preset removes Tailwind's palette, so the dim layer is a literal color.
        'backdrop:bg-[rgb(0_0_0/0.45)]',
        className,
      )}
    >
      {open && (
        <div className="p-5">
          <h2 id={titleId} className={cn('text-lg font-semibold', hideTitle && 'sr-only')}>
            {title}
          </h2>
          {children}
        </div>
      )}
    </dialog>
  );
}

export interface ConfirmDialogProps {
  open: boolean;
  title: string;
  /** Say plainly what will happen and whether it can be undone. */
  description: ReactNode;
  /** The words people must type to confirm, e.g. the run name. Omit for a simple yes/no. */
  confirmPhrase?: string;
  /** Button text, e.g. "Delete run". */
  actionLabel: string;
  /** Destructive actions get the danger style. */
  danger?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

export function ConfirmDialog({
  open,
  title,
  description,
  confirmPhrase,
  actionLabel,
  danger = true,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  const [typed, setTyped] = useState('');
  const inputId = useId();
  const matches = !confirmPhrase || typed.trim() === confirmPhrase;
  const close = () => {
    setTyped('');
    onCancel();
  };

  return (
    <Dialog open={open} onClose={close} title={title}>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (!matches) return;
          setTyped('');
          onConfirm();
        }}
      >
        <div className="mt-2 text-fg-muted">{description}</div>
        {confirmPhrase && (
          <div className="mt-4">
            <label htmlFor={inputId} className="block text-sm text-fg">
              To confirm, type <strong className="font-mono">{confirmPhrase}</strong>
            </label>
            <input
              id={inputId}
              className="mt-1 w-full rounded-md border border-border bg-bg px-3 py-2 text-fg"
              value={typed}
              autoComplete="off"
              spellCheck={false}
              onChange={(e) => {
                setTyped(e.target.value);
              }}
            />
          </div>
        )}
        <div className="mt-5 flex justify-end gap-2">
          <Button onClick={close}>Cancel</Button>
          <Button type="submit" variant={danger ? 'danger' : 'primary'} disabled={!matches}>
            {actionLabel}
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
