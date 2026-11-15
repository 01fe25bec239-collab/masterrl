import { useEffect, useState } from 'react';
import { cn } from '../lib/cn';
import { formatAge } from '../lib/format';
import { focusRing } from './Button';
import { Icon, type IconName } from './Icon';

const chip =
  'inline-flex items-center gap-1 rounded-full border border-border bg-surface px-2 py-0.5 text-xs font-medium';

export type Freshness = 'live' | 'sampled' | 'delayed' | 'stale';

const freshness: Record<Freshness, { label: string; icon: IconName; help: string; tone: string }> =
  {
    live: { label: 'Live', icon: 'radio', help: 'Updates as they happen.', tone: 'text-accent' },
    sampled: {
      label: 'Sampled',
      icon: 'chart',
      help: 'Some points are left out to keep the chart fast. Zoom in to see all of them.',
      tone: 'text-fg',
    },
    delayed: {
      label: 'Delayed',
      icon: 'clock',
      help: 'Updates arrive a little late.',
      tone: 'text-fg-muted',
    },
    stale: {
      label: 'Not up to date',
      icon: 'alert',
      help: 'Live updates stopped. What you see may be old.',
      tone: 'text-danger',
    },
  };

export interface FreshnessBadgeProps {
  freshness: Freshness;
  /** When the data was last updated; shown as "5 minutes ago". */
  asOf?: Date;
  className?: string;
}

export function FreshnessBadge({ freshness: kind, asOf, className }: FreshnessBadgeProps) {
  const f = freshness[kind];
  return (
    <span className={cn(chip, f.tone, className)} title={f.help}>
      <Icon name={f.icon} className="size-3.5" />
      {f.label}
      {asOf && kind !== 'live' && <span className="text-fg-muted">· {formatAge(asOf)}</span>}
    </span>
  );
}

export type Maturity = 'stable' | 'beta' | 'experimental';

const maturity: Record<Maturity, { label: string; icon: IconName; help: string }> = {
  stable: { label: 'Stable', icon: 'check', help: 'Tested and ready for everyday use.' },
  beta: { label: 'Beta', icon: 'beaker', help: 'Works, but may still change.' },
  experimental: {
    label: 'Experimental',
    icon: 'alert',
    help: 'Early and may break. Use it to try things out.',
  },
};

export function MaturityBadge({ level, className }: { level: Maturity; className?: string }) {
  const m = maturity[level];
  return (
    <span className={cn(chip, 'text-fg', className)} title={m.help}>
      <Icon name={m.icon} className="size-3.5" />
      {m.label}
    </span>
  );
}

export type LicenseKind = 'permissive' | 'copyleft' | 'restricted' | 'unknown';

const licenseKinds: Record<LicenseKind, { label: string; icon: IconName; help: string }> = {
  permissive: {
    label: 'Permissive',
    icon: 'check',
    help: 'You can use, change and share it, including for business.',
  },
  copyleft: {
    label: 'Share-alike',
    icon: 'info',
    help: 'If you share changes, you must share them under the same license.',
  },
  restricted: {
    label: 'Restricted',
    icon: 'lock',
    help: 'Has limits on use. Read the license before you rely on it.',
  },
  unknown: {
    label: 'License unknown',
    icon: 'alert',
    help: 'No license was found. Check with the owner before you use it.',
  },
};

export interface LicenseChipProps {
  /** SPDX id such as `Apache-2.0`, or a name such as `Llama 3 Community`. */
  license?: string;
  kind: LicenseKind;
  className?: string;
}

export function LicenseChip({ license, kind, className }: LicenseChipProps) {
  const k = licenseKinds[kind];
  return (
    <span
      className={cn(chip, kind === 'unknown' ? 'text-danger' : 'text-fg', className)}
      title={k.help}
    >
      <Icon name={k.icon} className="size-3.5" />
      {license ? `${license} · ${k.label}` : k.label}
    </span>
  );
}

export interface HashLabelProps {
  hash: string;
  /** Characters shown. */
  length?: number;
  /** What the hash identifies, for the copy button, e.g. "checkpoint hash". */
  label?: string;
  className?: string;
}

/** Short, copyable hash. The full value is in the title and goes to the clipboard. */
export function HashLabel({ hash, length = 8, label = 'hash', className }: HashLabelProps) {
  const [copied, setCopied] = useState(false);
  const short = hash.replace(/^sha256:/, '').slice(0, length);

  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => {
      setCopied(false);
    }, 1500);
    return () => {
      clearTimeout(t);
    };
  }, [copied]);

  return (
    <span className={cn('inline-flex items-center gap-1 font-mono text-xs', className)}>
      <span title={hash}>{short}</span>
      <button
        type="button"
        className={cn('rounded p-0.5 text-fg-muted hover:text-fg', focusRing)}
        aria-label={`Copy full ${label}`}
        onClick={() => {
          void navigator.clipboard.writeText(hash).then(() => {
            setCopied(true);
          });
        }}
      >
        <Icon name={copied ? 'check' : 'copy'} className="size-3.5" />
      </button>
      <span role="status" className="sr-only">
        {copied ? 'Copied' : ''}
      </span>
    </span>
  );
}
