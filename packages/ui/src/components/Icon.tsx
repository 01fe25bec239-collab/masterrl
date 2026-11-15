import { cn } from '../lib/cn';

/** Simple 24×24 stroke icons. Decorative by default: put the meaning in visible text. */
const paths = {
  check: ['M5 12.5l4.5 4.5L19 7.5'],
  x: ['M6 6l12 12', 'M18 6L6 18'],
  clock: ['M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18', 'M12 7v5l3 2'],
  progress: ['M12 3a9 9 0 1 0 9 9', 'M12 8v4l3 2'],
  pause: ['M9 6v12', 'M15 6v12'],
  play: ['M8 5l11 7l-11 7z'],
  ban: ['M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18', 'M5.6 5.6l12.8 12.8'],
  alert: ['M12 3l10 18H2z', 'M12 10v4', 'M12 17.5v.5'],
  circle: ['M12 5a7 7 0 1 0 0 14a7 7 0 1 0 0-14'],
  archive: ['M3 5h18v4H3z', 'M5 9v10h14V9', 'M10 13h4'],
  upload: ['M12 16V4', 'M7 9l5-5l5 5', 'M4 16v4h16v-4'],
  search: ['M11 4a7 7 0 1 0 0 14a7 7 0 1 0 0-14', 'M16.5 16.5L21 21'],
  chevronDown: ['M6 9l6 6l6-6'],
  chevronRight: ['M9 6l6 6l-6 6'],
  copy: ['M9 9h11v11H9z', 'M5 15H4V4h11v1'],
  sun: [
    'M12 8a4 4 0 1 0 0 8a4 4 0 1 0 0-8',
    'M12 2v2',
    'M12 20v2',
    'M4.9 4.9l1.4 1.4',
    'M17.7 17.7l1.4 1.4',
    'M2 12h2',
    'M20 12h2',
    'M4.9 19.1l1.4-1.4',
    'M17.7 6.3l1.4-1.4',
  ],
  moon: ['M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z'],
  home: ['M3 11l9-7l9 7', 'M5 10v10h14V10', 'M10 20v-6h4v6'],
  settings: [
    'M12 9a3 3 0 1 0 0 6a3 3 0 1 0 0-6',
    'M12 2v3',
    'M12 19v3',
    'M2 12h3',
    'M19 12h3',
    'M4.9 4.9l2.1 2.1',
    'M17 17l2.1 2.1',
    'M4.9 19.1L7 17',
    'M17 7l2.1-2.1',
  ],
  file: ['M6 3h8l4 4v14H6z', 'M14 3v4h4'],
  lock: ['M6 11h12v10H6z', 'M8 11V7a4 4 0 0 1 8 0v4'],
  compass: ['M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18', 'M15.5 8.5l-2 5l-5 2l2-5z'],
  info: ['M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18', 'M12 11v6', 'M12 7.5v.5'],
  grid: ['M4 4h7v7H4z', 'M13 4h7v7h-7z', 'M4 13h7v7H4z', 'M13 13h7v7h-7z'],
  chart: ['M4 4v16h16', 'M7 15l4-5l3 3l5-6'],
  list: ['M8 6h12', 'M8 12h12', 'M8 18h12', 'M4 6h.01', 'M4 12h.01', 'M4 18h.01'],
  radio: ['M12 9a3 3 0 1 0 0 6a3 3 0 1 0 0-6', 'M5 5a10 10 0 0 0 0 14', 'M19 5a10 10 0 0 1 0 14'],
  shield: ['M12 3l8 3v6c0 4.5-3.5 8-8 9c-4.5-1-8-4.5-8-9V6z'],
  beaker: ['M9 3h6', 'M10 3v6L4.5 19a1.5 1.5 0 0 0 1.3 2h12.4a1.5 1.5 0 0 0 1.3-2L14 9V3'],
  star: ['M12 3l2.7 5.6l6.1.9l-4.4 4.3l1 6.1L12 17l-5.4 2.9l1-6.1l-4.4-4.3l6.1-.9z'],
} as const;

export type IconName = keyof typeof paths;
export const iconNames = Object.keys(paths) as IconName[];

export interface IconProps {
  name: IconName;
  className?: string;
  /** Give a label only when the icon stands alone; otherwise it is hidden from screen readers. */
  label?: string;
}

export function Icon({ name, className, label }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={cn('inline-block size-4 shrink-0', className)}
      role={label ? 'img' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      focusable="false"
    >
      {paths[name].map((d) => (
        <path key={d} d={d} />
      ))}
    </svg>
  );
}
