/** Short number for labels: 1234 → "1,234", 0.000123 → "1.23e-4". */
export function formatNumber(value: number | null | undefined, unit?: string): string {
  if (value == null || Number.isNaN(value)) return 'no value';
  const abs = Math.abs(value);
  const text =
    abs !== 0 && (abs < 0.001 || abs >= 1e9)
      ? value.toExponential(2)
      : value.toLocaleString('en-US', { maximumFractionDigits: abs < 1 ? 4 : 2 });
  return unit ? `${text} ${unit}` : text;
}

export interface Range {
  min: number;
  max: number;
}

/** "2.1–3.4 GB", or "3 GB" when both ends match. */
export function formatRange({ min, max }: Range, unit?: string): string {
  const text = min === max ? formatNumber(min) : `${formatNumber(min)}–${formatNumber(max)}`;
  return unit ? `${text} ${unit}` : text;
}

/** "just now", "5 minutes ago", "3 hours ago", or a date for older times. */
export function formatAge(date: Date, now: Date = new Date()): string {
  const seconds = Math.round((now.getTime() - date.getTime()) / 1000);
  if (seconds < 10) return 'just now';
  if (seconds < 60) return `${seconds} seconds ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return minutes === 1 ? '1 minute ago' : `${minutes} minutes ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return hours === 1 ? '1 hour ago' : `${hours} hours ago`;
  return date.toLocaleDateString('en-US', { dateStyle: 'medium' });
}

export function formatDateTime(date: Date): string {
  return date.toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' });
}

export function formatBytes(bytes: number): string {
  const units = ['bytes', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i += 1;
  }
  return `${i === 0 ? value : value.toFixed(1)} ${units[i] ?? 'bytes'}`;
}
