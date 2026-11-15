import type { ButtonHTMLAttributes } from 'react';
import { cn } from '../lib/cn';

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

const variants: Record<ButtonVariant, string> = {
  primary: 'bg-accent text-accent-fg border-accent hover:opacity-90',
  secondary: 'bg-bg text-fg border-border hover:bg-surface',
  ghost: 'bg-transparent text-fg border-transparent hover:bg-surface',
  danger: 'bg-bg text-danger border-danger hover:bg-surface',
};

export const focusRing =
  'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: 'sm' | 'md';
}

export function Button({
  variant = 'secondary',
  size = 'md',
  className,
  type = 'button',
  ...rest
}: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(
        'inline-flex items-center justify-center gap-2 rounded-md border font-medium',
        'disabled:cursor-not-allowed disabled:opacity-50',
        size === 'sm' ? 'px-2 py-1 text-sm' : 'px-3 py-2 text-sm',
        variants[variant],
        focusRing,
        className,
      )}
      {...rest}
    />
  );
}

export interface SegmentedOption<T extends string> {
  value: T;
  label: string;
}

export interface SegmentedControlProps<T extends string> {
  label: string;
  options: readonly SegmentedOption<T>[];
  value: T;
  onChange: (value: T) => void;
  className?: string;
}

/** A small radio group drawn as joined buttons (theme switch, Beginner/Advanced). */
export function SegmentedControl<T extends string>({
  label,
  options,
  value,
  onChange,
  className,
}: SegmentedControlProps<T>) {
  return (
    <fieldset className={cn('inline-flex rounded-md border border-border', className)}>
      <legend className="sr-only">{label}</legend>
      {options.map((option) => (
        <label
          key={option.value}
          className={cn(
            'cursor-pointer px-2 py-1 text-sm first:rounded-l-md last:rounded-r-md',
            'has-[:focus-visible]:outline-2 has-[:focus-visible]:outline-accent',
            option.value === value ? 'bg-accent text-accent-fg' : 'bg-bg text-fg hover:bg-surface',
          )}
        >
          <input
            type="radio"
            className="sr-only"
            name={label}
            value={option.value}
            checked={option.value === value}
            onChange={() => {
              onChange(option.value);
            }}
          />
          {option.label}
        </label>
      ))}
    </fieldset>
  );
}
