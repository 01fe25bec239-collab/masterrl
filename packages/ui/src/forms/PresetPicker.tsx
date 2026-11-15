import { cn } from '../lib/cn';
import { Icon } from '../components/Icon';
import type { JsonObject } from './schema';

export interface Preset {
  id: string;
  title: string;
  summary: string;
  /** Values laid over the form defaults when picked. */
  values: JsonObject;
  recommended?: boolean;
}

export interface PresetPickerProps {
  label: string;
  presets: Preset[];
  /** Selected preset id, or undefined when values were changed by hand. */
  value: string | undefined;
  onChange: (preset: Preset) => void;
}

/** Pick a starting point; shown as a radio group of cards. */
export function PresetPicker({ label, presets, value, onChange }: PresetPickerProps) {
  return (
    <fieldset>
      <legend className="font-semibold text-fg">{label}</legend>
      <div className="mt-2 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {presets.map((p) => (
          <label
            key={p.id}
            className={cn(
              'flex cursor-pointer flex-col gap-1 rounded-md border p-3',
              'has-[:focus-visible]:outline-2 has-[:focus-visible]:outline-accent',
              p.id === value ? 'border-accent bg-surface' : 'border-border hover:bg-surface',
            )}
          >
            <span className="flex items-center gap-2">
              <input
                type="radio"
                name={label}
                className="accent-accent"
                checked={p.id === value}
                onChange={() => {
                  onChange(p);
                }}
              />
              <span className="font-medium text-fg">{p.title}</span>
              {p.recommended && (
                <span className="ml-auto inline-flex items-center gap-1 text-xs text-accent">
                  <Icon name="star" className="size-3" />
                  Recommended
                </span>
              )}
            </span>
            <span className="text-sm text-fg-muted">{p.summary}</span>
          </label>
        ))}
      </div>
      {value === undefined && (
        <p className="mt-2 text-sm text-fg-muted">
          You changed some values by hand, so no preset is selected.
        </p>
      )}
    </fieldset>
  );
}
