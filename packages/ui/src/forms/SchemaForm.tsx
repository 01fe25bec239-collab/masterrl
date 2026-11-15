import { useId, useMemo, useRef, useState, type ReactNode } from 'react';
import { Button } from '../components/Button';
import { Icon } from '../components/Icon';
import { cn } from '../lib/cn';
import {
  fieldPointerFor,
  getIn,
  isActive,
  pointerOf,
  resolveRef,
  setIn,
  validateValue,
  type FieldErrors,
  type JsonObject,
  type JsonSchema,
  type JsonValue,
  type Level,
} from './schema';

export interface SchemaFormProps {
  schema: JsonSchema;
  value: JsonObject;
  onChange: (value: JsonObject) => void;
  /** Beginner hides fields marked `x-mrl-level: advanced` (their values are kept). */
  level: Level;
  /** Errors from the server (RFC 9457 `errors[]`), keyed by JSON pointer. */
  externalErrors?: Record<string, string>;
  /** With this, the form renders a submit button and only calls it when valid. */
  onSubmit?: (value: JsonObject) => void;
  submitLabel?: string;
  /** Accessible name of the form. */
  label: string;
}

const inputClass =
  'w-full rounded-md border bg-bg px-3 py-2 text-fg focus-visible:outline-2 focus-visible:outline-accent';

/** Display text for any JSON value. */
const textOf = (v: JsonValue | undefined): string =>
  v === undefined || v === null
    ? ''
    : typeof v === 'object'
      ? JSON.stringify(v)
      : typeof v === 'string'
        ? v
        : String(v);

const humanize = (key: string) => {
  const words = key.replace(/[_-]+/g, ' ').trim();
  return words.charAt(0).toUpperCase() + words.slice(1);
};

interface FieldContext {
  root: JsonSchema;
  value: JsonObject;
  level: Level;
  errors: FieldErrors;
  visibleErrors: (pointer: string) => string | undefined;
  set: (path: string[], next: JsonValue | undefined) => void;
  touch: (pointer: string) => void;
  idBase: string;
}

/** Advanced fields that differ from their default, for the "custom" badge in Beginner mode. */
function customAdvanced(
  schema: JsonSchema,
  root: JsonSchema,
  value: JsonValue | undefined,
  path: string[] = [],
): string[] {
  const s = resolveRef(schema, root);
  const out: string[] = [];
  for (const [key, raw] of Object.entries(s.properties ?? {})) {
    const p = resolveRef(raw, root);
    const v = getIn(value, [...path, key]);
    if (p.type === 'object') out.push(...customAdvanced(p, root, value, [...path, key]));
    else if (
      p['x-mrl-level'] === 'advanced' &&
      v !== undefined &&
      JSON.stringify(v) !== JSON.stringify(p.default)
    ) {
      out.push(p.title ?? humanize(key));
    }
  }
  return out;
}

function hiddenErrorCount(
  schema: JsonSchema,
  root: JsonSchema,
  value: JsonValue | undefined,
  errors: FieldErrors,
  path: string[] = [],
): number {
  const s = resolveRef(schema, root);
  let n = 0;
  for (const [key, raw] of Object.entries(s.properties ?? {})) {
    const p = resolveRef(raw, root);
    const childPath = [...path, key];
    if (!isActive(p, getIn(value, path))) continue;
    if (p['x-mrl-level'] === 'advanced') {
      const prefix = pointerOf(childPath);
      for (const k of errors.keys()) if (k === prefix || k.startsWith(`${prefix}/`)) n += 1;
    } else if (p.type === 'object') n += hiddenErrorCount(p, root, value, errors, childPath);
  }
  return n;
}

function NumberInput({
  id,
  value,
  onChange,
  onBlur,
  describedBy,
  invalid,
}: {
  id: string;
  value: JsonValue | undefined;
  onChange: (v: JsonValue | undefined) => void;
  onBlur: () => void;
  describedBy: string | undefined;
  invalid: boolean;
}) {
  const shown = textOf(value);
  const [text, setText] = useState(shown);
  const [prev, setPrev] = useState(value);
  // Adopt outside changes (presets, YAML edits) without fighting the person typing.
  if (prev !== value) {
    setPrev(value);
    if (!(typeof value === 'number' && Number(text) === value)) setText(shown);
  }
  return (
    <input
      id={id}
      type="text"
      inputMode="decimal"
      className={cn(inputClass, invalid ? 'border-danger' : 'border-border')}
      aria-invalid={invalid || undefined}
      aria-describedby={describedBy}
      value={text}
      onBlur={onBlur}
      onChange={(e) => {
        const t = e.target.value;
        setText(t);
        const n = Number(t);
        onChange(t.trim() === '' ? undefined : Number.isFinite(n) ? n : t);
      }}
    />
  );
}

/** One item per line. Keeps the raw text so blank lines can be typed before the next item. */
function ListInput({
  id,
  numeric,
  value,
  onChange,
  onBlur,
  describedBy,
  invalid,
}: {
  id: string;
  numeric: boolean;
  value: JsonValue | undefined;
  onChange: (v: JsonValue | undefined) => void;
  onBlur: () => void;
  describedBy: string | undefined;
  invalid: boolean;
}) {
  const parse = (t: string): JsonValue[] => {
    const items = t.split('\n').filter((line) => line.trim() !== '');
    return numeric
      ? items.map((line) => (Number.isFinite(Number(line)) ? Number(line) : line))
      : items;
  };
  const shown = Array.isArray(value) ? value.map(textOf).join('\n') : '';
  const [text, setText] = useState(shown);
  const [prev, setPrev] = useState(value);
  if (prev !== value) {
    setPrev(value);
    if (JSON.stringify(parse(text)) !== JSON.stringify(value ?? [])) setText(shown);
  }
  return (
    <textarea
      id={id}
      rows={3}
      className={cn(inputClass, invalid ? 'border-danger' : 'border-border', 'font-mono')}
      value={text}
      aria-invalid={invalid || undefined}
      aria-describedby={describedBy}
      onBlur={onBlur}
      onChange={(e) => {
        setText(e.target.value);
        const items = parse(e.target.value);
        onChange(items.length === 0 ? undefined : items);
      }}
    />
  );
}

function Field({
  name,
  schema: raw,
  path,
  ctx,
}: {
  name: string;
  schema: JsonSchema;
  path: string[];
  ctx: FieldContext;
}) {
  const s = resolveRef(raw, ctx.root);
  const pointer = pointerOf(path);
  const id = `${ctx.idBase}${pointer.replace(/\//g, '-')}`;
  const value = getIn(ctx.value, path);
  const error = ctx.visibleErrors(pointer);
  const label = s.title ?? humanize(name);
  const help = s['x-mrl-help'] ?? s.description;
  const unit = s['x-mrl-unit'];
  const helpId = help ? `${id}-help` : undefined;
  const errorId = error ? `${id}-error` : undefined;
  const describedBy = [helpId, errorId].filter(Boolean).join(' ') || undefined;
  const set = (v: JsonValue | undefined) => {
    ctx.set(path, v);
  };
  const blur = () => {
    ctx.touch(pointer);
  };
  const widget =
    s['x-mrl-widget'] ??
    (s.enum
      ? 'select'
      : s.type === 'boolean'
        ? 'checkbox'
        : s.type === 'number' || s.type === 'integer'
          ? 'number'
          : s.type === 'array'
            ? 'list'
            : 'text');

  if (s.type === 'object') {
    return <ObjectFields schema={s} path={path} ctx={ctx} legend={label} help={help} />;
  }

  let control: ReactNode;
  const invalid = Boolean(error);
  const border = invalid ? 'border-danger' : 'border-border';
  const enumLabels = s['x-mrl-enumLabels'];
  const options = (s.enum ?? []).map((v, i) => ({ value: v, label: enumLabels?.[i] ?? textOf(v) }));

  switch (widget) {
    case 'checkbox':
      return (
        <div className="flex items-start gap-2">
          <input
            id={id}
            type="checkbox"
            className="mt-1 size-4 accent-accent"
            checked={value === true}
            aria-describedby={describedBy}
            onChange={(e) => {
              set(e.target.checked);
            }}
            onBlur={blur}
          />
          <div>
            <label htmlFor={id} className="font-medium text-fg">
              {label}
            </label>
            {help && (
              <p id={helpId} className="text-sm text-fg-muted">
                {help}
              </p>
            )}
            {error && (
              <p id={errorId} className="text-sm text-danger">
                {error}
              </p>
            )}
          </div>
        </div>
      );
    case 'radio':
      return (
        <fieldset aria-describedby={describedBy}>
          <legend className="font-medium text-fg">{label}</legend>
          {help && (
            <p id={helpId} className="text-sm text-fg-muted">
              {help}
            </p>
          )}
          <div className="mt-1 flex flex-wrap gap-4">
            {options.map((o) => (
              <label key={JSON.stringify(o.value)} className="flex items-center gap-2 text-fg">
                <input
                  type="radio"
                  name={id}
                  className="accent-accent"
                  checked={JSON.stringify(o.value) === JSON.stringify(value)}
                  onChange={() => {
                    set(o.value);
                  }}
                  onBlur={blur}
                />
                {o.label}
              </label>
            ))}
          </div>
          {error && (
            <p id={errorId} className="text-sm text-danger">
              {error}
            </p>
          )}
        </fieldset>
      );
    case 'select':
      control = (
        <select
          id={id}
          className={cn(inputClass, border)}
          value={value === undefined ? '' : JSON.stringify(value)}
          aria-invalid={invalid || undefined}
          aria-describedby={describedBy}
          onBlur={blur}
          onChange={(e) => {
            set(e.target.value === '' ? undefined : (JSON.parse(e.target.value) as JsonValue));
          }}
        >
          {value === undefined && <option value="">Choose…</option>}
          {options.map((o) => (
            <option key={JSON.stringify(o.value)} value={JSON.stringify(o.value)}>
              {o.label}
            </option>
          ))}
        </select>
      );
      break;
    case 'slider':
      control = (
        <div className="flex items-center gap-3">
          <input
            id={id}
            type="range"
            className="w-full accent-accent"
            min={s.minimum ?? s.exclusiveMinimum ?? 0}
            max={s.maximum ?? s.exclusiveMaximum ?? 100}
            step={s.multipleOf ?? (s.type === 'integer' ? 1 : 'any')}
            value={typeof value === 'number' ? value : (s.minimum ?? 0)}
            aria-describedby={describedBy}
            aria-valuetext={`${textOf(value)}${unit ? ` ${unit}` : ''}`}
            onBlur={blur}
            onChange={(e) => {
              set(Number(e.target.value));
            }}
          />
          <output htmlFor={id} className="min-w-16 text-right text-fg">
            {textOf(value)}
            {unit ? ` ${unit}` : ''}
          </output>
        </div>
      );
      break;
    case 'number':
      control = (
        <NumberInput
          id={id}
          value={value}
          onChange={set}
          onBlur={blur}
          describedBy={describedBy}
          invalid={invalid}
        />
      );
      break;
    case 'textarea':
      control = (
        <textarea
          id={id}
          rows={4}
          className={cn(inputClass, border)}
          value={typeof value === 'string' ? value : ''}
          aria-invalid={invalid || undefined}
          aria-describedby={describedBy}
          onBlur={blur}
          onChange={(e) => {
            set(e.target.value);
          }}
        />
      );
      break;
    case 'list':
      control = (
        <ListInput
          id={id}
          numeric={s.items?.type === 'number' || s.items?.type === 'integer'}
          value={value}
          onChange={set}
          onBlur={blur}
          describedBy={describedBy}
          invalid={invalid}
        />
      );
      break;
    default:
      control = (
        <input
          id={id}
          type="text"
          className={cn(inputClass, border)}
          value={typeof value === 'string' ? value : ''}
          aria-invalid={invalid || undefined}
          aria-describedby={describedBy}
          onBlur={blur}
          onChange={(e) => {
            set(e.target.value === '' ? undefined : e.target.value);
          }}
        />
      );
  }

  return (
    <div>
      <label htmlFor={id} className="font-medium text-fg">
        {label}
        {unit && widget !== 'slider' && (
          <span className="font-normal text-fg-muted"> ({unit})</span>
        )}
      </label>
      {help && (
        <p id={helpId} className="text-sm text-fg-muted">
          {help}
          {widget === 'list' ? ' One per line.' : ''}
        </p>
      )}
      <div className="mt-1">{control}</div>
      {error && (
        <p id={errorId} className="mt-1 flex items-center gap-1 text-sm text-danger">
          <Icon name="alert" className="size-3.5" />
          {error}
        </p>
      )}
    </div>
  );
}

function ObjectFields({
  schema,
  path,
  ctx,
  legend,
  help,
}: {
  schema: JsonSchema;
  path: string[];
  ctx: FieldContext;
  legend?: string;
  help?: string;
}) {
  const siblings = getIn(ctx.value, path);
  const entries = Object.entries(schema.properties ?? {}).filter(([, raw]) => {
    const p = resolveRef(raw, ctx.root);
    return (ctx.level === 'advanced' || p['x-mrl-level'] !== 'advanced') && isActive(p, siblings);
  });
  const groups = new Map<string, [string, JsonSchema][]>();
  for (const entry of entries) {
    const g = resolveRef(entry[1], ctx.root)['x-mrl-group'] ?? '';
    groups.set(g, [...(groups.get(g) ?? []), entry]);
  }
  if (entries.length === 0) return null;

  const body = [...groups].map(([group, fields]) => {
    const list = fields.map(([key, s]) => (
      <Field key={key} name={key} schema={s} path={[...path, key]} ctx={ctx} />
    ));
    return group ? (
      <fieldset key={group} className="flex flex-col gap-4 rounded-md border border-border p-4">
        <legend className="px-1 font-semibold text-fg">{group}</legend>
        {list}
      </fieldset>
    ) : (
      <div key="" className="flex flex-col gap-4">
        {list}
      </div>
    );
  });

  if (!legend) return <div className="flex flex-col gap-6">{body}</div>;
  return (
    <fieldset className="flex flex-col gap-4 rounded-md border border-border p-4">
      <legend className="px-1 font-semibold text-fg">{legend}</legend>
      {help && <p className="text-sm text-fg-muted">{help}</p>}
      {body}
    </fieldset>
  );
}

/** Renders a JSON Schema 2020-12 object with `x-mrl-*` hints; validates in plain words. */
export function SchemaForm({
  schema,
  value,
  onChange,
  level,
  externalErrors,
  onSubmit,
  submitLabel = 'Save',
  label,
}: SchemaFormProps) {
  const [touched, setTouched] = useState<ReadonlySet<string>>(new Set());
  const [submitted, setSubmitted] = useState(false);
  const idBase = useId();
  const formRef = useRef<HTMLFormElement>(null);
  // Every problem lands on the field that can show it; the rest are listed above the fields.
  const { errors, orphans } = useMemo(() => {
    const byField: FieldErrors = new Map();
    const other: { pointer: string; message: string }[] = [];
    const add = (pointer: string, message: string, replace: boolean) => {
      const field = fieldPointerFor(schema, pointer);
      if (field === null) other.push({ pointer, message });
      else if (replace || !byField.has(field)) byField.set(field, message);
    };
    for (const [k, v] of validateValue(schema, value)) add(k, v, false);
    for (const [k, v] of Object.entries(externalErrors ?? {})) add(k, v, true);
    return { errors: byField, orphans: other };
  }, [schema, value, externalErrors]);
  const problemCount = errors.size + orphans.length;

  const root = resolveRef(schema, schema);
  const ctx: FieldContext = {
    root: schema,
    value,
    level,
    errors,
    visibleErrors: (p) =>
      submitted || touched.has(p) || externalErrors?.[p] !== undefined ? errors.get(p) : undefined,
    set: (path, next) => {
      onChange(setIn(value, path, next));
    },
    touch: (p) => {
      if (!touched.has(p)) setTouched(new Set(touched).add(p));
    },
    idBase,
  };
  const custom = level === 'beginner' ? customAdvanced(root, schema, value) : [];
  const hidden = level === 'beginner' ? hiddenErrorCount(root, schema, value, errors) : 0;

  const fields = (
    <>
      {custom.length > 0 && (
        <p className="mb-4 inline-flex items-center gap-2 rounded-full border border-border bg-surface px-3 py-1 text-sm text-fg">
          <Icon name="settings" />
          Custom: {custom.length} advanced {custom.length === 1 ? 'setting' : 'settings'} changed (
          {custom.join(', ')})
        </p>
      )}
      {(submitted || externalErrors) && hidden > 0 && (
        <p role="alert" className="mb-4 flex items-center gap-2 text-danger">
          <Icon name="alert" />
          {hidden} advanced {hidden === 1 ? 'setting has a problem' : 'settings have problems'}.
          Switch to Advanced to fix {hidden === 1 ? 'it' : 'them'}.
        </p>
      )}
      {orphans.length > 0 && (
        <div role="alert" className="mb-4 rounded-md border border-danger p-3 text-fg">
          <p className="flex items-center gap-2 font-medium text-danger">
            <Icon name="alert" />
            Other problems
          </p>
          <ul className="ml-6 list-disc text-sm">
            {orphans.map((o) => (
              <li key={`${o.pointer}:${o.message}`}>
                {o.pointer ? `${o.pointer.slice(1).replace(/\//g, '.')}: ` : ''}
                {o.message}
              </li>
            ))}
          </ul>
        </div>
      )}
      <ObjectFields schema={root} path={[]} ctx={ctx} />
    </>
  );

  if (!onSubmit) {
    return (
      <div role="group" aria-label={label}>
        {fields}
      </div>
    );
  }

  return (
    <form
      ref={formRef}
      aria-label={label}
      noValidate
      onSubmit={(e) => {
        e.preventDefault();
        setSubmitted(true);
        if (problemCount === 0) {
          onSubmit(value);
          return;
        }
        requestAnimationFrame(() => {
          formRef.current?.querySelector<HTMLElement>('[aria-invalid="true"]')?.focus();
        });
      }}
    >
      {submitted && problemCount > 0 && (
        <p role="alert" className="mb-4 flex items-center gap-2 font-medium text-danger">
          <Icon name="alert" />
          {problemCount === 1
            ? '1 setting needs attention.'
            : `${problemCount} settings need attention.`}
        </p>
      )}
      {fields}
      <div className="mt-6">
        <Button type="submit" variant="primary">
          {submitLabel}
        </Button>
      </div>
    </form>
  );
}
