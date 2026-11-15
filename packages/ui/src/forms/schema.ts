import { Validator } from '@cfworker/json-schema';

export type JsonValue =
  string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export type Level = 'beginner' | 'advanced';
export type Widget =
  'text' | 'textarea' | 'number' | 'slider' | 'select' | 'radio' | 'checkbox' | 'list';

/** Show a field only when a sibling field has a given value (or, with neither key, is truthy). */
export interface DependsOn {
  field: string;
  equals?: JsonValue;
  oneOf?: JsonValue[];
}

/** The JSON Schema 2020-12 subset SchemaForm renders, plus `x-mrl-*` UI hints. */
export interface JsonSchema {
  $schema?: string;
  $id?: string;
  $ref?: string;
  $defs?: Record<string, JsonSchema>;
  title?: string;
  description?: string;
  type?: 'object' | 'string' | 'number' | 'integer' | 'boolean' | 'array' | 'null';
  properties?: Record<string, JsonSchema>;
  required?: string[];
  additionalProperties?: boolean | JsonSchema;
  items?: JsonSchema;
  enum?: JsonValue[];
  const?: JsonValue;
  default?: JsonValue;
  examples?: JsonValue[];
  minimum?: number;
  maximum?: number;
  exclusiveMinimum?: number;
  exclusiveMaximum?: number;
  multipleOf?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  minItems?: number;
  maxItems?: number;
  uniqueItems?: boolean;
  minProperties?: number;
  maxProperties?: number;
  'x-mrl-level'?: Level;
  'x-mrl-group'?: string;
  'x-mrl-widget'?: Widget;
  'x-mrl-unit'?: string;
  'x-mrl-help'?: string;
  'x-mrl-dependsOn'?: string | DependsOn;
  /** Labels for enum values, same order as `enum`. */
  'x-mrl-enumLabels'?: string[];
}

/** JSON pointer of a field, e.g. `/lora/r`. */
export const pointerOf = (path: readonly string[]) =>
  path.map((p) => '/' + p.replace(/~/g, '~0').replace(/\//g, '~1')).join('');

export function resolveRef(schema: JsonSchema, root: JsonSchema): JsonSchema {
  let s = schema;
  for (let hops = 0; s.$ref && hops < 16; hops += 1) {
    const match = /^#\/\$defs\/(.+)$/.exec(s.$ref);
    const target = match?.[1] ? root.$defs?.[match[1]] : undefined;
    if (!target) throw new Error(`The form schema points to ${s.$ref}, which it does not define.`);
    const rest: JsonSchema = { ...s };
    delete rest.$ref;
    s = { ...target, ...rest };
  }
  return s;
}

export function getIn(
  value: JsonValue | undefined,
  path: readonly string[],
): JsonValue | undefined {
  let v = value;
  for (const key of path) {
    if (v === null || typeof v !== 'object' || Array.isArray(v)) return undefined;
    v = v[key];
  }
  return v;
}

/** Copy of `obj` with `path` set to `next` (undefined removes the key). */
export function setIn(
  obj: JsonObject,
  path: readonly string[],
  next: JsonValue | undefined,
): JsonObject {
  const [head, ...rest] = path;
  if (head === undefined) return obj;
  const copy: JsonObject = { ...obj };
  if (rest.length === 0) {
    if (next !== undefined) return { ...copy, [head]: next };
    return Object.fromEntries(Object.entries(copy).filter(([k]) => k !== head));
  }
  const child = copy[head];
  copy[head] = setIn(
    child && typeof child === 'object' && !Array.isArray(child) ? child : {},
    rest,
    next,
  );
  return copy;
}

/** Defaults for every field that has one (nested objects included). */
export function defaultsOf(schema: JsonSchema, root: JsonSchema = schema): JsonObject {
  const s = resolveRef(schema, root);
  const out: JsonObject = {};
  for (const [key, raw] of Object.entries(s.properties ?? {})) {
    const p = resolveRef(raw, root);
    if (p.type === 'object' && p.properties) out[key] = defaultsOf(p, root);
    else if (p.default !== undefined) out[key] = p.default;
  }
  return out;
}

export function isActive(field: JsonSchema, siblings: JsonValue | undefined): boolean {
  const dep = field['x-mrl-dependsOn'];
  if (!dep) return true;
  const spec: DependsOn = typeof dep === 'string' ? { field: dep } : dep;
  const v = getIn(siblings, [spec.field]);
  if (spec.oneOf) return spec.oneOf.some((o) => JSON.stringify(o) === JSON.stringify(v));
  if (spec.equals !== undefined) return JSON.stringify(spec.equals) === JSON.stringify(v);
  return Boolean(v);
}

/** False when the field, or an object it sits in, is hidden by `x-mrl-dependsOn`. */
export function isPathActive(root: JsonSchema, value: JsonValue, path: readonly string[]): boolean {
  let s: JsonSchema | undefined = resolveRef(root, root);
  for (let i = 0; i < path.length; i += 1) {
    const child: JsonSchema | undefined = s.properties?.[path[i] as string];
    if (!child) return true;
    s = resolveRef(child, root);
    if (!isActive(s, getIn(value, path.slice(0, i)))) return false;
  }
  return true;
}

const unitSuffix = (s: JsonSchema) => (s['x-mrl-unit'] ? ` ${s['x-mrl-unit']}` : '');

/**
 * Keywords that usually summarize failures reported at or below their location
 * (`contains` often reports nothing below it). They are shown only when nothing more
 * specific is.
 */
const SUMMARY_KEYWORDS = new Set([
  'properties',
  'patternProperties',
  'additionalProperties',
  'unevaluatedProperties',
  'items',
  'prefixItems',
  'unevaluatedItems',
  'contains',
  'propertyNames',
  '$ref',
  '$dynamicRef',
  'allOf',
  'dependentSchemas',
  'if',
  'then',
  'else',
]);

/** Plain words for one failed keyword. Every keyword gets a message; unknown ones a generic one. */
function plainMessage(keyword: string, s: JsonSchema, key: string | undefined): string {
  switch (keyword) {
    case 'false':
      return key === undefined
        ? 'This value isn’t allowed here.'
        : `“${key}” isn’t a setting this form knows. Remove it or check its spelling.`;
    case 'type':
      return s.type === 'integer'
        ? 'Enter a whole number.'
        : s.type === 'number'
          ? 'Enter a number.'
          : s.type === 'boolean'
            ? 'Choose yes or no.'
            : 'This value has the wrong kind.';
    case 'minimum':
      return `Use ${s.minimum ?? ''}${unitSuffix(s)} or more.`;
    case 'maximum':
      return `Use ${s.maximum ?? ''}${unitSuffix(s)} or less.`;
    case 'exclusiveMinimum':
      return `Use more than ${s.exclusiveMinimum ?? ''}${unitSuffix(s)}.`;
    case 'exclusiveMaximum':
      return `Use less than ${s.exclusiveMaximum ?? ''}${unitSuffix(s)}.`;
    case 'multipleOf':
      return `Use a multiple of ${s.multipleOf ?? ''}.`;
    case 'minLength':
      return s.minLength === 1
        ? 'This can’t be empty.'
        : `Use at least ${s.minLength ?? ''} characters.`;
    case 'maxLength':
      return `Use at most ${s.maxLength ?? ''} characters.`;
    case 'pattern':
      return 'This doesn’t look right. Check the format in the help text.';
    case 'enum':
    case 'const':
      return 'Choose one of the options.';
    case 'minItems':
      return `Add at least ${s.minItems ?? ''} items.`;
    case 'maxItems':
      return `Use at most ${s.maxItems ?? ''} items.`;
    case 'contains':
      return 'Add at least one item of the kind this list needs.';
    case 'minContains':
    case 'maxContains':
      return 'This list has the wrong number of items of the needed kind.';
    case 'uniqueItems':
      return 'Remove the repeated items.';
    case 'oneOf':
    case 'anyOf':
      return 'This value doesn’t match any of the allowed kinds.';
    case 'not':
      return 'This value isn’t allowed here.';
    default:
      return 'This value isn’t allowed here. Check the help text.';
  }
}

function schemaAt(root: JsonSchema, path: readonly string[]): JsonSchema | undefined {
  let s: JsonSchema | undefined = resolveRef(root, root);
  for (const key of path) {
    if (!s) return undefined;
    s = s.type === 'array' ? s.items : s.properties?.[key];
    s = s ? resolveRef(s, root) : undefined;
  }
  return s;
}

const decode = (seg: string) => seg.replace(/~1/g, '/').replace(/~0/g, '~');

/**
 * The form field that shows an error at `pointer`: the nearest leaf field at or above it.
 * Null when no field can show it (an unknown key, or a rule on a whole object).
 */
export function fieldPointerFor(root: JsonSchema, pointer: string): string | null {
  const path = pointer.split('/').slice(1).map(decode);
  let s: JsonSchema = resolveRef(root, root);
  for (let i = 0; i < path.length; i += 1) {
    const child = s.properties?.[path[i] as string];
    if (!child) return null;
    s = resolveRef(child, root);
    if (s.type !== 'object') return pointerOf(path.slice(0, i + 1));
  }
  return null;
}

/** Field errors keyed by JSON pointer, in plain words. One message per location. */
export type FieldErrors = Map<string, string>;

export function validateValue(schema: JsonSchema, value: JsonValue): FieldErrors {
  const validator = new Validator(schema, '2020-12', false);
  const result = validator.validate(value);
  const errors: FieldErrors = new Map();
  const summaries: { pointer: string; message: string }[] = [];
  for (const unit of result.errors) {
    const path = unit.instanceLocation.replace(/^#/, '').split('/').slice(1).map(decode);
    if (unit.keyword === 'required') {
      const name = /"([^"]+)"/.exec(unit.error)?.[1];
      if (name === undefined) continue;
      const pointer = pointerOf([...path, name]);
      if (!errors.has(pointer)) errors.set(pointer, 'This is required.');
      continue;
    }
    const message = plainMessage(unit.keyword, schemaAt(schema, path) ?? {}, path.at(-1));
    const pointer = pointerOf(path);
    if (SUMMARY_KEYWORDS.has(unit.keyword)) summaries.push({ pointer, message });
    else if (!errors.has(pointer)) errors.set(pointer, message);
  }
  // A summary stays hidden only when a more specific error at or below it is kept.
  // Deepest first, so a kept inner summary also explains the outer ones.
  const depth = (p: string) => p.split('/').length;
  for (const { pointer, message } of summaries.sort(
    (a, b) => depth(b.pointer) - depth(a.pointer),
  )) {
    const explained = [...errors.keys()].some((k) => k === pointer || k.startsWith(`${pointer}/`));
    if (!explained) errors.set(pointer, message);
  }
  for (const pointer of [...errors.keys()]) {
    if (!isPathActive(schema, value, pointer.split('/').slice(1).map(decode)))
      errors.delete(pointer);
  }
  return errors;
}
