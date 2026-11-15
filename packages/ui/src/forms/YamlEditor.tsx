import { lazy, Suspense, useMemo, useState } from 'react';
import { Icon } from '../components/Icon';
import { LoadingState } from '../components/States';
import type { JsonObject, JsonSchema, JsonValue } from './schema';
import { toYaml, yamlDiagnostics } from './yaml-diagnostics';

const MonacoHost = lazy(() => import('./monaco-host'));

export interface YamlEditorProps {
  /** The same object SchemaForm edits: changes flow both ways. */
  value: JsonObject;
  onChange: (value: JsonObject) => void;
  schema: JsonSchema;
  label: string;
  height?: number;
}

const isObject = (v: JsonValue | undefined): v is JsonObject =>
  v !== null && typeof v === 'object' && !Array.isArray(v);

/** YAML view of a config with live schema diagnostics. Monaco loads on first use. */
export function YamlEditor({ value, onChange, schema, label, height = 320 }: YamlEditorProps) {
  const json = JSON.stringify(value);
  const [text, setText] = useState(() => toYaml(value));
  const [synced, setSynced] = useState(json);
  // Outside change (form edit, preset) → re-render the YAML. Own edits already match `synced`.
  if (json !== synced) {
    setSynced(json);
    setText(toYaml(value));
  }
  const result = useMemo(() => yamlDiagnostics(text, schema), [text, schema]);

  const edit = (next: string) => {
    setText(next);
    const parsed = yamlDiagnostics(next, schema).value;
    if (isObject(parsed)) {
      setSynced(JSON.stringify(parsed));
      onChange(parsed);
    }
  };

  return (
    <div>
      <Suspense fallback={<LoadingState label="Loading the editor" lines={6} />}>
        <MonacoHost
          value={text}
          onChange={edit}
          diagnostics={result.diagnostics}
          label={label}
          height={height}
        />
      </Suspense>
      <div className="mt-2" aria-live="polite">
        {result.diagnostics.length === 0 ? (
          <p className="flex items-center gap-2 text-sm text-fg">
            <Icon name="check" />
            No problems found.
          </p>
        ) : (
          <>
            <p className="text-sm font-medium text-danger">
              {result.diagnostics.length === 1
                ? '1 problem'
                : `${result.diagnostics.length} problems`}
              {result.value === undefined
                ? ' — the YAML can’t be read, so the form was not updated.'
                : ''}
            </p>
            <ul className="mt-1 text-sm text-fg">
              {result.diagnostics.map((d, i) => (
                <li key={i} className="flex items-center gap-2">
                  <Icon name="alert" className="size-3.5 text-danger" />
                  Line {d.line}: {d.message}
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
    </div>
  );
}
