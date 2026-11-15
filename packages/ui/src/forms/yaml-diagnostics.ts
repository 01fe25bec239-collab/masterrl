import { isMap, isScalar, isSeq, LineCounter, parseDocument, stringify, type Node } from 'yaml';
import { validateValue, type JsonSchema, type JsonValue } from './schema';

export interface Diagnostic {
  message: string;
  /** 1-based. */
  line: number;
  column: number;
  endLine: number;
  endColumn: number;
  severity: 'error' | 'warning';
}

export interface YamlResult {
  /** Parsed value; undefined when the YAML itself does not parse. */
  value: JsonValue | undefined;
  diagnostics: Diagnostic[];
}

export const toYaml = (value: JsonValue): string => stringify(value, { lineWidth: 0 });

function nodeAt(root: Node | null, path: string[]): { node: Node | null; matched: number } {
  let node = root;
  let matched = 0;
  for (const key of path) {
    if (isMap(node)) {
      const pair = node.items.find((p) => isScalar(p.key) && String(p.key.value) === key);
      if (!pair) break;
      node = (pair.value as Node | null) ?? (pair.key as Node);
    } else if (isSeq(node)) {
      const item = node.items[Number(key)] as Node | undefined;
      if (!item) break;
      node = item;
    } else break;
    matched += 1;
  }
  return { node, matched };
}

/** Parse YAML and check it against the schema; problems come back with line/column ranges. */
export function yamlDiagnostics(text: string, schema: JsonSchema): YamlResult {
  const lines = new LineCounter();
  const doc = parseDocument(text, { lineCounter: lines, prettyErrors: false });
  const at = (offset: number) => lines.linePos(offset);
  const range = (from: number, to: number) => {
    const a = at(from);
    const b = at(Math.max(from, to));
    return { line: a.line, column: a.col, endLine: b.line, endColumn: b.col };
  };

  const syntax: Diagnostic[] = [
    ...doc.errors.map((e) => ({ e, severity: 'error' as const })),
    ...doc.warnings.map((e) => ({ e, severity: 'warning' as const })),
  ].map(({ e, severity }) => ({
    message: e.message.split('\n')[0] ?? e.message,
    severity,
    ...range(e.pos[0], e.pos[1]),
  }));
  if (doc.errors.length > 0) return { value: undefined, diagnostics: syntax };

  const value = (doc.toJS() ?? {}) as JsonValue;
  const diagnostics = [...syntax];
  for (const [pointer, message] of validateValue(schema, value)) {
    const path = pointer
      .split('/')
      .slice(1)
      .map((s) => s.replace(/~1/g, '/').replace(/~0/g, '~'));
    const { node, matched } = nodeAt(doc.contents, path);
    const r = node?.range
      ? range(node.range[0], node.range[1])
      : { line: 1, column: 1, endLine: 1, endColumn: 1 };
    const missing = matched < path.length ? `${path.slice(matched).join('.')}: ` : '';
    diagnostics.push({ message: `${missing}${message}`, severity: 'error', ...r });
  }
  return { value, diagnostics };
}
