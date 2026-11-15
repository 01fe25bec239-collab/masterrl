import { useEffect, useEffectEvent, useRef } from 'react';
import * as monaco from 'monaco-editor/editor/editor.api.js';
import 'monaco-editor/languages/definitions/yaml/register.js';
import EditorWorker from 'monaco-editor/editor/editor.worker.js?worker';
import { resolvedTheme } from '../lib/theme';
import type { Diagnostic } from './yaml-diagnostics';

globalThis.MonacoEnvironment = { getWorker: () => new EditorWorker() };

export interface MonacoHostProps {
  value: string;
  onChange: (text: string) => void;
  diagnostics: Diagnostic[];
  label: string;
  height: number;
}

/** The Monaco part of YamlEditor. Loaded lazily: Monaco is large. */
export default function MonacoHost({
  value,
  onChange,
  diagnostics,
  label,
  height,
}: MonacoHostProps) {
  const hostRef = useRef<HTMLDivElement>(null);
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
  const emit = useEffectEvent(onChange);

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    const editor = monaco.editor.create(host, {
      value,
      language: 'yaml',
      ariaLabel: label,
      automaticLayout: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      tabSize: 2,
      theme:
        (document.documentElement.dataset.theme ?? resolvedTheme('system')) === 'dark'
          ? 'vs-dark'
          : 'vs',
    });
    editorRef.current = editor;
    const sub = editor.onDidChangeModelContent(() => {
      emit(editor.getValue());
    });
    return () => {
      sub.dispose();
      editor.getModel()?.dispose();
      editor.dispose();
      editorRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- created once; value/markers sync below
  }, [label]);

  useEffect(() => {
    const editor = editorRef.current;
    if (editor && editor.getValue() !== value) editor.setValue(value);
  }, [value]);

  useEffect(() => {
    const model = editorRef.current?.getModel();
    if (!model) return;
    monaco.editor.setModelMarkers(
      model,
      'mrl-schema',
      diagnostics.map((d) => ({
        message: d.message,
        severity:
          d.severity === 'error' ? monaco.MarkerSeverity.Error : monaco.MarkerSeverity.Warning,
        startLineNumber: d.line,
        startColumn: d.column,
        endLineNumber: d.endLine,
        endColumn: Math.max(d.endColumn, d.column + 1),
      })),
    );
  }, [diagnostics]);

  return <div ref={hostRef} style={{ height }} className="rounded-md border border-border" />;
}
