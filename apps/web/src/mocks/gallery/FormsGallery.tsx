import { useState } from 'react';
import {
  defaultsOf,
  PageHeader,
  PresetPicker,
  SchemaForm,
  YamlEditor,
  type JsonObject,
  type JsonSchema,
  type Preset,
} from '@mrl/ui';
import { usePrefs } from '../../app/prefs';

/** Shaped like a TRL SFT engine config schema with x-mrl hints. */
const schema: JsonSchema = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  type: 'object',
  required: ['run_name', 'learning_rate', 'num_train_epochs'],
  properties: {
    run_name: {
      type: 'string',
      title: 'Run name',
      minLength: 1,
      maxLength: 200,
      'x-mrl-help': 'A name you will recognize later.',
    },
    learning_rate: {
      type: 'number',
      title: 'Learning rate',
      exclusiveMinimum: 0,
      maximum: 0.01,
      default: 0.00002,
      'x-mrl-help': 'How big each learning step is. Smaller is safer.',
    },
    num_train_epochs: {
      type: 'integer',
      title: 'Passes over the data',
      minimum: 1,
      maximum: 20,
      default: 3,
      'x-mrl-widget': 'slider',
    },
    use_peft: {
      type: 'boolean',
      title: 'Train a small adapter (LoRA)',
      default: true,
      'x-mrl-group': 'Adapter',
    },
    lora_r: {
      type: 'integer',
      title: 'Adapter size (rank)',
      enum: [8, 16, 32, 64],
      default: 16,
      'x-mrl-group': 'Adapter',
      'x-mrl-level': 'advanced',
      'x-mrl-dependsOn': 'use_peft',
    },
    per_device_train_batch_size: {
      type: 'integer',
      title: 'Batch size per GPU',
      minimum: 1,
      maximum: 256,
      default: 4,
      'x-mrl-level': 'advanced',
      'x-mrl-group': 'Speed and memory',
    },
    max_seq_length: {
      type: 'integer',
      title: 'Longest sample',
      minimum: 128,
      maximum: 32768,
      default: 2048,
      'x-mrl-unit': 'tokens',
      'x-mrl-level': 'advanced',
      'x-mrl-group': 'Speed and memory',
    },
    lr_scheduler_type: {
      type: 'string',
      title: 'Learning-rate schedule',
      enum: ['linear', 'cosine', 'constant'],
      'x-mrl-enumLabels': ['Straight line down', 'Smooth curve down', 'Stay the same'],
      default: 'linear',
      'x-mrl-widget': 'radio',
      'x-mrl-level': 'advanced',
    },
  },
};

const presets: Preset[] = [
  {
    id: 'quick',
    title: 'Quick try',
    summary: 'One pass, small adapter. Done in minutes.',
    values: { num_train_epochs: 1, lora_r: 8 },
  },
  {
    id: 'balanced',
    title: 'Balanced',
    summary: 'Good results for most data.',
    values: { num_train_epochs: 3, lora_r: 16 },
    recommended: true,
  },
  {
    id: 'thorough',
    title: 'Thorough',
    summary: 'More passes and a bigger adapter. Takes longer.',
    values: { num_train_epochs: 6, lora_r: 32 },
  },
];

export default function FormsGallery() {
  const { mode } = usePrefs();
  const [value, setValue] = useState<JsonObject>(() => ({
    ...defaultsOf(schema),
    run_name: 'support-bot-v2',
  }));
  const [preset, setPreset] = useState<string | undefined>('balanced');
  const [saved, setSaved] = useState<string | null>(null);

  return (
    <>
      <PageHeader
        title="Forms"
        description="The form and the YAML editor edit the same settings. Switch Beginner/Advanced in the top bar."
      />
      <div className="mb-6">
        <PresetPicker
          label="Starting point"
          presets={presets}
          value={preset}
          onChange={(p) => {
            setPreset(p.id);
            setValue({ ...value, ...p.values });
          }}
        />
      </div>
      <div className="grid gap-6 lg:grid-cols-2">
        <SchemaForm
          label="Training settings"
          schema={schema}
          value={value}
          level={mode}
          onChange={(v) => {
            setValue(v);
            setPreset(undefined);
          }}
          onSubmit={(v) => {
            setSaved(JSON.stringify(v));
          }}
          submitLabel="Check settings"
        />
        <div>
          <h2 className="mb-2 font-semibold">Same settings as YAML</h2>
          <YamlEditor
            label="Training settings YAML"
            schema={schema}
            value={value}
            onChange={(v) => {
              setValue(v);
              setPreset(undefined);
            }}
          />
        </div>
      </div>
      {saved && (
        <p role="status" className="mt-4">
          Settings look good: <code className="break-all">{saved}</code>
        </p>
      )}
    </>
  );
}
