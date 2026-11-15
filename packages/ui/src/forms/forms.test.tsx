import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import axe from 'axe-core';
import { useState } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { PresetPicker } from './PresetPicker';
import { defaultsOf, validateValue, type JsonObject, type JsonSchema, type Level } from './schema';
import { SchemaForm } from './SchemaForm';
import { yamlDiagnostics } from './yaml-diagnostics';
import { YamlEditor } from './YamlEditor';

vi.mock('./monaco-host', () => ({
  default: ({
    label,
    value,
    onChange,
  }: {
    label: string;
    value: string;
    onChange: (t: string) => void;
  }) => (
    <textarea
      aria-label={label}
      value={value}
      onChange={(e) => {
        onChange(e.target.value);
      }}
    />
  ),
}));

/** Shaped like a TRL SFTConfig section of an engine manifest's config schema. */
const trlSft: JsonSchema = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  $id: 'https://masterrl.dev/schemas/engines/trl/sft-config.json',
  title: 'TRL SFT settings',
  type: 'object',
  required: ['output_name', 'learning_rate', 'num_train_epochs'],
  $defs: {
    positiveInt: { type: 'integer', minimum: 1 },
  },
  properties: {
    output_name: {
      type: 'string',
      title: 'Run name',
      minLength: 1,
      maxLength: 60,
      pattern: '^[a-z0-9-]+$',
      'x-mrl-help': 'Lowercase letters, numbers and dashes.',
    },
    learning_rate: {
      type: 'number',
      exclusiveMinimum: 0,
      maximum: 0.01,
      default: 0.00002,
      'x-mrl-help': 'How big each step is.',
    },
    num_train_epochs: {
      $ref: '#/$defs/positiveInt',
      title: 'Passes over the data',
      maximum: 20,
      default: 3,
    },
    use_peft: { type: 'boolean', title: 'Use LoRA', default: true, 'x-mrl-group': 'Adapter' },
    lora: {
      type: 'object',
      title: 'LoRA settings',
      'x-mrl-group': 'Adapter',
      'x-mrl-dependsOn': 'use_peft',
      properties: {
        r: { type: 'integer', title: 'Rank', enum: [8, 16, 32], default: 16 },
        dropout: {
          type: 'number',
          title: 'Dropout',
          minimum: 0,
          maximum: 0.5,
          default: 0.05,
          'x-mrl-level': 'advanced',
        },
      },
    },
    per_device_train_batch_size: {
      $ref: '#/$defs/positiveInt',
      title: 'Batch size per GPU',
      default: 4,
      'x-mrl-level': 'advanced',
      'x-mrl-group': 'Memory',
    },
    max_seq_length: {
      type: 'integer',
      title: 'Longest sample',
      minimum: 128,
      default: 2048,
      'x-mrl-unit': 'tokens',
      'x-mrl-level': 'advanced',
      'x-mrl-group': 'Memory',
    },
    lr_scheduler_type: {
      type: 'string',
      enum: ['linear', 'cosine'],
      'x-mrl-enumLabels': ['Straight line', 'Smooth curve'],
      default: 'linear',
      'x-mrl-widget': 'radio',
      'x-mrl-level': 'advanced',
    },
    eval_steps: {
      type: 'integer',
      minimum: 1,
      'x-mrl-widget': 'slider',
      default: 50,
      maximum: 500,
      'x-mrl-level': 'advanced',
    },
    stop_words: { type: 'array', items: { type: 'string' }, 'x-mrl-level': 'advanced' },
  },
};

function Harness({
  initial,
  level: start = 'beginner',
  onSubmit,
}: {
  initial?: JsonObject;
  level?: Level;
  onSubmit?: (v: JsonObject) => void;
}) {
  const [value, setValue] = useState<JsonObject>(
    initial ?? { ...defaultsOf(trlSft), output_name: 'my-run' },
  );
  const [level, setLevel] = useState<Level>(start);
  return (
    <>
      <button
        type="button"
        onClick={() => {
          setLevel(level === 'beginner' ? 'advanced' : 'beginner');
        }}
      >
        toggle level
      </button>
      <SchemaForm
        label="Training settings"
        schema={trlSft}
        value={value}
        onChange={setValue}
        level={level}
        onSubmit={onSubmit ?? (() => undefined)}
      />
      <output data-testid="value">{JSON.stringify(value)}</output>
    </>
  );
}

const current = () => JSON.parse(screen.getByTestId('value').textContent) as JsonObject;

describe('SchemaForm', () => {
  it('Beginner hides advanced fields; Advanced shows them; values are kept', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    expect(screen.getByLabelText('Run name')).toBeDefined();
    expect(screen.getByLabelText('Learning rate')).toBeDefined();
    expect(screen.queryByLabelText('Batch size per GPU')).toBeNull();
    expect(screen.queryByLabelText('Dropout')).toBeNull();

    await user.click(screen.getByRole('button', { name: 'toggle level' }));
    const batch = screen.getByLabelText('Batch size per GPU');
    await user.clear(batch);
    await user.type(batch, '8');
    expect(screen.getByLabelText('Dropout')).toBeDefined();
    expect(screen.getByRole('group', { name: 'Memory' })).toBeDefined();
    expect(
      screen.getByLabelText(/Longest sample/).closest('div')?.parentElement?.textContent,
    ).toContain('(tokens)');

    await user.click(screen.getByRole('button', { name: 'toggle level' }));
    expect(screen.queryByLabelText('Batch size per GPU')).toBeNull();
    expect(current().per_device_train_batch_size).toBe(8);
    expect(
      screen.getByText(/Custom: 1 advanced setting changed \(Batch size per GPU\)/),
    ).toBeDefined();
  });

  it('shows plain validation messages after a field is left', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const lr = screen.getByLabelText('Learning rate');
    await user.clear(lr);
    await user.type(lr, '0.5');
    expect(screen.queryByText('Use 0.01 or less.')).toBeNull();
    await user.tab();
    expect(screen.getByText('Use 0.01 or less.')).toBeDefined();
    expect(lr.getAttribute('aria-invalid')).toBe('true');
    expect(lr.getAttribute('aria-describedby')).toContain('error');

    await user.clear(lr);
    await user.type(lr, 'abc');
    expect(screen.getByText('Enter a number.')).toBeDefined();

    const name = screen.getByLabelText('Run name');
    await user.clear(name);
    await user.tab();
    expect(screen.getByText('This is required.')).toBeDefined();
    await user.type(name, 'Has Spaces');
    expect(screen.getByText(/doesn’t look right/)).toBeDefined();
  });

  it('keeps typed decimals while editing numbers', async () => {
    const user = userEvent.setup();
    render(<Harness />);
    const lr = screen.getByLabelText('Learning rate');
    await user.clear(lr);
    await user.type(lr, '0.0');
    expect((lr as HTMLInputElement).value).toBe('0.0');
    await user.type(lr, '01');
    expect(current().learning_rate).toBe(0.001);
  });

  it('blocks submit, focuses the first problem and points to hidden advanced problems', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    render(
      <Harness
        initial={{ ...defaultsOf(trlSft), output_name: 'ok', max_seq_length: 5 }}
        onSubmit={onSubmit}
      />,
    );
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSubmit).not.toHaveBeenCalled();
    expect(screen.getByText('1 setting needs attention.')).toBeDefined();
    expect(
      screen.getByText(/1 advanced setting has a problem\. Switch to Advanced to fix it\./),
    ).toBeDefined();

    await user.click(screen.getByRole('button', { name: 'toggle level' }));
    expect(screen.getByText('Use 128 tokens or more.')).toBeDefined();
    await user.clear(screen.getByLabelText(/Longest sample/));
    await user.type(screen.getByLabelText(/Longest sample/), '4096');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSubmit).toHaveBeenCalledOnce();
  });

  it('hides fields whose dependsOn is off and ignores their errors', async () => {
    const user = userEvent.setup();
    render(
      <Harness
        initial={{ ...defaultsOf(trlSft), output_name: 'ok', lora: { r: 99 } }}
        level="advanced"
      />,
    );
    expect(screen.getByRole('group', { name: 'LoRA settings' })).toBeDefined();
    await user.click(screen.getByLabelText('Use LoRA'));
    expect(screen.queryByRole('group', { name: 'LoRA settings' })).toBeNull();
    expect(validateValue(trlSft, current()).size).toBe(0);
  });

  it('renders radios, sliders, selects and lists from hints', async () => {
    const user = userEvent.setup();
    render(<Harness level="advanced" />);
    await user.click(screen.getByLabelText('Smooth curve'));
    expect(current().lr_scheduler_type).toBe('cosine');
    expect(screen.getByRole('slider', { name: 'Eval steps' })).toBeDefined();
    await user.selectOptions(screen.getByLabelText('Rank'), '32');
    expect((current().lora as JsonObject).r).toBe(32);
    await user.type(screen.getByLabelText('Stop words'), 'END{enter}STOP');
    expect(current().stop_words).toEqual(['END', 'STOP']);
  });

  it('shows server field errors at once', () => {
    render(
      <SchemaForm
        label="Settings"
        schema={trlSft}
        value={{ ...defaultsOf(trlSft), output_name: 'taken' }}
        onChange={() => undefined}
        level="beginner"
        externalErrors={{ '/output_name': 'A run with this name already exists.' }}
      />,
    );
    expect(screen.getByText('A run with this name already exists.')).toBeDefined();
  });

  it('has no serious a11y problems', async () => {
    const { container } = render(<Harness level="advanced" />);
    const r = await axe.run(container);
    expect(
      r.violations
        .filter((v) => v.impact === 'serious' || v.impact === 'critical')
        .map((v) => v.id),
    ).toEqual([]);
  });
});

describe('every validator failure is reported', () => {
  const strict: JsonSchema = {
    type: 'object',
    additionalProperties: false,
    minProperties: 1,
    properties: {
      name: { type: 'string', title: 'Name' },
      tags: { type: 'array', title: 'Tags', uniqueItems: true, items: { type: 'string' } },
      lora: {
        type: 'object',
        title: 'LoRA',
        additionalProperties: false,
        properties: { r: { type: 'integer', title: 'Rank' } },
      },
    },
  };

  it('reports unknown keys, unmapped keywords and empty objects in plain words', () => {
    const errors = validateValue(strict, {
      name: 'a',
      extra: 1,
      tags: ['x', 'x'],
      lora: { r: 8, alpha: 2 },
    });
    expect(Object.fromEntries(errors)).toEqual({
      '/extra': '“extra” isn’t a setting this form knows. Remove it or check its spelling.',
      '/lora/alpha': '“alpha” isn’t a setting this form knows. Remove it or check its spelling.',
      '/tags': 'Remove the repeated items.',
    });
    expect(Object.fromEntries(validateValue(strict, {}))).toEqual({
      '': 'This value isn’t allowed here. Check the help text.',
    });
  });

  it('keeps a failed contains rule, which reports nothing more specific', () => {
    const lists: JsonSchema = {
      type: 'object',
      properties: { xs: { type: 'array', contains: { type: 'number' } } },
    };
    expect(Object.fromEntries(validateValue(lists, { xs: ['x'] }))).toEqual({
      '/xs': 'Add at least one item of the kind this list needs.',
    });
    expect(validateValue(lists, { xs: ['x', 1] }).size).toBe(0);
  });

  it('hides a summary only when its specific error is kept', () => {
    const lists: JsonSchema = {
      type: 'object',
      properties: { xs: { type: 'array', items: { type: 'number' } } },
    };
    expect(Object.fromEntries(validateValue(lists, { xs: ['x'] }))).toEqual({
      '/xs/0': 'Enter a number.',
    });
  });

  it('SchemaForm lists problems no field can show and blocks submit', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    render(
      <SchemaForm
        label="Strict"
        schema={strict}
        value={{ name: 'a', extra: 1 }}
        onChange={() => undefined}
        level="advanced"
        onSubmit={onSubmit}
      />,
    );
    const other = screen.getByRole('alert');
    expect(other.textContent).toContain('Other problems');
    expect(other.textContent).toContain('extra: “extra” isn’t a setting this form knows.');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSubmit).not.toHaveBeenCalled();
    expect(screen.getByText('1 setting needs attention.')).toBeDefined();
  });

  it('YAML diagnostics point at the unknown key', () => {
    const r = yamlDiagnostics('name: a\nextra: 1\n', strict);
    expect(r.diagnostics).toEqual([
      expect.objectContaining({ line: 2, message: expect.stringContaining('“extra”') as string }),
    ]);
  });
});

describe('PresetPicker', () => {
  it('picks a preset and says when values were changed by hand', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    const presets = [
      { id: 'quick', title: 'Quick', summary: 'Fast.', values: { num_train_epochs: 1 } },
      {
        id: 'full',
        title: 'Full',
        summary: 'Slow.',
        values: { num_train_epochs: 5 },
        recommended: true,
      },
    ];
    const { rerender } = render(
      <PresetPicker label="Start from" presets={presets} value="quick" onChange={onChange} />,
    );
    await user.click(screen.getByRole('radio', { name: /Full/ }));
    expect(onChange).toHaveBeenCalledWith(presets[1]);
    expect(screen.getByText('Recommended')).toBeDefined();
    rerender(
      <PresetPicker label="Start from" presets={presets} value={undefined} onChange={onChange} />,
    );
    expect(screen.getByText(/changed some values by hand/)).toBeDefined();
  });
});

describe('YamlEditor', () => {
  it('maps schema problems to YAML lines', () => {
    const r = yamlDiagnostics('output_name: ok\nlearning_rate: 5\nnum_train_epochs: 3\n', trlSft);
    expect(r.diagnostics).toEqual([
      expect.objectContaining({ line: 2, message: 'Use 0.01 or less.' }),
    ]);
    const missing = yamlDiagnostics('output_name: ok\n', trlSft);
    expect(missing.diagnostics.map((d) => d.message)).toContain('learning_rate: This is required.');
    const broken = yamlDiagnostics('a: [1, 2\n', trlSft);
    expect(broken.value).toBeUndefined();
    expect(broken.diagnostics[0]?.severity).toBe('error');
  });

  function Pair() {
    const [value, setValue] = useState<JsonObject>({
      ...defaultsOf(trlSft),
      output_name: 'my-run',
    });
    return (
      <>
        <SchemaForm
          label="Form"
          schema={trlSft}
          value={value}
          onChange={setValue}
          level="beginner"
        />
        <YamlEditor label="Settings YAML" schema={trlSft} value={value} onChange={setValue} />
      </>
    );
  }

  it('syncs both ways with the form', async () => {
    const user = userEvent.setup();
    render(<Pair />);
    const yaml = await screen.findByLabelText<HTMLTextAreaElement>('Settings YAML');
    expect(yaml.value).toContain('output_name: my-run');

    const epochs = screen.getByLabelText('Passes over the data');
    await user.clear(epochs);
    await user.type(epochs, '7');
    await waitFor(() => {
      expect(yaml.value).toContain('num_train_epochs: 7');
    });

    await user.clear(yaml);
    await user.type(
      yaml,
      'output_name: from-yaml{enter}learning_rate: 0.001{enter}num_train_epochs: 2',
    );
    expect(screen.getByLabelText<HTMLInputElement>('Run name').value).toBe('from-yaml');
    expect(screen.getByLabelText<HTMLInputElement>('Passes over the data').value).toBe('2');
    expect(screen.getByText('No problems found.')).toBeDefined();

    await user.type(yaml, '{enter}learning_rate: [[');
    expect(screen.getByText(/the YAML can’t be read, so the form was not updated/)).toBeDefined();
    expect(screen.getByLabelText<HTMLInputElement>('Run name').value).toBe('from-yaml');
  });
});
