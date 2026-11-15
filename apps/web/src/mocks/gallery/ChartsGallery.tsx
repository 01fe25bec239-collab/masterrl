import { useEffect, useState } from 'react';
import {
  binValues,
  GpuGauge,
  Histogram,
  LiveMetricsStore,
  PageHeader,
  ResourceBreakdownBar,
  Sparkline,
  TimeSeriesChart,
} from '@mrl/ui';
import { lossAt } from '../data';

const series = [
  { id: 'loss_r0', label: 'Loss (GPU 0)', rank: 0 },
  { id: 'loss_r1', label: 'Loss (GPU 1)', rank: 1 },
  { id: 'baseline', label: 'Previous run', overlay: true },
];

/** 20,000 steps of history (downsampled for drawing), then one new step every 200 ms. */
function useDemoStore() {
  const [store] = useState(() => {
    const s = new LiveMetricsStore(50_000);
    const x = Array.from({ length: 20_000 }, (_, i) => i);
    s.load(x, {
      loss_r0: x.map((i) => lossAt(i)),
      loss_r1: x.map((i) => lossAt(i) * 1.03 + (i % 997 === 0 ? 0.8 : 0)),
      baseline: x.map((i) => lossAt(i * 0.8) + 0.1),
    });
    return s;
  });
  useEffect(() => {
    let step = 20_000;
    const t = setInterval(() => {
      step += 1;
      store.appendPoints('loss_r0', [[step, lossAt(step)]]);
      store.appendPoints('loss_r1', [[step, lossAt(step) * 1.03]]);
    }, 200);
    return () => {
      clearInterval(t);
    };
  }, [store]);
  return store;
}

const tokens = Array.from(
  { length: 2000 },
  (_, i) => 120 + Math.round(400 * Math.abs(Math.sin(i * 1.7)) ** 2),
);

export default function ChartsGallery() {
  const store = useDemoStore();
  return (
    <>
      <PageHeader
        title="Charts"
        description="Every chart has a text summary and a table you can open."
      />
      <div className="flex flex-col gap-6">
        <TimeSeriesChart title="Training loss" series={series} store={store} />
        <Histogram
          title="Sample length"
          valueLabel="Tokens per sample"
          bins={binValues(tokens, 24)}
        />
        <p className="flex items-center gap-2">
          Reward trend:{' '}
          <Sparkline label="Reward" values={[0.1, 0.15, 0.12, 0.2, 0.28, 0.31, 0.3, 0.38, 0.42]} />
        </p>
        <div className="grid gap-4 md:grid-cols-2">
          <GpuGauge
            name="NVIDIA RTX 3060 Laptop"
            utilizationPct={87}
            memoryUsedGb={5.1}
            memoryTotalGb={6}
            temperatureC={71}
          />
          <GpuGauge
            name="Apple M4 Pro"
            utilizationPct={42}
            memoryUsedGb={11.5}
            memoryTotalGb={24}
          />
        </div>
        <ResourceBreakdownBar
          title="GPU memory needed"
          unit="GB"
          capacity={6}
          parts={[
            { label: 'Model weights', range: { min: 1.0, max: 1.0 } },
            { label: 'LoRA and optimizer', range: { min: 0.4, max: 0.7 } },
            { label: 'Activations', range: { min: 1.8, max: 3.2 } },
          ]}
        />
      </div>
    </>
  );
}
