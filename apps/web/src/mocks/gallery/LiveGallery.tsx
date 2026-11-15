import { useState } from 'react';
import {
  FreshnessBadge,
  LiveLogViewer,
  LiveMetricsStore,
  LogBuffer,
  PageHeader,
  TimeSeriesChart,
  useEventStream,
} from '@mrl/ui';
import { EVENTS_PER_CONNECTION } from '../handlers';

interface MetricsEvent {
  series_id: string;
  points: number[][];
}
interface LogEvent {
  text: string;
  rank?: number;
  stream?: 'stdout' | 'stderr';
}

export default function LiveGallery() {
  const [store] = useState(() => new LiveMetricsStore());
  const [logs] = useState(() => new LogBuffer());

  const { stale } = useEventStream('run:run_demo:metrics', (e) => {
    if (e.type === 'metrics') {
      const m = e.json as MetricsEvent;
      store.appendPoints(m.series_id, m.points);
    }
  });
  useEventStream('run:run_demo:logs', (e) => {
    if (e.type === 'log') logs.append([e.json as LogEvent]);
  });

  return (
    <>
      <PageHeader
        title="Live data"
        description={`One shared stream for the page. The mock drops the connection every ${EVENTS_PER_CONNECTION} events; it reconnects and continues where it stopped.`}
        meta={<FreshnessBadge freshness={stale ? 'stale' : 'live'} />}
      />
      <div className="flex flex-col gap-6">
        <TimeSeriesChart
          title="Loss (live)"
          series={[{ id: 'loss', label: 'Loss' }]}
          store={store}
        />
        <LiveLogViewer buffer={logs} label="Training log" />
      </div>
    </>
  );
}
