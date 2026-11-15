import { Sparkline, StateChip } from '@mrl/ui';

/** Example home-dashboard card registered through `NavEntry.homeCard`. */
export default function HomeCard() {
  return (
    <section aria-labelledby="demo-card" className="rounded-md border border-border p-4">
      <h3 id="demo-card" className="font-semibold">
        Active runs (demo)
      </h3>
      <ul className="mt-2 flex flex-col gap-2">
        <li className="flex items-center gap-3">
          <span className="flex-1 whitespace-nowrap">support-bot-v2</span>
          <Sparkline label="Loss" values={[2.1, 1.6, 1.2, 0.9, 0.7, 0.6, 0.55]} />
          <StateChip machine="job" state="RUNNING" />
        </li>
        <li className="flex items-center gap-3">
          <span className="flex-1 whitespace-nowrap">math-tutor-grpo</span>
          <Sparkline label="Reward" values={[0.1, 0.14, 0.2, 0.22, 0.3]} />
          <StateChip machine="job" state="QUEUED" />
        </li>
      </ul>
    </section>
  );
}
