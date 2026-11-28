import { EmptyState } from '@mrl/ui';

/** Shown instead of a desktop-only setting in a plain browser. */
export function DesktopOnly({ what }: { what: string }) {
  return (
    <EmptyState
      icon="info"
      title="Only in the desktop app"
      description={`${what} can be changed in the MasterRL app on your computer.`}
    />
  );
}
