import type { Bridge } from '../../../../desktop/src/main/ipc.registry';

export type {
  Bridge,
  CoreStatus,
  CrashSummary,
  DesktopSettings,
  SignInResult,
  UpdateStatus,
} from '../../../../desktop/src/main/ipc.registry';

let override: Bridge | null | undefined;

/**
 * The desktop shell's IPC bridge (`window.mrl`, generated from `apps/desktop/src/main/ipc.registry.ts`),
 * or null in a plain browser.
 */
export function desktopBridge(): Bridge | null {
  if (override !== undefined) return override;
  return typeof window === 'undefined' ? null : ((window as { mrl?: Bridge }).mrl ?? null);
}

/** Tests: act as if the app runs in the desktop shell with this bridge (`undefined` resets). */
export function setDesktopBridge(bridge: Bridge | null | undefined): void {
  override = bridge;
}
