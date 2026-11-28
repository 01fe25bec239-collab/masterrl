import { useNavigate, useRouterState } from '@tanstack/react-router';
import { useEffect } from 'react';
import { useDesktopState } from './desktop-state';

/** Screens shown even before onboarding is finished. */
const ALWAYS_OPEN = ['/welcome', '/settings/crash/'];

/** Sends a first run (or one that stopped half-way) to the onboarding screens. */
export function FirstRunRedirect() {
  const state = useDesktopState();
  const path = useRouterState({ select: (s) => s.location.pathname });
  const navigate = useNavigate();
  const step = state.data?.onboarding.step;

  useEffect(() => {
    if (!step || step === 'done') return;
    if (ALWAYS_OPEN.some((p) => path.startsWith(p))) return;
    void navigate({ to: '/welcome' });
  }, [step, path, navigate]);
  return null;
}
