import { mrl } from '../../app/session';

/** One place the workspace switcher can show. */
export interface WorkspaceEntry {
  /** Unique across sources, like `solo` or `team:https://core.example.com`. */
  id: string;
  name: string;
  /** One plain line, like "Your role: admin". */
  detail: string;
  /** This window works in it now. */
  current: boolean;
  /** Where it lives when it is not on this computer (a team core's address). */
  address?: string;
}

/** Where switcher entries come from. */
export interface WorkspaceSource {
  /** Stable kebab-case id. */
  id: string;
  /** Group heading in the switcher, like "Teams". */
  label: string;
  /** The entries now; a failure hides only this group. */
  list: () => Promise<WorkspaceEntry[]>;
}

/**
 * Every source of workspaces in the switcher (append-only, one entry per feature).
 * P43 adds team workspaces joined from this app.
 */
export const workspaceSources: readonly WorkspaceSource[] = [
  // P30: the Solo workspace on this computer, where the app's own core runs.
  {
    id: 'solo',
    label: 'On this computer',
    list: () =>
      Promise.resolve([
        {
          id: 'solo',
          name: 'This computer',
          detail: 'Solo: everything stays here.',
          current: true,
        },
      ]),
  },
  // P30: teams from the synced setup profile (P10).
  {
    id: 'setup-profile-teams',
    label: 'Teams',
    list: async () => {
      const profile = await mrl().GET('/api/v1/sync/profile');
      return (profile.team_memberships ?? []).map((t) => ({
        id: `team:${t.address}`,
        name: t.name,
        detail: t.role ? `Your role: ${t.role}` : 'Team workspace',
        current: false,
        address: t.address,
      }));
    },
  },
];
