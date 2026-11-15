import { useQuery } from '@tanstack/react-query';
import { z } from 'zod';
import { requireApi } from '../lib/api';
import { setPrefs, usePrefs } from './prefs';

const named = z.object({ id: z.string(), name: z.string() });

/**
 * Organization → Workspace → Project tree the person can see.
 * Mock contract `GET /api/v1/me/scopes` until the tenancy module's spec lands (P30 aligns it).
 */
export const scopeTreeSchema = z.object({
  orgs: z.array(
    named.extend({
      workspaces: z.array(named.extend({ projects: z.array(named) })),
    }),
  ),
});

export type ScopeTree = z.infer<typeof scopeTreeSchema>;

export function useScopeTree() {
  return useQuery({
    queryKey: ['scopes'],
    queryFn: () => requireApi().get('/api/v1/me/scopes', scopeTreeSchema),
  });
}

export interface CurrentScope {
  org?: ScopeTree['orgs'][number];
  workspace?: ScopeTree['orgs'][number]['workspaces'][number];
  project?: { id: string; name: string };
}

/** The stored choice, falling back to the first option at each level. */
export function resolveScope(
  tree: ScopeTree | undefined,
  chosen: { org?: string; workspace?: string; project?: string },
): CurrentScope {
  const org = tree?.orgs.find((o) => o.id === chosen.org) ?? tree?.orgs[0];
  const workspace = org?.workspaces.find((w) => w.id === chosen.workspace) ?? org?.workspaces[0];
  const project =
    workspace?.projects.find((p) => p.id === chosen.project) ?? workspace?.projects[0];
  return { org, workspace, project };
}

export function useCurrentScope() {
  const query = useScopeTree();
  const { scope } = usePrefs();
  const current = resolveScope(query.data, scope);
  const choose = (level: 'org' | 'workspace' | 'project', id: string) => {
    // Picking a higher level resets the levels below it.
    if (level === 'org') setPrefs({ scope: { org: id } });
    else if (level === 'workspace') setPrefs({ scope: { org: current.org?.id, workspace: id } });
    else
      setPrefs({ scope: { org: current.org?.id, workspace: current.workspace?.id, project: id } });
  };
  return { query, current, choose };
}
