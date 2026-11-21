-- Who people are and what they may do (Doc 09 §2, Doc 23 §3). Ids are UUIDs; the API shows
-- them with their prefixes (`usr_`, `org_`, `ws_`, `prj_`).

-- A person. Solo mode has exactly one local owner; signing in links it to a Supabase user.
CREATE TABLE users (
  id uuid PRIMARY KEY,
  supabase_user_id uuid NULL UNIQUE,
  email text NULL,
  display_name text NOT NULL DEFAULT '',
  avatar_url text NULL,
  is_local_owner boolean NOT NULL DEFAULT false,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX users_one_local_owner ON users (is_local_owner) WHERE is_local_owner;

-- The tenant root: one implicit `personal` org in solo mode, one `team` org per team core.
CREATE TABLE organizations (
  id uuid PRIMARY KEY,
  slug text NOT NULL UNIQUE,
  name text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('personal', 'team')),
  settings jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX organizations_one_personal ON organizations (kind) WHERE kind = 'personal';

CREATE TABLE workspaces (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
  slug text NOT NULL,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz NULL,
  UNIQUE (org_id, slug)
);

CREATE TABLE projects (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
  workspace_id uuid NOT NULL REFERENCES workspaces (id) ON DELETE CASCADE,
  slug text NOT NULL,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz NULL,
  UNIQUE (org_id, workspace_id, slug)
);

-- RBAC: a role over an org, a workspace or a project. Roles are the human roles of P06.
CREATE TABLE role_bindings (
  id uuid PRIMARY KEY,
  org_id uuid NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
  principal_kind text NOT NULL CHECK (principal_kind IN ('user', 'service_account', 'group')),
  principal_id uuid NOT NULL,
  role text NOT NULL CHECK (role IN ('owner', 'maintainer', 'researcher', 'annotator', 'viewer')),
  scope_kind text NOT NULL CHECK (scope_kind IN ('org', 'workspace', 'project')),
  scope_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (principal_kind, principal_id, role, scope_kind, scope_id)
);
CREATE INDEX role_bindings_principal ON role_bindings (principal_id, org_id);
CREATE INDEX role_bindings_scope ON role_bindings (org_id, scope_kind, scope_id);
