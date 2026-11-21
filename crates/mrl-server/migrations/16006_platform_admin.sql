-- Platform admins may create orgs on this core (`POST /orgs`). The solo local owner is one
-- implicitly; in team mode the flag is set explicitly (team setup, P43).
ALTER TABLE users ADD COLUMN platform_admin boolean NOT NULL DEFAULT false;
