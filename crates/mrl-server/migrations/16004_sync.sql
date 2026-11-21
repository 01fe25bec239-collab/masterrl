-- Setup sync (Doc 22 §5). `setup_profiles` is this install's setup (`setup-profile/v1`,
-- no secrets); `sync_state` records how the sync key was made (the key is in the keychain).
CREATE TABLE setup_profiles (
  user_id uuid PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
  profile jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sync_state (
  user_id uuid PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
  kdf jsonb NOT NULL,
  set_up_at timestamptz NOT NULL DEFAULT now(),
  last_push_at timestamptz NULL,
  last_pushed_version bigint NULL,
  last_restore_at timestamptz NULL
);
