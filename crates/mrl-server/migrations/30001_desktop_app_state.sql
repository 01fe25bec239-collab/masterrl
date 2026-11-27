-- P30: the desktop app's own state on this core (one row): first-run onboarding.
-- `onboarding_completed_at` is set once, when onboarding first reaches `done`.
CREATE TABLE desktop_app_state (
  id smallint PRIMARY KEY CHECK (id = 1),
  onboarding_step text NOT NULL DEFAULT 'welcome'
    CHECK (onboarding_step IN ('welcome','sign_in','restore','check','done')),
  onboarding_completed_at timestamptz NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
