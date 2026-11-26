//! `mrl dev seed`: idempotently sets up a dev install (Doc 20 item 17).
//!
//! Steps run in order and each one checks before it creates, so running the seed again
//! changes nothing. Registry parts add their step (fixture model, datasets, …) as one
//! `<name>.rs` file under `steps/` and one line inside `// <mrl:seed-steps>` below.

pub mod steps;

use crate::api::Api;

/// What earlier steps made; later steps read it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeedState {
    /// The signed-in admin's principal id.
    pub admin: Option<String>,
    /// Org `dev`.
    pub org: Option<String>,
    /// Workspace `default`.
    pub workspace: Option<String>,
    /// Project `demo`.
    pub project: Option<String>,
}

/// One idempotent seed step.
pub trait SeedStep {
    /// Short name, printed while it runs.
    fn name(&self) -> &'static str;
    /// Finds or creates what this step owns; returns a line for the user.
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String>;
}

/// Every step, in order.
pub fn steps() -> Vec<Box<dyn SeedStep>> {
    vec![
        Box::new(steps::tenancy::Admin),
        Box::new(steps::tenancy::Org),
        Box::new(steps::tenancy::Workspace),
        Box::new(steps::tenancy::Project),
        // <mrl:seed-steps> Later parts' steps go right below this line.
        // <mrl:seed-steps P26> P26 steps go right below this line.
        // <mrl:seed-steps P27> P27 steps go right below this line.
    ]
}

/// Runs every step and prints what each found or made.
pub fn run(api: &Api) -> Result<SeedState, String> {
    let mut state = SeedState::default();
    for step in steps() {
        let line = step
            .run(api, &mut state)
            .map_err(|e| format!("seed step `{}` failed:\n{e}", step.name()))?;
        println!("{:<10} {line}", step.name());
    }
    Ok(state)
}
