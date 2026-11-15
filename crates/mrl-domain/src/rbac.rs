//! Roles, actions and the static permission matrix.
//!
//! Actions are one per endpoint family, split into read and write where it matters.
//! Scope (which project or org a role binding covers) is checked by the caller;
//! this table only answers "may this role ever do this action".

use std::fmt;

/// A role. Humans get one of the first five; `Agent` and `Worker` are machine identities only.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Owns the org: can do every human action.
    Owner,
    /// Runs projects: approves promotions, serves models, manages secrets and machine access.
    Maintainer,
    /// Imports data and models, writes rewards and runs training.
    Researcher,
    /// Works only on feedback tasks assigned to them.
    Annotator,
    /// Reads only.
    Viewer,
    /// The node agent (machine identity; never a person).
    Agent,
    /// A training worker process (machine identity; never a person).
    Worker,
}

impl Role {
    /// Every role.
    pub const ALL: [Self; 7] = [
        Self::Owner,
        Self::Maintainer,
        Self::Researcher,
        Self::Annotator,
        Self::Viewer,
        Self::Agent,
        Self::Worker,
    ];

    /// True for roles people hold; false for machine identities.
    pub const fn is_human(self) -> bool {
        !matches!(self, Self::Agent | Self::Worker)
    }

    /// The snake-case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Maintainer => "maintainer",
            Self::Researcher => "researcher",
            Self::Annotator => "annotator",
            Self::Viewer => "viewer",
            Self::Agent => "agent",
            Self::Worker => "worker",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

macro_rules! actions {
    ($( $(#[$meta:meta])* $name:ident => $wire:literal, )+) => {
        /// Something a caller wants to do, named `<family>.<verb>`.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub enum Action {
            $( $(#[$meta])* #[serde(rename = $wire)] $name, )+
        }

        impl Action {
            /// Every action, in table order.
            pub const ALL: &'static [Self] = &[$(Self::$name),+];

            /// The wire name, like `runs.submit`.
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$name => $wire,)+ }
            }
        }
    };
}

actions! {
    /// Sign in and out, read your own profile.
    AuthSession => "auth.session",
    /// Create and revoke your own access tokens.
    AuthManageTokens => "auth.manage_tokens",
    /// Read orgs, workspaces, projects and members.
    TenancyRead => "tenancy.read",
    /// Create, change and delete projects.
    TenancyManageProjects => "tenancy.manage_projects",
    /// Manage org members, role bindings and sign-in (SSO).
    TenancyManageMembers => "tenancy.manage_members",
    /// Create workspaces and set org quotas and budgets.
    TenancyManageOrg => "tenancy.manage_org",
    /// Upload files.
    UploadsWrite => "uploads.write",
    /// Read models and model versions.
    ModelsRead => "models.read",
    /// Import models, approve their remote code and accept licenses.
    ModelsImport => "models.import",
    /// Download or export model files.
    ModelsExport => "models.export",
    /// Read datasets, records and validation reports.
    DatasetsRead => "datasets.read",
    /// Import and transform datasets.
    DatasetsImport => "datasets.import",
    /// Download or export dataset files.
    DatasetsExport => "datasets.export",
    /// Read engines, capabilities and algorithm forms.
    EnginesRead => "engines.read",
    /// Install, approve and disable third-party engines.
    EnginesManage => "engines.manage",
    /// Read runs, jobs, attempts and timelines.
    RunsRead => "runs.read",
    /// Validate, submit and branch runs.
    RunsSubmit => "runs.submit",
    /// Cancel, pause or resume runs you started.
    RunsCancelOwn => "runs.cancel_own",
    /// Cancel, pause or resume any run, and change job priority.
    RunsCancelAny => "runs.cancel_any",
    /// Read metrics, logs and samples.
    MetricsLogsRead => "metrics_logs.read",
    /// Download full logs.
    MetricsLogsDownload => "metrics_logs.download",
    /// Read checkpoints.
    CheckpointsRead => "checkpoints.read",
    /// Promote, verify, consolidate and delete checkpoints.
    CheckpointsWrite => "checkpoints.write",
    /// Download or export checkpoints.
    CheckpointsExport => "checkpoints.export",
    /// Read rewards and reward versions.
    RewardsRead => "rewards.read",
    /// Create, test and publish rewards.
    RewardsWrite => "rewards.write",
    /// Read every feedback task in a project.
    FeedbackRead => "feedback.read",
    /// Create, close, resolve and export feedback tasks.
    FeedbackManage => "feedback.manage",
    /// Read feedback tasks assigned to you.
    FeedbackReadAssigned => "feedback.read_assigned",
    /// Record answers on feedback items assigned to you.
    FeedbackAnnotate => "feedback.annotate",
    /// Read pools, nodes and the queue.
    ComputeRead => "compute.read",
    /// Manage machine access: pools, enrollment tokens, drain and decommission nodes.
    ComputeManage => "compute.manage",
    /// Read evaluation suites and results.
    EvaluationsRead => "evaluations.read",
    /// Run and compare evaluations.
    EvaluationsRun => "evaluations.run",
    /// Read promotion requests.
    PromotionsRead => "promotions.read",
    /// Ask to promote a model version (EXPERIMENTAL to CANDIDATE).
    PromotionsRequest => "promotions.request",
    /// Approve or reject promotions (CANDIDATE to APPROVED).
    PromotionsApprove => "promotions.approve",
    /// Use the playground and manage deployments (serving).
    PlaygroundUse => "playground.use",
    /// Read lineage graphs.
    LineageRead => "lineage.read",
    /// Change installation settings and global policies.
    AdminManage => "admin.manage",
    /// Register and delete secret references.
    SecretsManage => "secrets.manage",
    /// Read and export the audit log.
    AuditRead => "audit.read",
    /// Subscribe to live event streams.
    StreamsSubscribe => "streams.subscribe",
    /// Node agent gateway: heartbeats, commands, transfer tickets.
    AgentGateway => "agent.gateway",
    /// Worker channel: progress, metrics, checkpoints and exit reports.
    WorkerGateway => "worker.gateway",
}

impl Action {
    /// True for actions only machine identities take.
    pub const fn is_machine(self) -> bool {
        matches!(self, Self::AgentGateway | Self::WorkerGateway)
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// True if `role` may take `action` (within the scope of its role binding).
pub const fn allowed(role: Role, action: Action) -> bool {
    use Action::*;
    // Read access shared by every human role except annotators.
    let read = matches!(
        action,
        AuthSession
            | TenancyRead
            | ModelsRead
            | DatasetsRead
            | EnginesRead
            | RunsRead
            | MetricsLogsRead
            | CheckpointsRead
            | RewardsRead
            | FeedbackRead
            | FeedbackReadAssigned
            | ComputeRead
            | EvaluationsRead
            | PromotionsRead
            | LineageRead
            | StreamsSubscribe
    );
    let research = read
        || matches!(
            action,
            AuthManageTokens
                | UploadsWrite
                | ModelsImport
                | ModelsExport
                | DatasetsImport
                | DatasetsExport
                | RunsSubmit
                | RunsCancelOwn
                | MetricsLogsDownload
                | CheckpointsWrite
                | CheckpointsExport
                | RewardsWrite
                | FeedbackManage
                | FeedbackAnnotate
                | EvaluationsRun
                | PromotionsRequest
        );
    match role {
        Role::Owner => !action.is_machine(),
        Role::Maintainer => {
            research
                || matches!(
                    action,
                    TenancyManageProjects
                        | RunsCancelAny
                        | PromotionsApprove
                        | PlaygroundUse
                        | SecretsManage
                        | ComputeManage
                        | AuditRead
                )
        }
        Role::Researcher => research,
        Role::Annotator => matches!(
            action,
            AuthSession | FeedbackReadAssigned | FeedbackAnnotate
        ),
        Role::Viewer => read,
        Role::Agent => matches!(action, AgentGateway),
        Role::Worker => matches!(action, WorkerGateway),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn permission_matrix_snapshot() -> fmt::Result {
        let mut out = String::new();
        write!(out, "{:<24}", "action")?;
        for r in Role::ALL {
            write!(out, " {:<10}", r.as_str())?;
        }
        writeln!(out)?;
        for &a in Action::ALL {
            write!(out, "{:<24}", a.as_str())?;
            for r in Role::ALL {
                write!(out, " {:<10}", if allowed(r, a) { "yes" } else { "-" })?;
            }
            writeln!(out)?;
        }
        let out: Vec<&str> = out.lines().map(str::trim_end).collect();
        insta::assert_snapshot!(out.join("\n"));
        Ok(())
    }

    #[test]
    fn machines_and_humans_never_share_actions() {
        for &a in Action::ALL {
            let human = Role::ALL.iter().any(|&r| r.is_human() && allowed(r, a));
            let machine = Role::ALL.iter().any(|&r| !r.is_human() && allowed(r, a));
            assert!(
                !(human && machine),
                "{a} is open to both people and machines"
            );
            assert!(human || machine, "{a} is open to no one");
            assert_eq!(machine, a.is_machine(), "{a}");
        }
    }

    #[test]
    fn roles_nest() {
        for &a in Action::ALL {
            assert_eq!(allowed(Role::Owner, a), !a.is_machine(), "owner: {a}");
            if allowed(Role::Viewer, a) {
                assert!(
                    allowed(Role::Researcher, a),
                    "researcher lacks viewer action {a}"
                );
            }
            if allowed(Role::Researcher, a) {
                assert!(
                    allowed(Role::Maintainer, a),
                    "maintainer lacks researcher action {a}"
                );
            }
            if allowed(Role::Annotator, a) {
                assert!(
                    allowed(Role::Researcher, a),
                    "researcher lacks annotator action {a}"
                );
            }
        }
    }

    #[test]
    fn spec_rows() {
        use Action::*;
        use Role::*;
        assert!(allowed(Maintainer, PromotionsApprove) && !allowed(Researcher, PromotionsApprove));
        assert!(allowed(Researcher, RunsCancelOwn) && !allowed(Researcher, RunsCancelAny));
        assert!(allowed(Maintainer, RunsCancelAny));
        assert!(allowed(Maintainer, SecretsManage) && !allowed(Researcher, SecretsManage));
        assert!(allowed(Maintainer, AuditRead) && !allowed(Researcher, AuditRead));
        assert!(!allowed(Maintainer, TenancyManageMembers) && !allowed(Maintainer, AdminManage));
        assert!(!allowed(Viewer, RunsSubmit) && !allowed(Viewer, ModelsExport));
        assert!(!allowed(Annotator, RunsRead) && !allowed(Annotator, FeedbackRead));
        assert!(
            !allowed(Owner, AgentGateway)
                && !allowed(Agent, RunsRead)
                && !allowed(Worker, AgentGateway)
        );
    }

    #[test]
    fn names_are_unique_and_shaped() {
        let names: std::collections::BTreeSet<_> =
            Action::ALL.iter().map(|a| a.to_string()).collect();
        assert_eq!(names.len(), Action::ALL.len());
        for n in &names {
            let (family, verb) = n.split_once('.').unwrap_or_default();
            assert!(!family.is_empty() && !verb.is_empty(), "{n}");
        }
        assert_eq!(
            serde_json::to_string(&Action::RunsCancelOwn).ok(),
            Some("\"runs.cancel_own\"".into())
        );
        assert_eq!(
            serde_json::to_string(&Role::Maintainer).ok(),
            Some("\"maintainer\"".into())
        );
        assert_eq!(Role::Worker.to_string(), "worker");
    }
}
