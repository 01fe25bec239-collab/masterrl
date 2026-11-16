//! The tenancy seed steps: the signed-in admin, org `dev`, workspace `default` and
//! project `demo`. They use the tenancy endpoints of Doc 09 §6.3.

use super::super::{SeedState, SeedStep};
use crate::api::Api;
use serde_json::{json, Value};

/// Slug and name of the seeded org.
pub const ORG: &str = "dev";
/// Slug and name of the seeded workspace.
pub const WORKSPACE: &str = "default";
/// Slug and name of the seeded project.
pub const PROJECT: &str = "demo";

/// The item with `slug` in a `{items: [...]}` page, if any.
fn find(page: &Value, slug: &str) -> Option<String> {
    page["items"]
        .as_array()?
        .iter()
        .find(|i| i["slug"] == slug)
        .and_then(|i| i["id"].as_str())
        .map(str::to_owned)
}

/// Returns the id of `slug` under `list`, creating it with `create` when missing.
fn ensure(api: &Api, list: &str, slug: &str) -> Result<(String, bool), String> {
    if let Some(id) = find(&api.get(list)?, slug) {
        return Ok((id, false));
    }
    let made = api.post(list, Some(&json!({ "slug": slug, "name": slug })), None)?;
    made["id"]
        .as_str()
        .map(|id| (id.to_owned(), true))
        .ok_or_else(|| format!("the server created `{slug}` but sent no id: {made}"))
}

fn line(kind: &str, slug: &str, id: &str, created: bool) -> String {
    let verb = if created { "created" } else { "exists " };
    format!("{verb} {kind} `{slug}` ({id})")
}

fn need(v: &Option<String>, what: &str) -> Result<String, String> {
    v.clone()
        .ok_or_else(|| format!("the {what} step did not run first"))
}

/// Checks who is signed in: the seed's admin.
pub struct Admin;

impl SeedStep for Admin {
    fn name(&self) -> &'static str {
        "admin"
    }
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let me = api.get("/me")?;
        let id = me["id"]
            .as_str()
            .ok_or_else(|| format!("`/me` sent no id: {me}"))?
            .to_owned();
        state.admin = Some(id.clone());
        Ok(format!("signed in as {id}"))
    }
}

/// Org `dev`.
pub struct Org;

impl SeedStep for Org {
    fn name(&self) -> &'static str {
        "org"
    }
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let (id, created) = ensure(api, "/orgs", ORG)?;
        state.org = Some(id.clone());
        Ok(line("org", ORG, &id, created))
    }
}

/// Workspace `default` in org `dev`.
pub struct Workspace;

impl SeedStep for Workspace {
    fn name(&self) -> &'static str {
        "workspace"
    }
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let org = need(&state.org, "org")?;
        let (id, created) = ensure(api, &format!("/orgs/{org}/workspaces"), WORKSPACE)?;
        state.workspace = Some(id.clone());
        Ok(line("workspace", WORKSPACE, &id, created))
    }
}

/// Project `demo` in workspace `default`.
pub struct Project;

impl SeedStep for Project {
    fn name(&self) -> &'static str {
        "project"
    }
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let ws = need(&state.workspace, "workspace")?;
        let (id, created) = ensure(api, &format!("/workspaces/{ws}/projects"), PROJECT)?;
        state.project = Some(id.clone());
        Ok(line("project", PROJECT, &id, created))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_matches_by_slug() {
        let page = json!({"items": [{"id": "org_1", "slug": "dev"}, {"id": "org_2", "slug": "x"}]});
        assert_eq!(find(&page, "dev").as_deref(), Some("org_1"));
        assert_eq!(find(&page, "nope"), None);
        assert_eq!(find(&json!({}), "dev"), None);
        assert!(line("org", "dev", "org_1", true).starts_with("created"));
    }
}
