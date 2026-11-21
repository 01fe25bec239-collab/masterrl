//! The tenancy seed steps: the signed-in admin, org `dev`, workspace `default` and
//! project `demo`. The admin check uses P16's `GET /auth/me`; the others use the tenancy
//! endpoints of Doc 09 §6.3 (`GET/POST /orgs`, `/orgs/{org}/workspaces`,
//! `/workspaces/{ws}/projects`), paging through every `next_cursor` before creating.

use super::super::{SeedState, SeedStep};
use crate::api::{all_items, Api, Failure};
use serde_json::{json, Value};

/// Slug and name of the seeded org.
pub const ORG: &str = "dev";
/// Slug and name of the seeded workspace.
pub const WORKSPACE: &str = "default";
/// Slug and name of the seeded project.
pub const PROJECT: &str = "demo";

/// The id of the item with `slug` on any page of `list`.
pub fn find_on_any_page(
    get: impl FnMut(&str) -> Result<Value, String>,
    list: &str,
    slug: &str,
) -> Result<Option<String>, String> {
    Ok(all_items(get, list)?
        .iter()
        .find(|i| i["slug"] == slug)
        .and_then(|i| i["id"].as_str())
        .map(str::to_owned))
}

/// Returns `(id, created)` for `slug` under `list`: lists every page first and creates
/// only when it is missing. A 409 on create means someone else made it meanwhile, so the
/// lists are read again and the existing item is used.
pub fn ensure_with(
    mut get: impl FnMut(&str) -> Result<Value, Failure>,
    mut post: impl FnMut(&str, &Value) -> Result<Value, Failure>,
    list: &str,
    slug: &str,
) -> Result<(String, bool), String> {
    if let Some(id) = find_on_any_page(|p| get(p).map_err(String::from), list, slug)? {
        return Ok((id, false));
    }
    match post(list, &json!({ "slug": slug, "name": slug })) {
        Ok(made) => made["id"]
            .as_str()
            .map(|id| (id.to_owned(), true))
            .ok_or_else(|| format!("the server created `{slug}` but sent no id: {made}")),
        Err(f) if f.status == Some(409) => {
            find_on_any_page(|p| get(p).map_err(String::from), list, slug)?
                .map(|id| (id, false))
                .ok_or_else(|| {
                    format!(
                        "the server says `{slug}` already exists but no page lists it:\n{}",
                        f.message
                    )
                })
        }
        Err(f) => Err(f.into()),
    }
}

fn ensure(api: &Api, list: &str, slug: &str) -> Result<(String, bool), String> {
    ensure_with(
        |p| api.get(p),
        |p, b| api.post(p, Some(b), None),
        list,
        slug,
    )
}

fn line(kind: &str, slug: &str, id: &str, created: bool) -> String {
    let verb = if created { "created" } else { "exists " };
    format!("{verb} {kind} `{slug}` ({id})")
}

/// `/{parent}/{id}/{child}`, with the id (read from a server reply) percent-encoded. An
/// empty, `.` or `..` id is refused: it would not stay one path segment.
fn under(parent: &str, id: &str, child: &str) -> Result<String, String> {
    if matches!(id, "" | "." | "..") {
        return Err(format!(
            "The server gave `{id}` as an id in {parent}, which can't be used in a request."
        ));
    }
    Ok(format!("/{parent}/{}/{child}", crate::commands::encode(id)))
}

fn need(v: &Option<String>, what: &str) -> Result<String, String> {
    v.clone()
        .ok_or_else(|| format!("the {what} step did not run first"))
}

/// Checks who is signed in (`GET /auth/me`): the person the seed sets up for.
pub struct Admin;

impl SeedStep for Admin {
    fn name(&self) -> &'static str {
        "admin"
    }
    fn run(&self, api: &Api, state: &mut SeedState) -> Result<String, String> {
        let me = api.get("/auth/me").map_err(String::from)?;
        let id = me["user"]["id"]
            .as_str()
            .ok_or_else(|| format!("`/auth/me` names no person for this sign-in: {me}"))?
            .to_owned();
        let kind = me["principal_kind"].as_str().unwrap_or("user");
        state.admin = Some(id.clone());
        Ok(format!("signed in as {id} ({kind})"))
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
        let (id, created) = ensure(api, &under("orgs", &org, "workspaces")?, WORKSPACE)?;
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
        let (id, created) = ensure(api, &under("workspaces", &ws, "projects")?, PROJECT)?;
        state.project = Some(id.clone());
        Ok(line("project", PROJECT, &id, created))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_from_replies_stay_one_path_segment() {
        assert_eq!(
            under("orgs", "org_1", "workspaces").unwrap(),
            "/orgs/org_1/workspaces"
        );
        assert_eq!(
            under("workspaces", "../auth/x?y", "projects").unwrap(),
            "/workspaces/..%2Fauth%2Fx%3Fy/projects"
        );
        for dots in ["", ".", ".."] {
            assert!(under("orgs", dots, "workspaces").is_err(), "{dots:?}");
        }
    }

    #[test]
    fn find_matches_by_slug() {
        let page = json!({"items": [{"id": "org_1", "slug": "dev"}, {"id": "org_2", "slug": "x"}]});
        let one = |_: &str| Ok(page.clone());
        assert_eq!(
            find_on_any_page(one, "/orgs", "dev").unwrap().as_deref(),
            Some("org_1")
        );
        assert_eq!(find_on_any_page(one, "/orgs", "nope").unwrap(), None);
        let empty = |_: &str| Ok(json!({}));
        assert_eq!(find_on_any_page(empty, "/orgs", "dev").unwrap(), None);
        assert!(line("org", "dev", "org_1", true).starts_with("created"));
    }

    #[test]
    fn later_pages_are_searched_and_cursor_loops_stop() {
        let pages = |path: &str| -> Result<Value, String> {
            Ok(match path {
                "/orgs" => json!({"items": [{"id": "org_1", "slug": "a"}], "next_cursor": "c 1"}),
                "/orgs?cursor=c%201" => {
                    json!({"items": [{"id": "org_2", "slug": "dev"}], "next_cursor": null})
                }
                other => return Err(format!("unexpected {other}")),
            })
        };
        assert_eq!(
            find_on_any_page(pages, "/orgs", "dev").unwrap().as_deref(),
            Some("org_2")
        );
        assert_eq!(find_on_any_page(pages, "/orgs", "zzz").unwrap(), None);
        let looping =
            |_: &str| -> Result<Value, String> { Ok(json!({"items": [], "next_cursor": "same"})) };
        assert!(find_on_any_page(looping, "/orgs", "dev")
            .unwrap_err()
            .contains("twice"));
    }

    #[test]
    fn ensure_lists_first_creates_once_and_takes_409_as_existing() {
        use std::cell::RefCell;
        let empty = json!({"items": [], "next_cursor": null});
        let listed = json!({"items": [{"id": "org_9", "slug": "dev"}], "next_cursor": null});
        // Found while listing: no create at all.
        let posts = RefCell::new(0);
        let post = |_: &str, _: &Value| {
            *posts.borrow_mut() += 1;
            Ok(json!({}))
        };
        let found = ensure_with(|_| Ok(listed.clone()), post, "/orgs", "dev").unwrap();
        assert_eq!((found, *posts.borrow()), (("org_9".into(), false), 0));
        // Missing: one create.
        let make = |_: &str, b: &Value| Ok(json!({"id": "org_1", "slug": b["slug"]}));
        let made = ensure_with(|_| Ok(empty.clone()), make, "/orgs", "dev").unwrap();
        assert_eq!(made, ("org_1".into(), true));
        // Made by someone else between list and create: 409, then found on the second listing.
        let lists = RefCell::new(0);
        let get = |_: &str| {
            *lists.borrow_mut() += 1;
            Ok(if *lists.borrow() == 1 {
                empty.clone()
            } else {
                listed.clone()
            })
        };
        let conflict = |_: &str, _: &Value| {
            Err(Failure {
                status: Some(409),
                message: "MRL-CONFLICT-001".into(),
            })
        };
        assert_eq!(
            ensure_with(get, conflict, "/orgs", "dev").unwrap(),
            ("org_9".into(), false)
        );
        // Other errors stop the seed; a 409 for something no page lists is reported.
        let denied = |_: &str, _: &Value| {
            Err(Failure {
                status: Some(403),
                message: "MRL-PERM-001".into(),
            })
        };
        let err = ensure_with(|_| Ok(empty.clone()), denied, "/orgs", "dev").unwrap_err();
        assert_eq!(err, "MRL-PERM-001");
        let ghost = |_: &str, _: &Value| {
            Err(Failure {
                status: Some(409),
                message: "dup".into(),
            })
        };
        let err = ensure_with(|_| Ok(empty.clone()), ghost, "/orgs", "dev").unwrap_err();
        assert!(err.contains("no page lists it"), "{err}");
    }
}
