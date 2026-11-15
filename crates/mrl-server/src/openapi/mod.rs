//! OpenAPI: the base document (health, problem schema) plus every module's fragment,
//! nested under `/api/v1`. `mrl-server openapi` prints it as YAML; it is served at
//! `/api-docs/openapi.json`.

use crate::app::ServerModule;
use crate::error::{FieldError, Problem};
use crate::http::API_PREFIX;
use crate::telemetry::{CheckResult, Health, Readiness};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(title = "MasterRL API", description = "The MasterRL server API. Errors use RFC 9457 problem documents."),
    paths(crate::telemetry::healthz, crate::telemetry::readyz),
    components(schemas(Problem, FieldError, Health, Readiness, CheckResult)),
    tags((name = "health", description = "Liveness and readiness."))
)]
struct Base;

/// The full document for these modules.
pub fn document(modules: &[Box<dyn ServerModule>]) -> utoipa::openapi::OpenApi {
    let mut doc = Base::openapi();
    doc.info.version = env!("CARGO_PKG_VERSION").to_owned();
    for m in modules {
        doc = doc.nest(API_PREFIX, m.openapi());
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;

    #[utoipa::path(get, path = "/things", responses((status = 200, description = "Things.")))]
    #[allow(dead_code)]
    async fn things() {}

    #[derive(OpenApi)]
    #[openapi(paths(things))]
    struct ThingsDoc;

    struct Things;
    impl ServerModule for Things {
        fn name(&self) -> &'static str {
            "things"
        }
        fn routes(&self) -> Router<crate::app::AppState> {
            Router::new()
        }
        fn openapi(&self) -> utoipa::openapi::OpenApi {
            ThingsDoc::openapi()
        }
    }

    #[test]
    fn empty_document_has_health_and_problem() {
        let doc = document(&[]);
        assert!(doc.paths.paths.contains_key("/healthz"));
        assert!(doc.paths.paths.contains_key("/readyz"));
        let schemas = &doc.components.as_ref().unwrap().schemas;
        assert!(schemas.contains_key("Problem"));
        assert!(doc.to_yaml().unwrap().contains("MasterRL API"));
    }

    #[test]
    fn module_paths_are_nested_under_api_v1() {
        let doc = document(&[Box::new(Things)]);
        assert!(doc.paths.paths.contains_key("/api/v1/things"));
    }
}
