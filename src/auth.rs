//! Scoped API-key authentication for the HTTP control plane.
//!
//! Replaces the single global `API_KEY` with a multi-key registry where each
//! key carries a set of scopes:
//!
//! - `control`     — `POST /control`
//! - `settlements` — `GET /settlements`
//! - `admin`       — everything above
//!
//! Configuration:
//!
//! - `API_KEYS` — JSON array:
//!   `[{"name":"ci","token":"...","scopes":["control"]},
//!     {"name":"root","token":"...","scopes":["admin"]}]`
//! - `API_KEY` (legacy) — still honored, granted the `admin` scope.
//!
//! When neither variable is set, authentication is disabled (open control
//! plane — the historical default, unchanged for local development).
//!
//! Token comparison is constant-time (`constant_time_eq`); the registry is
//! tiny, so comparing against every key leaks only the key count, never
//! which key (or how many bytes) matched.

use crate::constant_time_eq;
use crate::security::SecurityCounters;
use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// A permission grant on the HTTP control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Control,
    Settlements,
    Admin,
}

/// Human-readable scope name (for logs and error bodies).
pub fn scope_name(scope: Scope) -> &'static str {
    match scope {
        Scope::Control => "control",
        Scope::Settlements => "settlements",
        Scope::Admin => "admin",
    }
}

/// Set of scopes granted to one API key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scopes {
    control: bool,
    settlements: bool,
    admin: bool,
}

impl Scopes {
    /// Parse scope names (case-insensitive). Unknown names are ignored with
    /// a warning so a typo doesn't brick the daemon's auth config.
    pub fn from_names<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut scopes = Self::default();
        for name in names {
            match name.as_ref().trim().to_ascii_lowercase().as_str() {
                "control" => scopes.control = true,
                "settlements" => scopes.settlements = true,
                "admin" => scopes.admin = true,
                "" => {}
                other => warn!(scope = other, "unknown API scope ignored (known: control, settlements, admin)"),
            }
        }
        scopes
    }

    /// Full-access scope set (legacy `API_KEY` / `admin` entries).
    pub fn admin() -> Self {
        Self {
            control: true,
            settlements: true,
            admin: true,
        }
    }

    /// Does this set satisfy `required`? `admin` implies every scope.
    pub fn grants(&self, required: Scope) -> bool {
        self.admin
            || match required {
                Scope::Control => self.control,
                Scope::Settlements => self.settlements,
                Scope::Admin => self.admin,
            }
    }
}

/// Authenticated identity attached to authorized requests (`Extension`).
#[derive(Debug, Clone)]
pub struct ApiIdentity {
    pub name: String,
    pub scopes: Scopes,
}

#[derive(Debug, Deserialize)]
struct ApiKeyEntry {
    name: Option<String>,
    token: String,
    scopes: Vec<String>,
}

/// Registry of valid API keys. Built once at startup from the environment.
#[derive(Debug, Default)]
pub struct ApiKeyRegistry {
    keys: Vec<(String, ApiIdentity)>,
}

impl ApiKeyRegistry {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Build from `API_KEYS` (JSON) plus legacy `API_KEY` (admin-scoped).
    /// A malformed `API_KEYS` value logs a warning and is ignored; the
    /// daemon never fails to boot over auth config.
    pub fn from_env() -> Self {
        let mut registry = Self::default();

        if let Some(json) = std::env::var("API_KEYS").ok().filter(|v| !v.is_empty()) {
            match Self::parse_keys_json(&json) {
                Ok(entries) => {
                    for (idx, (name, token, scopes)) in entries.into_iter().enumerate() {
                        let name = name.unwrap_or_else(|| format!("key-{idx}"));
                        registry.insert(name, token, scopes);
                    }
                }
                Err(e) => warn!(error = %e, "API_KEYS is not valid JSON; ignoring"),
            }
        }

        if let Some(token) = std::env::var("API_KEY").ok().filter(|v| !v.is_empty()) {
            registry.insert("legacy-api-key".to_string(), token, Scopes::admin());
        }

        if !registry.is_empty() {
            info!(keys = registry.len(), "API key authentication enabled");
        }
        registry
    }

    fn insert(&mut self, name: String, token: String, scopes: Scopes) {
        if token.trim().is_empty() {
            warn!(key = %name, "skipping API key entry with empty token");
            return;
        }
        if self.keys.iter().any(|(t, _)| t == &token) {
            warn!(key = %name, "duplicate API token ignored");
            return;
        }
        self.keys.push((
            token,
            ApiIdentity {
                name,
                scopes,
            },
        ));
    }

    fn parse_keys_json(json: &str) -> Result<Vec<(Option<String>, String, Scopes)>, serde_json::Error> {
        let entries: Vec<ApiKeyEntry> = serde_json::from_str(json)?;
        Ok(entries
            .into_iter()
            .map(|e| {
                let scopes = Scopes::from_names(e.scopes);
                (e.name, e.token, scopes)
            })
            .collect())
    }

    /// Constant-time lookup of a bearer token. Returns the identity of the
    /// matching key, or `None`.
    pub fn authenticate(&self, token: &str) -> Option<ApiIdentity> {
        let mut matched: Option<&ApiIdentity> = None;
        for (expected, identity) in &self.keys {
            if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
                matched = Some(identity);
            }
        }
        matched.cloned()
    }
}

/// Middleware state: which scope a route group requires.
#[derive(Clone)]
pub struct AuthGate {
    pub registry: Arc<ApiKeyRegistry>,
    pub required: Scope,
    pub security: Arc<SecurityCounters>,
}

/// Axum middleware: require a valid bearer token carrying `gate.required`.
///
/// - No keys configured → request passes (auth disabled, backward compatible).
/// - Missing/invalid token → 401 (+ `WWW-Authenticate: Bearer`).
/// - Valid token, missing scope → 403.
///
/// On success the [`ApiIdentity`] is attached to request extensions.
pub async fn require_scope(
    State(gate): State<AuthGate>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    if gate.registry.is_empty() {
        return next.run(req).await;
    }

    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    let Some(token) = token else {
        gate.security.inc_api_auth_failure();
        return unauthorized_response();
    };

    match gate.registry.authenticate(token) {
        None => {
            gate.security.inc_api_auth_failure();
            unauthorized_response()
        }
        Some(identity) if !identity.scopes.grants(gate.required) => {
            warn!(
                key = %identity.name,
                required = scope_name(gate.required),
                "API key lacks required scope"
            );
            (
                StatusCode::FORBIDDEN,
                format!(
                    "API key '{}' is missing the '{}' scope",
                    identity.name,
                    scope_name(gate.required)
                ),
            )
                .into_response()
        }
        Some(identity) => {
            debug!(key = %identity.name, scope = scope_name(gate.required), "API request authorized");
            req.extensions_mut().insert(identity);
            next.run(req).await
        }
    }
}

fn unauthorized_response() -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, "missing or invalid API key").into_response();
    resp.headers_mut()
        .insert(header::WWW_AUTHENTICATE, "Bearer".parse().unwrap());
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_grants_matrix() {
        let settlements_only = Scopes::from_names(["settlements"]);
        assert!(settlements_only.grants(Scope::Settlements));
        assert!(!settlements_only.grants(Scope::Control));
        assert!(!settlements_only.grants(Scope::Admin));

        let control = Scopes::from_names(["Control"]); // case-insensitive
        assert!(control.grants(Scope::Control));
        assert!(!control.grants(Scope::Settlements));

        let admin = Scopes::from_names(["admin"]);
        assert!(admin.grants(Scope::Control));
        assert!(admin.grants(Scope::Settlements));
        assert!(admin.grants(Scope::Admin));

        let none = Scopes::from_names(Vec::<&str>::new());
        assert!(!none.grants(Scope::Control));
        assert!(!none.grants(Scope::Settlements));
    }

    #[test]
    fn unknown_scope_names_are_ignored() {
        let scopes = Scopes::from_names(["control", "bogus", ""]);
        assert!(scopes.grants(Scope::Control));
        assert!(!scopes.grants(Scope::Settlements));
    }

    #[test]
    fn authenticate_matches_exact_token() {
        let mut registry = ApiKeyRegistry::default();
        registry.insert(
            "ci".to_string(),
            "tok-ci".to_string(),
            Scopes::from_names(["control"]),
        );
        registry.insert(
            "ops".to_string(),
            "tok-ops".to_string(),
            Scopes::admin(),
        );

        let id = registry.authenticate("tok-ops").expect("ops token");
        assert_eq!(id.name, "ops");
        assert!(id.scopes.grants(Scope::Admin));

        let id = registry.authenticate("tok-ci").expect("ci token");
        assert_eq!(id.name, "ci");
        assert!(!id.scopes.grants(Scope::Admin));

        assert!(registry.authenticate("tok-nope").is_none());
        assert!(registry.authenticate("").is_none());
        assert!(registry.authenticate("tok-ci-extra").is_none());
    }

    #[test]
    fn empty_token_entries_are_skipped() {
        let mut registry = ApiKeyRegistry::default();
        registry.insert("bad".to_string(), "   ".to_string(), Scopes::admin());
        assert!(registry.is_empty());
    }

    #[test]
    fn duplicate_tokens_keep_first_identity() {
        let mut registry = ApiKeyRegistry::default();
        registry.insert("first".to_string(), "dup".to_string(), Scopes::admin());
        registry.insert("second".to_string(), "dup".to_string(), Scopes::default());
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.authenticate("dup").unwrap().name, "first");
    }

    #[test]
    fn parse_keys_json_happy_path() {
        let json = r#"[
            {"name":"ci","token":"t1","scopes":["control","settlements"]},
            {"token":"t2","scopes":["admin"]}
        ]"#;
        let entries = ApiKeyRegistry::parse_keys_json(json).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0.as_deref(), Some("ci"));
        assert!(entries[0].2.grants(Scope::Control));
        assert!(entries[0].2.grants(Scope::Settlements));
        assert!(entries[1].2.grants(Scope::Admin));
    }

    #[test]
    fn parse_keys_json_rejects_garbage() {
        assert!(ApiKeyRegistry::parse_keys_json("not json").is_err());
        assert!(ApiKeyRegistry::parse_keys_json("{}").is_err());
    }

    #[test]
    fn from_env_covers_legacy_json_and_disabled_paths() {
        // Single test (not several) — these env vars are global, and cargo
        // runs tests in parallel threads within one process.
        std::env::remove_var("API_KEYS");
        std::env::remove_var("API_KEY");
        assert!(ApiKeyRegistry::from_env().is_empty(), "unset → disabled");

        std::env::set_var("API_KEY", "legacy-tok");
        let legacy_only = ApiKeyRegistry::from_env();
        assert_eq!(legacy_only.len(), 1);
        assert!(legacy_only
            .authenticate("legacy-tok")
            .expect("legacy key")
            .scopes
            .grants(Scope::Admin));

        std::env::set_var(
            "API_KEYS",
            r#"[{"name":"json-key","token":"json-tok","scopes":["settlements"]}]"#,
        );
        let both = ApiKeyRegistry::from_env();
        assert_eq!(both.len(), 2);
        assert!(both.authenticate("json-tok").is_some());
        assert!(both
            .authenticate("json-tok")
            .unwrap()
            .scopes
            .grants(Scope::Settlements));
        assert!(!both.authenticate("json-tok").unwrap().scopes.grants(Scope::Control));

        // Malformed API_KEYS falls back to legacy only (warn + continue).
        std::env::set_var("API_KEYS", "not-json");
        let fallback = ApiKeyRegistry::from_env();
        assert_eq!(fallback.len(), 1);
        assert!(fallback.authenticate("legacy-tok").is_some());

        std::env::remove_var("API_KEYS");
        std::env::remove_var("API_KEY");
    }
}
