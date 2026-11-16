//! The `MRL-<FAMILY>-<NNN>` error code type and the registry of every code.
//!
//! Titles match the guidance catalog (`guidance/build/catalog.json`), which also holds the
//! "why" and "what to do" text. A test keeps the two in step.

use std::fmt;
use std::str::FromStr;

use crate::capability::codes::CapabilityRejection;

/// The family part of an error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorFamily {
    /// `AUTH`: sign-in and tokens.
    Auth,
    /// `PERM`: the role doesn't allow the action.
    Perm,
    /// `VAL`: the request could not be read or has a value the app doesn't accept.
    Val,
    /// `CAP`: engine, method, model, dataset and hardware don't work together.
    Cap,
    /// `QUOTA`: limits, budgets and rate limits.
    Quota,
    /// `IDEM`: request-key (idempotency) conflicts.
    Idem,
    /// `DATA`: the dataset is not usable yet.
    Data,
    /// `MODEL`: model safety, remote code and licenses.
    Model,
    /// `ENV`: this computer's setup.
    Env,
    /// `LINK`: machines that must work together.
    Link,
    /// `CONFLICT`: the item changed since it was read (ETag / version checks).
    Conflict,
    /// `INTERNAL`: the server failed or a service behind it is down.
    Internal,
    /// `RES`: the item or address does not exist, or is hidden from you.
    Res,
    /// `STORE`: object storage, file lists and transfer links.
    Store,
    /// `STATE`: the action isn't allowed in the item's current state.
    State,
    /// `RWD`: reward policies and reward services.
    Rwd,
    /// `NODE`: machines and their agents.
    Node,
}

impl ErrorFamily {
    /// Every family.
    pub const ALL: [Self; 17] = [
        Self::Auth,
        Self::Perm,
        Self::Val,
        Self::Cap,
        Self::Quota,
        Self::Idem,
        Self::Data,
        Self::Model,
        Self::Env,
        Self::Link,
        Self::Conflict,
        Self::Internal,
        Self::Res,
        Self::Store,
        Self::State,
        Self::Rwd,
        Self::Node,
    ];

    /// The family name, like `CAP`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "AUTH",
            Self::Perm => "PERM",
            Self::Val => "VAL",
            Self::Cap => "CAP",
            Self::Quota => "QUOTA",
            Self::Idem => "IDEM",
            Self::Data => "DATA",
            Self::Model => "MODEL",
            Self::Env => "ENV",
            Self::Link => "LINK",
            Self::Conflict => "CONFLICT",
            Self::Internal => "INTERNAL",
            Self::Res => "RES",
            Self::Store => "STORE",
            Self::State => "STATE",
            Self::Rwd => "RWD",
            Self::Node => "NODE",
        }
    }
}

/// An error code like `MRL-VAL-002`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ErrorCode {
    family: ErrorFamily,
    number: u16,
}

/// Why an error code could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not an error code like `MRL-VAL-001`")]
pub struct ErrorCodeParseError(pub String);

impl ErrorCode {
    /// The lowest code number.
    pub const MIN_NUMBER: u16 = 1;
    /// The highest code number (codes have three digits).
    pub const MAX_NUMBER: u16 = 999;

    /// A code from its family and number, or `None` if the number is not 1 to 999.
    pub const fn new(family: ErrorFamily, number: u16) -> Option<Self> {
        if number >= Self::MIN_NUMBER && number <= Self::MAX_NUMBER {
            Some(Self { family, number })
        } else {
            None
        }
    }

    /// A code for the built-in tables. Use it only in `const` items, so a number outside
    /// 1 to 999 stops the build instead of reaching runtime.
    pub(crate) const fn known(family: ErrorFamily, number: u16) -> Self {
        match Self::new(family, number) {
            Some(code) => code,
            None => panic!("error code numbers must be 1 to 999"),
        }
    }

    /// The family.
    pub const fn family(&self) -> ErrorFamily {
        self.family
    }

    /// The number.
    pub const fn number(&self) -> u16 {
        self.number
    }

    /// The title from the registry, or `None` if the registry has no such code.
    pub fn title(&self) -> Option<&'static str> {
        all().find(|(c, _)| c == self).map(|(_, t)| t)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MRL-{}-{:03}", self.family.as_str(), self.number)
    }
}

impl FromStr for ErrorCode {
    type Err = ErrorCodeParseError;

    fn from_str(s: &str) -> Result<Self, ErrorCodeParseError> {
        let bad = || ErrorCodeParseError(s.to_owned());
        let rest = s.strip_prefix("MRL-").ok_or_else(bad)?;
        let (family, number) = rest.split_once('-').ok_or_else(bad)?;
        let family = ErrorFamily::ALL
            .iter()
            .copied()
            .find(|f| f.as_str() == family)
            .ok_or_else(bad)?;
        if number.len() != 3 || !number.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        let number: u16 = number.parse().map_err(|_| bad())?;
        Self::new(family, number).ok_or_else(bad)
    }
}

impl serde::Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

const fn code(family: ErrorFamily, number: u16) -> ErrorCode {
    ErrorCode::known(family, number)
}

use ErrorFamily::{
    Auth, Conflict, Data, Env, Idem, Internal, Link, Model, Perm, Quota, Res, Store, Val,
};

/// Every non-capability code with its title. `MRL-CAP` codes live in [`CapabilityRejection`].
pub const REGISTRY: &[(ErrorCode, &str)] = &[
    (code(Auth, 1), "You need to sign in."),
    (
        code(Auth, 2),
        "The app could not sign you in with these details.",
    ),
    (code(Auth, 3), "This access token no longer works."),
    (code(Perm, 1), "Your role doesn't allow this action."),
    (code(Perm, 2), "This feedback task isn't assigned to you."),
    (code(Val, 1), "The app could not read this request."),
    (code(Val, 2), "A required field is missing."),
    (code(Val, 3), "A field has a value the app doesn't accept."),
    (
        code(Quota, 1),
        "Your team reached its limit on jobs or GPUs.",
    ),
    (
        code(Quota, 2),
        "The compute budget for this period is used up.",
    ),
    (
        code(Quota, 3),
        "The app got too many requests from you in a short time.",
    ),
    (code(Idem, 1), "This request is already running."),
    (
        code(Idem, 2),
        "This request key already belongs to a different request.",
    ),
    (code(Data, 1), "Your dataset did not pass its checks."),
    (code(Data, 2), "Your dataset is not ready yet."),
    (
        code(Model, 1),
        "The app did not load this model's files, to keep your computer safe.",
    ),
    (
        code(Model, 2),
        "This model needs custom code your workspace hasn't approved.",
    ),
    (
        code(Model, 3),
        "You haven't accepted this model's license yet.",
    ),
    (
        code(Env, 1),
        "The app doesn't support this version of your operating system.",
    ),
    (
        code(Env, 2),
        "This computer's processor doesn't meet the app's needs.",
    ),
    (
        code(Env, 3),
        "This machine has too little free memory (RAM).",
    ),
    (code(Env, 4), "There is not enough free disk space."),
    (
        code(Env, 5),
        "The app found no usable GPU, or its memory is too small.",
    ),
    (
        code(Env, 6),
        "The NVIDIA driver or CUDA version is too old.",
    ),
    (code(Env, 7), "WSL2 is missing or not set up."),
    (code(Env, 8), "WSL2 mirrored networking is off."),
    (code(Env, 9), "This Mac can train only tiny models."),
    (code(Env, 10), "AMD GPU support is experimental."),
    (code(Env, 11), "Python or uv is missing or too old."),
    (code(Env, 12), "This computer doesn't have Docker."),
    (
        code(Link, 1),
        "These machines can't share one training job.",
    ),
    (
        code(Link, 2),
        "The machines run different driver, CUDA or engine versions.",
    ),
    (
        code(Link, 3),
        "The connection between the machines is slow.",
    ),
    (
        code(Link, 4),
        "A firewall is blocking the machines from talking.",
    ),
    (code(Link, 5), "The clocks on these machines don't match."),
    (
        code(Link, 6),
        "A machine in this group is low on disk space.",
    ),
    // Server request and startup errors (P07).
    (code(Val, 8), "The server could not open its settings file."),
    (
        code(Val, 9),
        "The server could not understand its settings file.",
    ),
    (code(Val, 10), "A server setting has a value it cannot use."),
    (
        code(Val, 11),
        "A server setting from the environment could not be used.",
    ),
    (
        code(Val, 12),
        "The server could not use its certificate or key file.",
    ),
    (
        code(Internal, 3),
        "The server could not start because a required part is missing.",
    ),
    (
        code(Internal, 4),
        "Team mode could not start because no sign-in provider is set up.",
    ),
    (
        code(Internal, 5),
        "The server could not open a network port.",
    ),
    (code(Internal, 6), "The server stopped during startup."),
    (
        code(Internal, 7),
        "The server could not set up trace export.",
    ),
    (code(Internal, 8), "The server took too long to stop."),
    (
        code(Internal, 9),
        "A background job in the server stopped, so the server shut down.",
    ),
    (code(Res, 1), "The item you asked for could not be found."),
    (code(Val, 4), "Some values in the request need a change."),
    (
        code(Val, 5),
        "The page settings in the request could not be used.",
    ),
    (code(Val, 6), "The filter in the request could not be used."),
    (code(Val, 7), "The request is too large."),
    (
        code(Conflict, 1),
        "Someone changed this item after you opened it.",
    ),
    (
        code(Conflict, 2),
        "This change needs the version of the item you edited.",
    ),
    (code(Idem, 3), "The request key could not be used."),
    (code(Res, 2), "There is nothing at this address."),
    (code(Val, 13), "This address does not support that action."),
    (
        code(Internal, 1),
        "The server hit a problem it did not expect.",
    ),
    (
        code(Internal, 2),
        "A service the server needs is not answering.",
    ),
    // Storage errors (P08).
    (code(Store, 1), "The app refused a storage path."),
    (
        code(Store, 2),
        "A name used for storage has characters the app doesn't allow.",
    ),
    (
        code(Store, 3),
        "The app could not find this file in storage.",
    ),
    (
        code(Store, 4),
        "The storage service did not respond as expected.",
    ),
    (
        code(Store, 5),
        "The app could not read or write a file on this computer.",
    ),
    (code(Store, 6), "The storage settings need a change."),
    (code(Store, 7), "The app refused this transfer link."),
    (code(Store, 8), "This transfer link has expired."),
    (code(Store, 9), "This transfer link doesn't allow that."),
    (
        code(Store, 10),
        "A file changed on its way through storage.",
    ),
    (
        code(Store, 11),
        "Some files in storage don't match their file list.",
    ),
    (code(Store, 12), "The app could not read this file list."),
    (
        code(Store, 13),
        "A file changed while the app was uploading it.",
    ),
    // <mrl:errors P11> rows for P11 go right below this line.

    // <mrl:errors P12> rows for P12 go right below this line.

    // <mrl:errors P13> rows for P13 go right below this line.
    (code(Val, 130), "The engine manifest has a problem."),
    (code(Val, 131), "A built-in task got inputs it can't use."),
    (
        code(ErrorFamily::Cap, 130),
        "This built-in task is not installed.",
    ),
    (
        code(Env, 130),
        "The training worker and the app use different protocol versions.",
    ),
    (
        code(Env, 131),
        "This engine needs a different version of the MasterRL runtime.",
    ),
    (
        code(Internal, 130),
        "The app and its training worker disagreed about the next step.",
    ),
    // <mrl:errors P14> rows for P14 go right below this line.

    // <mrl:errors P15> rows for P15 go right below this line.
];

/// Every code with its title: the registry plus every capability rejection.
pub fn all() -> impl Iterator<Item = (ErrorCode, &'static str)> {
    REGISTRY.iter().copied().chain(
        CapabilityRejection::ALL
            .iter()
            .map(|r| (r.code(), r.title())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::{FailureClass, FailureCode, SubCode};
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn codes_parse_and_display() {
        let c = code(Val, 2);
        assert_eq!(c.to_string(), "MRL-VAL-002");
        assert_eq!("MRL-VAL-002".parse(), Ok(c));
        assert_eq!((c.family(), c.number()), (Val, 2));
        assert_eq!(c.title(), Some("A required field is missing."));
        assert_eq!(code(Val, 99).title(), None);
        for bad in [
            "",
            "MRL-VAL-02",
            "MRL-VAL-0002",
            "MRL-NOPE-001",
            "MRL-VAL-000",
            "XRL-VAL-001",
            "MRL-VAL",
            "MRL-VAL-+01",
        ] {
            assert_eq!(
                bad.parse::<ErrorCode>(),
                Err(ErrorCodeParseError(bad.into())),
                "{bad}"
            );
        }
        let msg = "x".parse::<ErrorCode>().map_err(|e| e.to_string());
        assert_eq!(
            msg,
            Err("`x` is not an error code like `MRL-VAL-001`".into())
        );
        assert_eq!(
            serde_json::to_string(&c).ok(),
            Some("\"MRL-VAL-002\"".into())
        );
        assert_eq!(
            serde_json::from_str::<ErrorCode>("\"MRL-CAP-023\"").ok(),
            Some(code(ErrorFamily::Cap, 23))
        );
        assert!(serde_json::from_str::<ErrorCode>("\"MRL-CAP-23\"").is_err());
    }

    #[test]
    fn numbers_must_be_one_to_999() {
        assert_eq!(ErrorCode::new(Val, 0), None);
        assert_eq!(
            ErrorCode::new(Val, 1).map(|c| c.to_string()),
            Some("MRL-VAL-001".into())
        );
        assert_eq!(
            ErrorCode::new(Val, 999).map(|c| c.to_string()),
            Some("MRL-VAL-999".into())
        );
        assert_eq!(ErrorCode::new(Val, 1000), None);
        assert_eq!(ErrorCode::new(Val, u16::MAX), None);
        for n in [1, 999] {
            let c = ErrorCode::new(Link, n);
            let back = c.map(|c| c.to_string().parse::<ErrorCode>());
            assert_eq!(back, c.map(Ok), "{n}");
        }
    }

    #[test]
    fn every_code_is_unique_and_round_trips() {
        let mut seen = BTreeSet::new();
        for (c, title) in all() {
            assert!(seen.insert(c), "duplicate {c}");
            assert_eq!(c.to_string().parse(), Ok(c));
            assert!(title.ends_with('.') && !title.is_empty(), "{c}");
        }
        // Families reserved by names-frozen-v1 may stay empty until a part adds codes.
        let reserved = [ErrorFamily::State, ErrorFamily::Rwd, ErrorFamily::Node];
        let families: BTreeSet<_> = seen.iter().map(ErrorCode::family).collect();
        let expected: BTreeSet<_> = ErrorFamily::ALL
            .into_iter()
            .filter(|f| !reserved.contains(f) || families.contains(f))
            .collect();
        assert_eq!(families, expected, "every family has a code");
    }

    /// `(code, title)` for every entry of the built guidance catalog.
    fn catalog() -> BTreeMap<String, String> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../guidance/build/catalog.json"
        );
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let json: serde_json::Value = serde_json::from_str(&text).expect("catalog.json is JSON");
        let errors = json["errors"]
            .as_array()
            .expect("catalog has an errors list");
        errors
            .iter()
            .map(|e| {
                let code = e["code"].as_str().expect("code").to_owned();
                (code, e["title"].as_str().expect("title").to_owned())
            })
            .collect()
    }

    #[test]
    fn registry_matches_guidance_catalog() {
        let catalog = catalog();
        let domain: BTreeMap<String, &str> = all().map(|(c, t)| (c.to_string(), t)).collect();
        for (c, title) in &domain {
            assert_eq!(
                catalog.get(c).map(String::as_str),
                Some(*title),
                "{c}: title differs or missing in catalog"
            );
        }
        for c in catalog.keys().filter(|c| c.starts_with("MRL-")) {
            assert!(
                domain.contains_key(c),
                "{c} is in the catalog but not in the domain registry"
            );
        }
    }

    /// Catalog failure codes the domain no longer uses. They live in P05's base data
    /// (`guidance/data/errors.yaml`), which P06 must not edit; the replacement is in `p06.yaml`.
    const RETIRED_IN_CATALOG: &[&str] = &["OOM/SHM_EXHAUSTED"];

    #[test]
    fn failure_codes_match_guidance_catalog() {
        let catalog = catalog();
        let domain: BTreeSet<String> = FailureClass::ALL
            .iter()
            .map(|&c| FailureCode::from(c).to_string())
            .chain(
                SubCode::ALL
                    .iter()
                    .map(|&s| FailureCode::from(s).to_string()),
            )
            .collect();
        let listed: BTreeSet<String> = catalog
            .keys()
            .filter(|c| !c.starts_with("MRL-") && !RETIRED_IN_CATALOG.contains(&c.as_str()))
            .cloned()
            .collect();
        assert_eq!(
            domain, listed,
            "failure classes and sub-codes differ from the catalog"
        );
        for c in RETIRED_IN_CATALOG {
            assert!(
                c.parse::<FailureCode>().is_err(),
                "{c} is retired but still parses"
            );
        }
    }
}
