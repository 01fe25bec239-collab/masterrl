//! Typed, prefixed IDs.
//!
//! Every ID wraps a UUIDv7. Its text form is a short prefix, an underscore and the
//! 16 UUID bytes in Crockford base32, like `run_01JD8X3F4G5H6J7K8M9N0PQRST`.
//! Parsing accepts lower or upper case; display is always upper case.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// Crockford base32 alphabet (no I, L, O or U).
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Length of the base32 part: 128 bits need 26 characters (130 bits, top 2 always zero).
pub const ENCODED_LEN: usize = 26;

/// Why a text ID could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The text does not start with the expected prefix.
    #[error("this ID should start with `{expected}_`, but it is `{found}`")]
    WrongPrefix {
        /// The prefix this ID type needs, like `run`.
        expected: &'static str,
        /// The text that was given.
        found: String,
    },
    /// The part after the prefix is not 26 characters long.
    #[error("this ID should have {ENCODED_LEN} characters after the prefix, but it has {0}")]
    WrongLength(usize),
    /// The part after the prefix has a character outside the Crockford base32 alphabet.
    #[error("this ID has the character `{0}`, which IDs never use")]
    BadCharacter(char),
    /// The first character is too large: the value would not fit in 128 bits.
    #[error("this ID is too large to be a real ID")]
    Overflow,
}

/// Encode 16 bytes as 26 Crockford base32 characters (most significant first).
pub fn encode_base32(bytes: [u8; 16]) -> String {
    let mut n = u128::from_be_bytes(bytes);
    let mut out = [0u8; ENCODED_LEN];
    for slot in out.iter_mut().rev() {
        // ponytail: the mask keeps the index below 32, so the lookup can't go out of range.
        *slot = ALPHABET[(n & 0x1f) as usize];
        n >>= 5;
    }
    out.iter().map(|&b| char::from(b)).collect()
}

/// Decode 26 Crockford base32 characters (either case) into 16 bytes.
pub fn decode_base32(text: &str) -> Result<[u8; 16], IdError> {
    let len = text.chars().count();
    if len != ENCODED_LEN {
        return Err(IdError::WrongLength(len));
    }
    let mut n: u128 = 0;
    for (i, c) in text.chars().enumerate() {
        let upper = c.to_ascii_uppercase();
        let value = ALPHABET
            .iter()
            .position(|&a| char::from(a) == upper)
            .ok_or(IdError::BadCharacter(c))?;
        if i == 0 && value > 7 {
            return Err(IdError::Overflow);
        }
        n = (n << 5) | value as u128;
    }
    Ok(n.to_be_bytes())
}

macro_rules! ids {
    ($( $(#[$meta:meta])* $name:ident => $prefix:literal, )+) => {
        $(
            $(#[$meta])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[cfg_attr(feature = "sqlx", derive(sqlx::Type), sqlx(transparent))]
            pub struct $name(Uuid);

            impl $name {
                /// The text prefix of this ID type, without the underscore.
                pub const PREFIX: &'static str = $prefix;

                /// A new ID from the current time and random bits (UUIDv7).
                pub fn new() -> Self {
                    Self(Uuid::now_v7())
                }

                /// Wrap an existing UUID.
                pub const fn from_uuid(uuid: Uuid) -> Self {
                    Self(uuid)
                }

                /// The wrapped UUID.
                pub const fn as_uuid(&self) -> Uuid {
                    self.0
                }
            }

            impl Default for $name {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}_{}", $prefix, encode_base32(self.0.into_bytes()))
                }
            }

            impl FromStr for $name {
                type Err = IdError;

                fn from_str(s: &str) -> Result<Self, IdError> {
                    let rest = s
                        .strip_prefix(concat!($prefix, "_"))
                        .ok_or_else(|| IdError::WrongPrefix { expected: $prefix, found: s.to_owned() })?;
                    decode_base32(rest).map(|b| Self(Uuid::from_bytes(b)))
                }
            }

            impl serde::Serialize for $name {
                fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                    s.collect_str(self)
                }
            }

            impl<'de> serde::Deserialize<'de> for $name {
                fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                    let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                    text.parse().map_err(serde::de::Error::custom)
                }
            }
        )+

        /// Every ID prefix, in declaration order.
        pub const PREFIXES: &[&str] = &[$($prefix),+];
    };
}

ids! {
    /// An organization (`org_`).
    OrgId => "org",
    /// A workspace inside an organization (`ws_`).
    WorkspaceId => "ws",
    /// A project inside a workspace (`prj_`).
    ProjectId => "prj",
    /// An experiment that groups runs (`exp_`).
    ExperimentId => "exp",
    /// A training run (`run_`).
    RunId => "run",
    /// A scheduled job (`job_`).
    JobId => "job",
    /// One attempt at running a job (`att_`).
    AttemptId => "att",
    /// A checkpoint (`ckpt_`).
    CheckpointId => "ckpt",
    /// A model (`mdl_`).
    ModelId => "mdl",
    /// One version of a model (`mv_`).
    ModelVersionId => "mv",
    /// A dataset (`ds_`).
    DatasetId => "ds",
    /// One version of a dataset (`dsv_`).
    DatasetVersionId => "dsv",
    /// A compute node (`node_`).
    NodeId => "node",
    /// One GPU device on a node (`gpu_`).
    GpuId => "gpu",
    /// A pool of nodes (`pool_`).
    PoolId => "pool",
    /// A resource reservation (`rsv_`).
    ReservationId => "rsv",
    /// A resource estimate (`est_`).
    EstimateId => "est",
    /// A reward definition (`rwd_`).
    RewardId => "rwd",
    /// One version of a reward (`rwdv_`).
    RewardVersionId => "rwdv",
    /// A reward component (`rwc_`).
    RewardComponentId => "rwc",
    /// A policy version used for weight sync (`pv_`).
    PolicyVersionId => "pv",
    /// A batch of rollouts (`rb_`).
    RolloutBatchId => "rb",
    /// A feedback task (`fbt_`).
    FeedbackTaskId => "fbt",
    /// A feedback record (`fbr_`).
    FeedbackRecordId => "fbr",
    /// An annotation guideline (`gl_`).
    GuidelineId => "gl",
    /// A safety monitor (`mon_`).
    MonitorId => "mon",
    /// An alert (`alr_`).
    AlertId => "alr",
    /// An evaluation run (`evr_`).
    EvaluationId => "evr",
    /// A deployment (`dep_`).
    DeploymentId => "dep",
    /// A cloud instance (`cinst_`).
    CloudInstanceId => "cinst",
    /// An upload (`up_`).
    UploadId => "up",
    /// A stored artifact (`art_`).
    ArtifactId => "art",
    /// A secret reference (`sec_`).
    SecretId => "sec",
    /// An access token (`tok_`).
    TokenId => "tok",
    /// A user (`usr_`).
    UserId => "usr",
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn prefixes_are_unique_and_lowercase() {
        let mut seen = std::collections::BTreeSet::new();
        for p in PREFIXES {
            assert!(seen.insert(*p), "duplicate prefix {p}");
            assert!(p.chars().all(|c| c.is_ascii_lowercase()), "{p}");
        }
        assert_eq!(PREFIXES.len(), 35);
    }

    #[test]
    fn known_value_encodes_like_ulid() {
        let id = RunId::from_uuid(Uuid::from_u128(0));
        assert_eq!(id.to_string(), "run_00000000000000000000000000");
        let id = RunId::from_uuid(Uuid::from_u128(u128::MAX));
        assert_eq!(id.to_string(), "run_7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
        let id = JobId::from_uuid(Uuid::from_u128(32));
        assert_eq!(id.to_string(), "job_00000000000000000000000010");
    }

    #[test]
    fn new_ids_are_v7_and_ordered() {
        let a = JobId::new();
        let b = JobId::default();
        assert_eq!(a.as_uuid().get_version_num(), 7);
        assert!(a.to_string() <= b.to_string());
    }

    #[test]
    fn parse_errors_are_specific() {
        assert_eq!(
            "job_00000000000000000000000000".parse::<RunId>(),
            Err(IdError::WrongPrefix {
                expected: "run",
                found: "job_00000000000000000000000000".into()
            })
        );
        assert_eq!("run_000".parse::<RunId>(), Err(IdError::WrongLength(3)));
        assert_eq!(
            "run_0000000000000000000000000U".parse::<RunId>(),
            Err(IdError::BadCharacter('U'))
        );
        assert_eq!(
            "run_80000000000000000000000000".parse::<RunId>(),
            Err(IdError::Overflow)
        );
        let msg = "run_x".parse::<RunId>().map_err(|e| e.to_string());
        assert_eq!(
            msg,
            Err("this ID should have 26 characters after the prefix, but it has 1".into())
        );
    }

    #[test]
    fn parse_accepts_lowercase() {
        let id = RunId::from_uuid(Uuid::from_u128(0xdead_beef));
        let lower = id.to_string().to_lowercase();
        assert_eq!(lower.parse::<RunId>(), Ok(id));
    }

    #[test]
    fn serde_uses_the_text_form() {
        let id = CheckpointId::from_uuid(Uuid::from_u128(7));
        let json = serde_json::to_string(&id).map_err(|e| e.to_string());
        assert_eq!(json, Ok("\"ckpt_00000000000000000000000007\"".to_owned()));
        let back: Result<CheckpointId, _> =
            serde_json::from_str("\"ckpt_00000000000000000000000007\"");
        assert_eq!(back.ok(), Some(id));
        let bad: Result<CheckpointId, _> =
            serde_json::from_str("\"run_00000000000000000000000007\"");
        assert!(bad.is_err());
    }

    proptest! {
        #[test]
        fn text_round_trips(n in any::<u128>()) {
            let id = ModelVersionId::from_uuid(Uuid::from_u128(n));
            let text = id.to_string();
            prop_assert_eq!(text.len(), "mv_".len() + ENCODED_LEN);
            prop_assert_eq!(text.parse::<ModelVersionId>(), Ok(id));
            prop_assert_eq!(text.to_lowercase().parse::<ModelVersionId>(), Ok(id));
        }

        #[test]
        fn base32_round_trips(bytes in any::<[u8; 16]>()) {
            prop_assert_eq!(decode_base32(&encode_base32(bytes)), Ok(bytes));
        }

        #[test]
        fn text_order_matches_uuid_order(a in any::<u128>(), b in any::<u128>()) {
            let (x, y) = (UserId::from_uuid(Uuid::from_u128(a)), UserId::from_uuid(Uuid::from_u128(b)));
            prop_assert_eq!(x.cmp(&y), x.to_string().cmp(&y.to_string()));
        }

        #[test]
        fn serde_round_trips(n in any::<u128>()) {
            let id = TokenId::from_uuid(Uuid::from_u128(n));
            let json = serde_json::to_string(&id).map_err(|e| TestCaseError::fail(e.to_string()))?;
            let back: TokenId = serde_json::from_str(&json).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(back, id);
        }
    }
}
