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
    (
        code(Internal, 110),
        "The database needs an update before the server can start.",
    ),
    (code(Internal, 111), "The database update did not finish."),
    (
        code(Internal, 112),
        "The app's database stopped and could not start again.",
    ),
    (
        code(Internal, 113),
        "The app could not back up its database.",
    ),
    (
        code(Internal, 114),
        "The app could not move its database to the new version.",
    ),
    (
        code(Internal, 115),
        "The server set an event aside after it failed many times.",
    ),
    (code(Env, 110), "The app's database programs are missing."),
    (
        code(Env, 111),
        "Another copy of the app is already using this data folder.",
    ),
    (
        code(Env, 112),
        "This data folder was made by a newer version of the app.",
    ),
    (
        code(Conflict, 110),
        "Too many changes happened at the same time.",
    ),
    // <mrl:errors P12> rows for P12 go right below this line.
    (code(Val, 120), "The agent could not use its settings file."),
    (
        code(ErrorFamily::Node, 121),
        "The agent could not save or read its local files.",
    ),
    (code(Env, 122), "The sandbox can't run on this computer."),
    (
        code(ErrorFamily::Node, 123),
        "The agent could not start or stop the program.",
    ),
    (
        code(ErrorFamily::Node, 124),
        "The agent could not read this computer's hardware.",
    ),
    (
        code(ErrorFamily::Node, 125),
        "The agent could not set itself up to start with your computer.",
    ),
    (code(Val, 126), "The app doesn't know this name."),
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
    (
        code(ErrorFamily::Rwd, 140),
        "The reward policy has a problem.",
    ),
    (
        code(ErrorFamily::Rwd, 141),
        "A condition in the reward policy could not be read.",
    ),
    (
        code(ErrorFamily::Rwd, 142),
        "The reward policy uses a component that isn't installed.",
    ),
    (
        code(ErrorFamily::Rwd, 143),
        "A reward component's settings need a change.",
    ),
    (
        code(ErrorFamily::Rwd, 144),
        "Too many samples in a batch failed to get a reward.",
    ),
    (
        code(ErrorFamily::Rwd, 145),
        "The reward service got a request for a different run or reward version.",
    ),
    (
        code(ErrorFamily::Rwd, 146),
        "This reward's custom code can't run here yet.",
    ),
    (
        code(ErrorFamily::Rwd, 147),
        "The app could not load the reward files.",
    ),
    (
        code(ErrorFamily::Rwd, 148),
        "Step-by-step rewards aren't available yet.",
    ),
    (
        code(ErrorFamily::Rwd, 149),
        "The reward service could not start.",
    ),
    // <mrl:errors P15> rows for P15 go right below this line.
    (
        code(Model, 150),
        "The app could not read this model's files.",
    ),
    (
        code(Model, 151),
        "This model can answer prompts, but the app can't train it.",
    ),
    (
        code(Model, 152),
        "The app could not use this model's tokenizer or chat template.",
    ),
    (code(Data, 150), "The app could not read this dataset file."),
    (
        code(Data, 151),
        "This dataset file has a column type the app doesn't read.",
    ),
    // <mrl:errors P16> rows for P16 go right below this line.
    (code(Auth, 160), "Your sign-in has expired."),
    (code(Auth, 161), "The sign-in did not finish."),
    (code(Auth, 162), "Sign-in isn't set up in this app."),
    (
        code(Auth, 163),
        "This app is signed in to a different account.",
    ),
    (
        code(Link, 160),
        "The app could not reach the sign-in service.",
    ),
    (
        code(Val, 160),
        "This passphrase or recovery key doesn't open your saved setup.",
    ),
    (
        code(ErrorFamily::State, 160),
        "Setup sync isn't turned on yet.",
    ),
    (
        code(ErrorFamily::State, 161),
        "Setup sync runs on your own computer.",
    ),
    (code(Res, 160), "There is no saved setup to restore."),
    (code(Env, 160), "This computer has no keychain service."),
    (
        code(Env, 161),
        "The app could not use this computer's keychain.",
    ),
    (code(Env, 162), "The keychain request was cancelled."),
    (code(Conflict, 160), "This short name is already used here."),
    // <mrl:errors P17> rows for P17 go right below this line.
    (
        code(ErrorFamily::State, 170),
        "This job can't take that step right now.",
    ),
    (
        code(Conflict, 170),
        "The job changed while the app was updating it.",
    ),
    (
        code(Conflict, 171),
        "An experiment with this name already exists in this project.",
    ),
    (code(Idem, 170), "Submitting a run needs a request key."),
    (
        code(Auth, 170),
        "The `mrl` command has no access token yet.",
    ),
    (
        code(Env, 170),
        "The `mrl` command could not reach the server.",
    ),
    // <mrl:errors P18> rows for P18 go right below this line.
    (
        code(ErrorFamily::Node, 180),
        "The join token can't be used.",
    ),
    (
        code(ErrorFamily::Node, 181),
        "The app did not accept this machine's certificate.",
    ),
    (
        code(ErrorFamily::Node, 182),
        "This machine has not joined the app yet.",
    ),
    (
        code(ErrorFamily::Node, 183),
        "The app's certificate authority can't be used.",
    ),
    (
        code(ErrorFamily::Link, 184),
        "The app and this machine can't reach each other.",
    ),
    (
        code(ErrorFamily::Link, 185),
        "Confirm this machine's key before the first connection.",
    ),
    (
        code(ErrorFamily::Link, 186),
        "This machine's sign-in key changed.",
    ),
    (
        code(ErrorFamily::Node, 187),
        "The app did not send this command to the machine.",
    ),
    (
        code(ErrorFamily::Node, 188),
        "This machine has too many messages waiting for the app.",
    ),
    (
        code(ErrorFamily::Node, 189),
        "The app and the agent don't understand each other.",
    ),
    // <mrl:errors P19> rows for P19 go right below this line.
    (code(Conflict, 190), "This name is already used here."),
    (code(Conflict, 191), "This upload is already finished."),
    (code(Conflict, 192), "This secret is being deleted."),
    (
        code(Store, 190),
        "Some parts of this upload are missing or the wrong size.",
    ),
    (code(Perm, 190), "This secret can't be used for this."),
    (code(Res, 190), "This secret has no value yet."),
    (
        code(Env, 190),
        "This server has no place to keep secret values.",
    ),
    // <mrl:errors P20> rows for P20 go right below this line.
    (
        code(Val, 200),
        "The training attempt has settings the agent can't use.",
    ),
    (
        code(Env, 201),
        "The training environment's lock file doesn't match its fingerprint.",
    ),
    (
        code(Env, 202),
        "The training environment could not be installed.",
    ),
    (
        code(ErrorFamily::Node, 203),
        "The agent could not open the connection to the training worker.",
    ),
    (
        code(ErrorFamily::State, 204),
        "This training attempt can't do that right now.",
    ),
    (
        code(Val, 205),
        "A file path in the training attempt points outside its folder.",
    ),
    (
        code(ErrorFamily::Node, 206),
        "This machine is not taking new training runs right now.",
    ),
    (
        code(ErrorFamily::Node, 207),
        "The agent was stopping and did not run the command.",
    ),
    // <mrl:errors P21> rows for P21 go right below this line.
    (
        code(ErrorFamily::Cap, 210),
        "No machine in this pool can run this job as asked.",
    ),
    (code(Quota, 210), "This job is waiting for a free machine."),
    (
        code(Quota, 211),
        "This pool is paused and starts no new work.",
    ),
    (
        code(Quota, 212),
        "Too many jobs of this project are ahead in the queue.",
    ),
    (code(Perm, 210), "No pool is open to this project."),
    (
        code(Conflict, 210),
        "This reservation was changed by a newer holder.",
    ),
    (
        code(ErrorFamily::State, 210),
        "The agent lost its credentials before launch.",
    ),
    // <mrl:errors P22> rows for P22 go right below this line.
    (
        code(ErrorFamily::Quota, 220),
        "Too many live views are open.",
    ),
    (
        code(ErrorFamily::State, 220),
        "This alert is already resolved.",
    ),
    (
        code(Store, 220),
        "A saved log or sample file is missing or damaged.",
    ),
    // <mrl:errors P23> rows for P23 go right below this line.
    (
        code(ErrorFamily::State, 230),
        "This checkpoint isn't committed, so it can't be used yet.",
    ),
    (
        code(Conflict, 230),
        "This checkpoint is still in use, so it can't be deleted.",
    ),
    // <mrl:errors P24> rows for P24 go right below this line.
    (
        code(ErrorFamily::Cap, 240),
        "This engine can't train with these settings.",
    ),
    (
        code(ErrorFamily::Cap, 241),
        "This engine hasn't been checked on this kind of machine yet.",
    ),
    (
        code(ErrorFamily::Cap, 242),
        "This method needs an input the run doesn't have.",
    ),
    (
        code(ErrorFamily::Cap, 243),
        "This dataset's format doesn't fit this method.",
    ),
    (
        code(ErrorFamily::Cap, 244),
        "This method can't train this kind of model.",
    ),
    (
        code(ErrorFamily::Cap, 245),
        "No connected machine has the hardware this run asks for.",
    ),
    (
        code(Model, 240),
        "The app has no record of this model version.",
    ),
    (
        code(Data, 240),
        "The app has no record of this dataset version.",
    ),
    // <mrl:errors P25> rows for P25 go right below this line.
    (
        code(ErrorFamily::Cap, 250),
        "This setting needs an NVIDIA GPU.",
    ),
    (
        code(ErrorFamily::Cap, 251),
        "This checkpoint can't resume training.",
    ),
    // <mrl:errors P26> rows for P26 go right below this line.
    // <mrl:errors P27> rows for P27 go right below this line.
    // <mrl:errors P28> rows for P28 go right below this line.
    // <mrl:errors P29> rows for P29 go right below this line.
    (
        code(ErrorFamily::Rwd, 290),
        "This reward uses a learned reward model or a judge model, which isn't available yet.",
    ),
    (
        code(ErrorFamily::Rwd, 291),
        "The reward service sent an answer the app can't trust.",
    ),
    (
        code(ErrorFamily::Rwd, 292),
        "The reward service runs a different reward than this job uses.",
    ),
    (
        code(ErrorFamily::Rwd, 293),
        "The reward service stopped working.",
    ),
    (
        code(ErrorFamily::Rwd, 294),
        "The sandbox stopped the reward code.",
    ),
    (
        code(ErrorFamily::Rwd, 295),
        "Custom reward code can't run on this machine.",
    ),
    (
        code(ErrorFamily::Rwd, 296),
        "The reward preview can't write answers on this machine.",
    ),
    (
        code(ErrorFamily::Rwd, 297),
        "The results of a reward check could not be read.",
    ),
    (
        code(ErrorFamily::Rwd, 298),
        "The command took too long or printed too much.",
    ),
    (
        code(ErrorFamily::State, 290),
        "This reward version can't take that step right now.",
    ),
    (
        code(ErrorFamily::State, 291),
        "This reward version can't be published until its checks pass.",
    ),
    (
        code(ErrorFamily::State, 292),
        "This reward version has nothing to check against yet.",
    ),
    (
        code(ErrorFamily::Conflict, 290),
        "A reward with this name already exists in this project.",
    ),
    (
        code(ErrorFamily::Val, 290),
        "A reward test case can't be read.",
    ),
    // <mrl:errors P30> rows for P30 go right below this line.
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
