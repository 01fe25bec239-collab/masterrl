//! Server modules. Each part adds one `pub mod <name>;` line inside the block below
//! and registers its module in `registry.rs`.

// <mrl:modules>
pub mod builtin;
pub mod events;

// <mrl:modules P16> P16 `pub mod` lines go right below this line.
pub mod audit;
pub mod auth;
pub mod keychain;
pub mod sync;
pub mod tenancy;

// <mrl:modules P17> P17 `pub mod` lines go right below this line.
pub mod jobs;
pub mod runs;

// <mrl:modules P18> P18 `pub mod` lines go right below this line.
pub mod nodes;

// <mrl:modules P19> P19 `pub mod` lines go right below this line.
pub mod artifacts;
pub mod lineage;
pub mod secrets;
pub mod uploads;
// <mrl:modules P21> P21 `pub mod` lines go right below this line.
pub mod compute;
pub mod scheduler;
// <mrl:modules P22> P22 `pub mod` lines go right below this line.
pub mod alerts;
pub mod streams;
pub mod telemetry;
// <mrl:modules P23> P23 `pub mod` lines go right below this line.
pub mod checkpoints;
// <mrl:modules P24> P24 `pub mod` lines go right below this line.
pub mod engines;
// <mrl:modules P26> P26 `pub mod` lines go right below this line.
// <mrl:modules P27> P27 `pub mod` lines go right below this line.
// <mrl:modules P28> P28 `pub mod` lines go right below this line.
// <mrl:modules P29> P29 `pub mod` lines go right below this line.
// <mrl:modules P30> P30 `pub mod` lines go right below this line.
pub mod desktop;
// </mrl:modules>
