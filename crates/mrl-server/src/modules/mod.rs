//! Server modules. Each part adds one `pub mod <name>;` line inside the block below
//! and registers its module in `registry.rs`.

// <mrl:modules>
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
// <mrl:modules P23> P23 `pub mod` lines go right below this line.
// <mrl:modules P24> P24 `pub mod` lines go right below this line.
// </mrl:modules>
