//! The list of modules this build registers. Each part adds one line inside the block,
//! like `Box::new(crate::modules::runs::RunsModule::new()),`. An empty list is valid.

use crate::app::ServerModule;

/// Every registered module, in startup order.
pub fn modules() -> Vec<Box<dyn ServerModule>> {
    vec![
        // <mrl:register>
        // </mrl:register>
    ]
}
