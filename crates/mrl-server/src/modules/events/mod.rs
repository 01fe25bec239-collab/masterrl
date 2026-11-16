//! The `events` module: migrations at startup, the database readiness check, the
//! leader-elected outbox dispatcher and the retention job.
//!
//! Writing an event (inside the transaction that changes the aggregate):
//! ```text
//! db.transaction(|tx| Box::pin(async move {
//!     sqlx::query("UPDATE jobs SET state = $2, state_version = $3 WHERE id = $1")
//!         .bind(id).bind("QUEUED").bind(v).execute(&mut **tx).await?;
//!     OutboxWriter::write(tx, &NewEvent::new("job.queued", "job", id, v, json!({}))?).await
//! })).await?;
//! ```
//!
//! Consuming events: implement [`EventConsumer`] and add it inside `// <mrl:consumers>`
//! in `registry.rs`.

mod dispatcher;
mod metrics;
mod outbox;
mod retention;

pub use dispatcher::{
    AlertHook, ConsumerError, Dispatcher, DispatcherOptions, EventConsumer, Quarantined,
};
pub use metrics::EventMetrics;
pub use outbox::{Event, NewEvent, OutboxWriter, CHANNEL};
pub use retention::{sweep, Swept};

use crate::app::{ServerModule, Task};
use crate::config::Role;
use crate::db::{Db, DbReady, LeaderElection, Migrations};
use crate::seams::{ReadinessCheck, SeamError, StartupStep};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// How often retention runs.
const SWEEP_EVERY: Duration = Duration::from_secs(3600);

/// Migrations, database readiness, the dispatcher and retention.
pub struct EventsModule {
    db: Db,
    dispatcher: Arc<Dispatcher>,
}

impl EventsModule {
    /// The module with these consumers and default dispatcher options.
    pub fn new(db: Db, consumers: Vec<Arc<dyn EventConsumer>>) -> Self {
        let dispatcher = Dispatcher::new(db.clone(), consumers);
        Self::with_dispatcher(db, dispatcher)
    }

    /// The module with a dispatcher built by the caller (options, alert hook).
    pub fn with_dispatcher(db: Db, dispatcher: Dispatcher) -> Self {
        Self {
            db,
            dispatcher: Arc::new(dispatcher),
        }
    }
}

impl ServerModule for EventsModule {
    fn name(&self) -> &'static str {
        "events"
    }

    fn startup_steps(&self) -> Vec<Arc<dyn StartupStep>> {
        vec![
            Arc::new(UniqueConsumers(self.dispatcher.consumer_names())),
            Arc::new(Migrations(self.db.clone())),
        ]
    }

    fn readiness_checks(&self) -> Vec<Arc<dyn ReadinessCheck>> {
        vec![Arc::new(DbReady(self.db.clone()))]
    }

    fn background_tasks(&self) -> Vec<Task> {
        let dispatcher = self.dispatcher.clone();
        let db = self.db.clone();
        let names = self.dispatcher.consumer_names();
        vec![
            Task::new(
                "events.dispatcher",
                Role::Background,
                move |cancel| async move {
                    dispatcher.run(cancel).await;
                    Ok(())
                },
            ),
            Task::new(
                "events.retention",
                Role::Background,
                move |cancel| async move {
                    let election = LeaderElection::new(
                        db.clone(),
                        "events-retention",
                        Duration::from_secs(30),
                    );
                    election
                        .run(cancel, |lead| {
                            let (db, names) = (db.clone(), names.clone());
                            async move {
                                loop {
                                    let swept = sweep(&db, &names).await?;
                                    tracing::info!(?swept, "event retention done");
                                    tokio::select! {
                                        () = lead.cancelled() => return Ok(()),
                                        () = tokio::time::sleep(SWEEP_EVERY) => {}
                                    }
                                }
                            }
                        })
                        .await;
                    Ok(())
                },
            ),
        ]
    }
}

/// Two consumers with one name would share a cursor; refuse to start.
struct UniqueConsumers(Vec<&'static str>);

#[async_trait]
impl StartupStep for UniqueConsumers {
    fn name(&self) -> &str {
        "event-consumers"
    }

    async fn run(&self) -> Result<(), SeamError> {
        let mut seen = HashSet::new();
        match self.0.iter().find(|n| !seen.insert(**n)) {
            Some(dup) => Err(SeamError::new(
                "events",
                format!("two event consumers are named {dup:?}; consumer names must be unique"),
            )),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn duplicate_consumer_names_stop_startup() {
        assert!(UniqueConsumers(vec!["a", "b"]).run().await.is_ok());
        let err = UniqueConsumers(vec!["a", "b", "a"])
            .run()
            .await
            .unwrap_err();
        assert!(err.detail.contains("\"a\""), "{err}");
    }
}
