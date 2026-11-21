//! Runs, jobs and the `mrl` binary against PostgreSQL and a real team-mode test server.
//! Every test needs `DATABASE_URL` (`just dev`) and runs with `just test-integration`.

mod cli;
mod runs_jobs;
mod support;
