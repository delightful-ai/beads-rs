//! `bd upgrade` - install latest binary and restart daemon.

use beads_cli::backend::{CliHostBackend, UpgradeMethod, UpgradeOutcome, UpgradeRequest};
use clap::Args;
use serde::Serialize;

use super::super::{print_json, print_line};
use crate::Result;

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Run upgrade in the background (internal).
    #[arg(long, hide = true, default_value_t = false)]
    pub background: bool,
}

#[derive(Serialize)]
struct UpgradeJson<'a> {
    updated: bool,
    from_version: &'a str,
    to_version: Option<&'a str>,
    install_path: &'a str,
    method: &'a str,
}

pub(crate) fn handle<B>(json: bool, background: bool, backend: &B) -> Result<()>
where
    B: CliHostBackend<Error = crate::Error>,
{
    let outcome = backend.run_upgrade(UpgradeRequest { background })?;
    if json {
        let payload = UpgradeJson {
            updated: outcome.updated,
            from_version: &outcome.from_version,
            to_version: outcome.to_version.as_deref(),
            install_path: outcome.install_path.to_str().unwrap_or(""),
            method: method_str(outcome.method),
        };
        print_json(&payload)?;
        return Ok(());
    }

    if background {
        return Ok(());
    }

    render_human(&outcome)
}

fn render_human(outcome: &UpgradeOutcome) -> Result<()> {
    if !outcome.updated {
        return print_line(&format!(
            "bd is up to date (version {}).",
            outcome.from_version
        ));
    }

    print_line(&format!(
        "Upgraded bd from {} to {} using {}.",
        outcome.from_version,
        outcome.to_version.as_deref().unwrap_or("unknown"),
        method_str(outcome.method),
    ))?;
    print_line(&format!("Installed: {}", outcome.install_path.display()))
}

fn method_str(method: UpgradeMethod) -> &'static str {
    match method {
        UpgradeMethod::Prebuilt => "prebuilt binary",
        UpgradeMethod::Cargo => "cargo build",
        UpgradeMethod::None => "none",
    }
}
