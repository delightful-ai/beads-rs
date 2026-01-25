//! `bd upgrade` - install latest binary and restart daemon.

use clap::Args;
use serde::Serialize;

use super::super::print_json;
use crate::Result;
use crate::config::load_or_init;
use crate::upgrade::{UpgradeMethod, UpgradeOutcome, run_upgrade};

#[derive(Serialize)]
struct UpgradeJson<'a> {
    updated: bool,
    from_version: &'a str,
    to_version: Option<&'a str>,
    install_path: &'a str,
    method: &'a str,
}

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Run upgrade in the background (internal).
    #[arg(long, hide = true, default_value_t = false)]
    pub background: bool,
}

pub(crate) fn handle(json: bool, background: bool) -> Result<()> {
    let cfg = load_or_init();
    let outcome = run_upgrade(cfg, background)?;
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

    render_human(&outcome);
    Ok(())
}

fn render_human(outcome: &UpgradeOutcome) {
    if !outcome.updated {
        println!("bd is up to date (version {}).", outcome.from_version);
        return;
    }

    println!(
        "Upgraded bd from {} to {} using {}.",
        outcome.from_version,
        outcome.to_version.as_deref().unwrap_or("unknown"),
        method_str(outcome.method),
    );
    println!("Installed: {}", outcome.install_path.display());
}

fn method_str(method: UpgradeMethod) -> &'static str {
    match method {
        UpgradeMethod::Prebuilt => "prebuilt binary",
        UpgradeMethod::Cargo => "cargo build",
        UpgradeMethod::None => "none",
    }
}
