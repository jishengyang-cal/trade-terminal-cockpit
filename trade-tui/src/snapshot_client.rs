use crate::cli::Cli;
use anyhow::{Context, Result};
use std::fs;
use trade_core::ProjectionSnapshot;

pub fn load_snapshot(cli: &Cli) -> Result<Option<ProjectionSnapshot>> {
    let Some(path) = cli.snapshot_json.as_ref() else {
        return Ok(None);
    };

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read projection snapshot {}", path.display()))?;
    let snapshot = serde_json::from_str::<ProjectionSnapshot>(&content)
        .with_context(|| format!("failed to decode projection snapshot {}", path.display()))?;
    Ok(Some(snapshot))
}
