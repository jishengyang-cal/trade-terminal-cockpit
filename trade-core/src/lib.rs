pub mod commands;
pub mod events;
pub mod filter;
pub mod reducer;
pub mod sample;
pub mod state;

pub use commands::{CommandEnvelope, CommandPayload, DangerLevel};
pub use events::{DomainEvent, EventEnvelope};
pub use filter::EventFilter;
pub use reducer::reduce_event;
pub use state::AppState;

pub fn unix_ts_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
