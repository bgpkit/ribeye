//! RIB data processors.
//!
//! This module contains the processors that are used to process RIB data.

mod as2rel;
mod meta;
mod peer_stats;
mod pfx2as;

pub use peer_stats::PeerStatsProcessor;
pub use pfx2as::Prefix2AsProcessor;
