use bgpkit_broker::BrokerItem;
use chrono::Timelike;
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use ribeye::processors::PeerStatsProcessor;
use ribeye::{MessageProcessor, RibEye};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Process recent RIB dump files
    Cook {
        /// Number of days to search back for
        #[clap(short, long, default_value = "1")]
        days: u32,
    },
}

fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "none,ribeye=info");
    }
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = Cli::parse();

    match opts.command {
        Commands::Cook { days } => {
            // find corresponding RIB dump files
            let now = chrono::Utc::now().naive_utc();
            let ts_start = now - chrono::Duration::days(days as i64);
            let rib_files = bgpkit_broker::BgpkitBroker::new()
                .broker_url("https://api.broker.bgpkit.com/v3")
                .data_type("rib")
                .ts_start(ts_start.timestamp())
                .query()
                .unwrap()
                .into_iter()
                .filter(|entry| entry.ts_start.hour() == 0)
                .collect::<Vec<BrokerItem>>();

            info!("Found {} matching RIB dump files", rib_files.len());

            rib_files.par_iter().for_each(|item| {
                let url = item.url.clone();
                let peer_stats = PeerStatsProcessor::new_from_broker_item(item, "./results");
                let mut ribeye = RibEye::new();
                ribeye.add_processor(peer_stats.to_boxed()).unwrap();
                ribeye.process_mrt_file(url.as_str()).unwrap();
            });
        }
    }
}
