use bgpkit_broker::BrokerItem;
use chrono::Timelike;
use clap::{Parser, Subcommand};
use itertools::Itertools;
use rayon::prelude::*;
use ribeye::processors::RibMeta;
use ribeye::RibEye;
use std::process::exit;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,

    /// Path to environment variables file
    #[clap(short, long, global = true)]
    env: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Process recent RIB dump files
    Cook {
        /// Number of days to search back for
        #[clap(long, default_value = "1")]
        days: u32,

        /// limit to process the smallest N RIB dump files
        #[clap(short, long)]
        limit: Option<usize>,

        /// specify processors to use.
        ///
        /// Available processors: pfx2as, pfx2dist, as2rel, peer_stats
        ///
        /// If not specified, all processors will be used
        #[clap(short, long)]
        processors: Vec<String>,

        /// Root data directory
        #[clap(short, long, default_value = "./results")]
        dir: String,

        /// Only summarize latest results
        #[clap(long)]
        summarize_only: bool,
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

    if let Some(env_path) = opts.env {
        match dotenvy::from_path_override(env_path.as_str()) {
            Ok(_) => {
                info!("loaded environment variables from {}", env_path);
            }
            Err(_) => {
                error!("failed to load environment variables from {}", env_path);
                exit(1);
            }
        };
    }
    dotenvy::dotenv().ok();

    match opts.command {
        Commands::Cook {
            days,
            processors,
            dir,
            limit,
            summarize_only,
        } => {
            // check s3 environment variables if dir starts with s3://
            if dir.starts_with("s3://") && oneio::s3_env_check().is_err() {
                error!("S3 environment variables not set");
                exit(1);
            }

            // find corresponding RIB dump files
            let now = chrono::Utc::now().naive_utc();
            let ts_start = now - chrono::Duration::days(days as i64);
            info!("Searching for RIB dump files since {}", ts_start);
            let mut rib_files = bgpkit_broker::BgpkitBroker::new()
                .broker_url("https://api.broker.bgpkit.com/v3")
                .data_type("rib")
                .ts_start(ts_start.and_utc().timestamp())
                .ts_end(now.and_utc().timestamp())
                .query()
                .unwrap()
                .into_iter()
                .filter(|entry| entry.ts_start.hour() == 0)
                .sorted_by_key(|entry| entry.rough_size)
                .collect::<Vec<BrokerItem>>();
            rib_files = match limit {
                None => rib_files,
                Some(l) => rib_files.into_iter().take(l).collect::<Vec<BrokerItem>>(),
            };

            let rib_metas: Vec<RibMeta> = rib_files.iter().map(RibMeta::from).collect();

            if !summarize_only {
                // process each RIB file in parallel with provided meta information
                info!("processing {} matching RIB dump files", rib_files.len(),);
                rib_metas.par_iter().for_each(|rib_meta| {
                    let mut ribeye =
                        match RibEye::new().with_processor_names(&processors, dir.as_str()) {
                            Ok(p) => p.with_rib_meta(rib_meta),
                            Err(e) => {
                                error!("failed to initialize RibEye: {}", e);
                                exit(2);
                            }
                        };
                    ribeye
                        .process_mrt_file(rib_meta.rib_dump_url.as_str())
                        .unwrap();
                });
            }

            info!("summarize all latest results");
            let mut ribeye = match RibEye::new().with_processor_names(&processors, dir.as_str()) {
                Ok(p) => p,
                Err(e) => {
                    error!("failed to initialize RibEye: {}", e);
                    exit(3);
                }
            };
            ribeye.summarize_latest_files(&rib_metas).unwrap();
        }
    }
}
