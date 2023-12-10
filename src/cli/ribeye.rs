use bgpkit_broker::BrokerItem;
use chrono::Timelike;
use clap::{Parser, Subcommand};
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
        #[clap(short, long, default_value = "1")]
        days: u32,

        /// Output directory
        #[clap(short, long, default_value = "./results")]
        dir: String,
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
        Commands::Cook { days, dir } => {
            // check s3 environment variables if dir starts with s3://
            if dir.starts_with("s3://") && oneio::s3_env_check().is_err() {
                error!("S3 environment variables not set");
                std::process::exit(1);
            }

            // find corresponding RIB dump files
            let now = chrono::Utc::now().naive_utc();
            let ts_start = now - chrono::Duration::days(days as i64);
            info!("Searching for RIB dump files since {}", ts_start);
            let rib_files = bgpkit_broker::BgpkitBroker::new()
                .broker_url("https://api.broker.bgpkit.com/v3")
                .data_type("rib")
                .ts_start(ts_start.timestamp())
                .ts_end(now.timestamp())
                .query()
                .unwrap()
                .into_iter()
                .filter(|entry| entry.ts_start.hour() == 0)
                .collect::<Vec<BrokerItem>>();

            info!("Found {} matching RIB dump files", rib_files.len(),);

            rib_files.par_iter().for_each(|item| {
                let rib_meta = RibMeta::from(item);
                let mut ribeye = RibEye::new()
                    .with_default_processors(dir.as_str())
                    .with_rib_meta(&rib_meta);

                ribeye
                    .process_mrt_file(rib_meta.rib_dump_url.as_str())
                    .unwrap();
            });
        }
    }
}
