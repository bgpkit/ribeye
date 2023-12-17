//! # RibEye
//!
//! RibEye is a framework library for processing RIB dumps using BGPKIT Parser.
//!
//! The key concept of ribeye is the [MessageProcessor] trait, which defines the
//! interface for processing RIB data.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/icon-transparent.png",
    html_favicon_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/favicon.ico"
)]

pub use crate::processors::{MessageProcessor, RibMeta};
use anyhow::Result;
use tracing::info;

#[cfg(feature = "processors")]
pub mod processors;

#[derive(Default)]
pub struct RibEye {
    processors: Vec<Box<dyn MessageProcessor>>,
}

impl RibEye {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add default processors to the pipeline
    ///
    /// The default processors are:
    /// - PeerStatsProcessor
    /// - Prefix2AsProcessor
    /// - As2relProcessor
    pub fn with_default_processors(mut self, output_dir: &str) -> Self {
        self.add_processor(Box::new(processors::PeerStatsProcessor::new(output_dir)));
        self.add_processor(Box::new(processors::Prefix2AsProcessor::new(output_dir)));
        self.add_processor(Box::new(processors::As2relProcessor::new(output_dir)));
        self
    }

    pub fn with_rib_meta(mut self, rib_meta: &RibMeta) -> Self {
        for processor in &mut self.processors {
            processor.reset_processor(rib_meta);
        }
        self
    }

    /// Add a processor to the pipeline
    pub fn add_processor(&mut self, processor: Box<dyn MessageProcessor>) {
        self.processors.push(processor);
    }

    pub fn initialize_processors(&mut self, rib_meta: &RibMeta) -> Result<()> {
        for processor in &mut self.processors {
            processor.reset_processor(rib_meta);
        }
        Ok(())
    }

    /// Process each entry in
    pub fn process_mrt_file(&mut self, file_path: &str) -> Result<()> {
        if self.processors.is_empty() {
            info!("no processors added, skip processing: {}", file_path);
            return Ok(());
        }

        info!("processing RIB file: {}", file_path);

        let parser = bgpkit_parser::BgpkitParser::new(file_path)?;
        for msg in parser {
            for processor in &mut self.processors {
                processor.process_entry(&msg)?;
            }
        }

        for processor in &mut self.processors {
            processor.output()?;
        }
        Ok(())
    }

    pub fn summarize_latest_files(&mut self, rib_metas: &[RibMeta]) -> Result<()> {
        for processor in &mut self.processors {
            info!(
                "summarizing latest files for processor: {}",
                processor.name()
            );
            if let Err(e) = processor.summarize_latest(rib_metas, true) {
                info!("failed to summarize latest files: {}", e);
            }
        }
        Ok(())
    }
}
