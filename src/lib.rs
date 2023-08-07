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

#[cfg(feature = "processors")]
pub mod processors;

use anyhow::Result;
use bgpkit_parser::BgpElem;
use tracing::info;

pub trait MessageProcessor {
    /// Get the name of the processor
    fn name(&self) -> String;

    /// Initialize the processor, return None if the processor should be skipped
    fn initialize(&mut self) -> Result<SkipProcessor> {
        // by default, do not skip any processor
        Ok(SkipProcessor::No)
    }

    /// Process a single entry in the RIB
    fn process_entry(&mut self, elem: &BgpElem) -> Result<()>;

    /// Finalize the processor, including producing the output and storing it
    fn finalize(&mut self) -> Result<()>;

    fn to_boxed(self) -> Box<dyn MessageProcessor>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[derive(Default)]
pub enum SkipProcessor {
    Yes,
    #[default]
    No,
}

impl SkipProcessor {
    pub fn should_skip(&self) -> bool {
        match self {
            SkipProcessor::Yes => true,
            SkipProcessor::No => false,
        }
    }
}

#[derive(Default)]
pub struct RibEye {
    processors: Vec<Box<dyn MessageProcessor>>,
}

impl RibEye {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a processor to the pipeline
    pub fn add_processor(&mut self, mut processor: Box<dyn MessageProcessor>) -> Result<()> {
        if !processor.initialize()?.should_skip() {
            self.processors.push(processor);
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
            processor.finalize()?;
        }
        Ok(())
    }
}
