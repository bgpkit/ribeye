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
use std::io::Write;
use tracing::info;

pub trait MessageProcessor {
    /// Get the name of the processor
    fn name(&self) -> String;

    fn output_path(&self) -> Option<String>;

    /// Initialize the processor, return None if the processor should be skipped
    fn initialize(&mut self) -> Result<SkipProcessor> {
        match self.output_path() {
            None => {
                // by default, do not skip any processor
                Ok(SkipProcessor::No)
            }
            Some(path) => match std::path::Path::new(path.as_str()).exists() {
                true => {
                    info!(
                        "output file {} exists, skip peer-stats processing",
                        path.as_str()
                    );
                    Ok(SkipProcessor::Yes)
                }
                false => Ok(SkipProcessor::No),
            },
        }
    }

    /// Process a single entry in the RIB
    fn process_entry(&mut self, elem: &BgpElem) -> Result<()>;

    /// Generate final result in String to be written to output file
    fn to_result_string(&self) -> Option<String> {
        None
    }

    /// Finalize the processor, including producing the output and storing it
    fn finalize(&mut self) -> Result<()> {
        if self.output_path().is_none() {
            return Ok(());
        }

        let output_string = match self.to_result_string() {
            None => return Ok(()),
            Some(o) => o,
        };

        let path = self.output_path().unwrap();
        info!(
            "finalizing {} processing, writing output to {}",
            self.name(),
            path.as_str()
        );
        let file = match std::fs::File::create(path.as_str()) {
            Err(_why) => return Err(anyhow::anyhow!("couldn't open {}", path.as_str())),
            Ok(file) => file,
        };

        let compressor = bzip2::write::BzEncoder::new(file, bzip2::Compression::best());
        let mut writer = std::io::BufWriter::with_capacity(128 * 1024, compressor);

        writer.write_all(output_string.as_ref())?;

        Ok(())
    }

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
