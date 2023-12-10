//! RIB data processors.
//!
//! This module contains the processors that are used to process RIB data.

mod as2rel;
mod meta;
mod peer_stats;
mod pfx2as;

pub use as2rel::As2relProcessor;
pub use meta::RibMeta;
pub use peer_stats::PeerStatsProcessor;
pub use pfx2as::Prefix2AsProcessor;

use anyhow::Result;
use bgpkit_parser::BgpElem;
use std::io::Write;
use tracing::info;

pub trait MessageProcessor {
    /// Get the name of the processor
    fn name(&self) -> String;

    fn output_path(&self) -> Option<String>;

    /// Check if a should skip processing
    fn should_skip_local(&mut self) -> bool {
        // by default, skip if output path file already exists
        if let Some(path) = self.output_path() {
            if std::path::Path::new(path.as_str()).exists() {
                return true;
            }
        }
        false
    }

    fn reset_processor(&mut self, rib_meta: &RibMeta);

    /// Process a single entry in the RIB
    fn process_entry(&mut self, elem: &BgpElem) -> Result<()>;

    /// Generate final result in String to be written to output file
    fn to_result_string(&self) -> Option<String> {
        None
    }

    /// Finalize the processor, including producing the output and storing it
    fn output(&mut self) -> Result<()> {
        if self.output_path().is_none() {
            // no output path, skip
            return Ok(());
        }

        let output_string = match self.to_result_string() {
            None => return Ok(()),
            Some(o) => o,
        };

        let output_path = self.output_path().unwrap();

        // if output_path starts with s3://, upload to S3
        if output_path.starts_with("s3://") {
            let path = self.output_path().unwrap();
            info!(
                "finalizing {} processing, writing output to {}",
                self.name(),
                path.as_str()
            );

            let temp_dir = tempfile::tempdir().unwrap();
            let file_path = temp_dir
                .path()
                .join("temp.bz2")
                .to_str()
                .unwrap()
                .to_string();
            let mut writer = oneio::get_writer(file_path.as_str()).unwrap();
            writer.write_all(output_string.as_ref())?;
            drop(writer);

            let (bucket, p) = oneio::s3_url_parse(path.as_str())?;
            oneio::s3_upload(bucket.as_str(), p.as_str(), file_path.as_str()).unwrap();
            temp_dir.close().unwrap();
        } else {
            info!(
                "finalizing {} processing, writing output to {}",
                self.name(),
                output_path.as_str()
            );

            let mut writer = oneio::get_writer(output_path.as_str())?;
            writer.write_all(output_string.as_ref())?;
            drop(writer);
        }
        Ok(())
    }

    fn to_boxed(self) -> Box<dyn MessageProcessor>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
