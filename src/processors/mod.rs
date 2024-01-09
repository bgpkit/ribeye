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
use tempfile::tempdir;
use tracing::info;

pub trait MessageProcessor {
    /// Get the name of the processor
    fn name(&self) -> String;

    /// Output paths of the processor. An output path can be a local file path or an S3 path.
    fn output_paths(&self) -> Option<Vec<String>>;

    fn reset_processor(&mut self, rib_meta: &RibMeta);

    /// Process a single entry in the RIB
    fn process_entry(&mut self, elem: &BgpElem) -> Result<()>;

    /// Generate final result in String to be written to output file
    fn to_result_string(&self) -> Option<String> {
        None
    }

    /// Finalize the processor, including producing the output and storing it
    fn output(&mut self) -> Result<()> {
        if self.output_paths().is_none() {
            // no output path, skip
            return Ok(());
        }

        let output_string = match self.to_result_string() {
            None => return Ok(()),
            Some(o) => o,
        };

        let output_paths = self.output_paths().unwrap();

        for output_path in output_paths {
            // if output_path starts with s3://, upload to S3
            if output_path.starts_with("s3://") {
                info!(
                    "finalizing {} processing, writing output to {}",
                    self.name(),
                    output_path.as_str(),
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

                let (bucket, p) = oneio::s3_url_parse(output_path.as_str())?;
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
        }
        Ok(())
    }

    /// Summarize the latest RIBEye result files
    fn summarize_latest(&self, rib_metas: &[RibMeta], ignore_error: bool) -> Result<()>;

    fn to_boxed(self) -> Box<dyn MessageProcessor>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub(crate) fn write_output_file(
    output_file_dir: &str,
    output_content: &str,
    compress: bool,
) -> Result<()> {
    let output_file_path = match compress {
        true => format!("{}/latest.json.bz2", output_file_dir),
        false => format!("{}/latest.json", output_file_dir),
    };
    match output_file_dir.starts_with("s3://") {
        true => {
            // write to a temporary file first
            let tmp_dir = tempdir()?;
            let file_path = tmp_dir
                .path()
                .join("latest.json.bz2")
                .to_string_lossy()
                .to_string();
            let mut writer = oneio::get_writer(file_path.as_str())?;
            write!(writer, "{}", output_content)?;
            drop(writer);

            let (bucket, p) = oneio::s3_url_parse(output_file_path.as_str())?;
            oneio::s3_upload(bucket.as_str(), p.as_str(), file_path.as_str())?;
        }
        false => {
            let mut writer = oneio::get_writer(output_file_path.as_str())?;
            write!(writer, "{}", output_content)?;
            drop(writer);
        }
    }

    Ok(())
}
