#![doc(
    html_logo_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/icon-transparent.png",
    html_favicon_url = "https://raw.githubusercontent.com/bgpkit/assets/main/logos/favicon.ico"
)]

use anyhow::Result;
use bgpkit_parser::BgpElem;

pub trait MessageProcessor {
    /// Get the name of the processor
    fn name(&self) -> String;

    /// Initialize the processor
    fn initialize(&mut self) -> Result<()>;

    /// Process a single entry in the RIB
    fn process_entry(&mut self, elem: &BgpElem) -> Result<()>;

    /// Finalize the processor, including producing the output and storing it
    fn finalize(&mut self) -> Result<()>;
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
    pub fn add_processor(&mut self, processor: Box<dyn MessageProcessor>) {
        self.processors.push(processor);
    }

    /// Initialize all processors
    pub fn initialize(&mut self) -> Result<()> {
        for processor in &mut self.processors {
            processor.initialize()?;
        }
        Ok(())
    }

    /// Process each entry in
    pub fn process_mrt_file(&mut self, rib_path: &str) -> Result<()> {
        let parser = bgpkit_parser::BgpkitParser::new(rib_path)?;
        for msg in parser {
            for processor in &mut self.processors {
                processor.process_entry(&msg)?;
            }
        }
        Ok(())
    }

    /// Finalize all processors
    pub fn finalize(&mut self) -> Result<()> {
        for processor in &mut self.processors {
            processor.finalize()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bgpkit_parser::models::ElemType;
    use super::*;

    struct EntryCounter {
        a_count: usize,
        w_count: usize,
    }

    impl EntryCounter {
        pub fn new() -> Self {
            Self {
                a_count: 0,
                w_count: 0,
            }
        }
    }

    #[test]
    fn test_basic_processor() {

        impl MessageProcessor for EntryCounter {

            fn name(&self) -> String {
                "basic_counter".to_string()
            }

            fn initialize(&mut self) -> Result<()> {
                Ok(())
            }

            fn process_entry(&mut self, elem: &BgpElem) -> Result<()> {
                match elem.elem_type {
                    ElemType::ANNOUNCE => { self.a_count += 1; }
                    ElemType::WITHDRAW => { self.w_count += 1; }
                }
                Ok(())
            }

            fn finalize(&mut self) -> Result<()> {
                println!("{}: {} announcements, {} withdrawals", self.name(), self.a_count, self.w_count);
                Ok(())
            }
        }

        let mut ribeye = RibEye::new();
        ribeye.add_processor(Box::new(EntryCounter::new()));
        ribeye.initialize().unwrap();
        ribeye.process_mrt_file("https://data.ris.ripe.net/rrc21/2023.08/updates.20230806.1640.gz").unwrap();
        ribeye.finalize().unwrap();
    }
}