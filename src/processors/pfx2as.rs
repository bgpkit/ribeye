use crate::processors::meta::{get_output_path, ProcessorMeta, RibMeta};
use crate::{MessageProcessor, SkipProcessor};
use anyhow::anyhow;
use bgpkit_broker::BrokerItem;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use chrono::NaiveDateTime;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prefix2AsCount {
    pub prefix: String,
    pub asn: u32,
    pub count: usize,
}
pub struct Prefix2AsProcessor {
    rib_meta: RibMeta,
    processor_meta: ProcessorMeta,
    output_path: String,

    pfx2as_map: HashMap<(String, u32), u32>,
}

impl Prefix2AsProcessor {
    pub fn new_from_broker_item(item: &BrokerItem, output_dir: &str) -> Self {
        let rib_meta = RibMeta::from(item);
        let processor_meta = ProcessorMeta {
            name: "pfx2as".to_string(),
            output_dir: output_dir.to_string(),
        };
        let output_path = get_output_path(&rib_meta, &processor_meta);
        Self {
            rib_meta,
            processor_meta,
            output_path,
            pfx2as_map: HashMap::new(),
        }
    }
    pub fn new(
        project: &str,
        collector: &str,
        rib_dump_url: &str,
        timestamp: &NaiveDateTime,
        output_dir: &str,
    ) -> Self {
        let rib_meta = RibMeta {
            project: project.to_string(),
            collector: collector.to_string(),
            rib_dump_url: rib_dump_url.to_string(),
            timestamp: timestamp.clone(),
        };

        let processor_meta = ProcessorMeta {
            name: "pfx2as".to_string(),
            output_dir: output_dir.to_string(),
        };

        let output_path = get_output_path(&rib_meta, &processor_meta);

        Prefix2AsProcessor {
            rib_meta,
            processor_meta,
            output_path,
            pfx2as_map: HashMap::new(),
        }
    }

    pub fn get_count_vec(&self) -> Vec<Prefix2AsCount> {
        let res: Vec<Prefix2AsCount> = self
            .pfx2as_map
            .iter()
            .map(|((prefix, asn), count)| Prefix2AsCount {
                prefix: prefix.clone(),
                asn: *asn,
                count: *count as usize,
            })
            .collect();
        res
    }
}

impl Serialize for Prefix2AsProcessor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Pfx2as", 4)?;
        state.serialize_field("project", &self.rib_meta.project.as_str())?;
        state.serialize_field("collector", &self.rib_meta.collector.as_str())?;
        state.serialize_field("rib_dump_url", &self.rib_meta.rib_dump_url.as_str())?;
        state.serialize_field("pfx2as", &self.get_count_vec())?;
        state.end()
    }
}

impl MessageProcessor for Prefix2AsProcessor {
    fn name(&self) -> String {
        self.processor_meta.name.clone()
    }

    fn initialize(&mut self) -> anyhow::Result<SkipProcessor> {
        match std::path::Path::new(self.output_path.as_str()).exists() {
            true => {
                info!(
                    "output file {} exists, skip peer-stats processing",
                    self.output_path.as_str()
                );
                Ok(SkipProcessor::Yes)
            }
            false => Ok(SkipProcessor::No),
        }
    }

    fn process_entry(&mut self, elem: &BgpElem) -> anyhow::Result<()> {
        if elem.elem_type != ElemType::ANNOUNCE {
            // skip processing non-announce messages
            return Ok(());
        }

        // skip default route
        if elem.prefix.prefix.prefix_len() == 0 {
            return Ok(());
        }

        if let Some(path) = &elem.as_path {
            if let Some(p) = path.to_u32_vec() {
                if let Some(origin) = p.last() {
                    let prefix = elem.prefix.to_string();
                    let count = self.pfx2as_map.entry((prefix, *origin)).or_insert(0);
                    *count += 1;
                }
            }
        }

        Ok(())
    }

    fn finalize(&mut self) -> anyhow::Result<()> {
        info!(
            "finalizing {} processing, writing output to {}",
            self.name(),
            self.output_path.as_str()
        );
        let file = match std::fs::File::create(self.output_path.as_str()) {
            Err(_why) => return Err(anyhow!("couldn't open {}", self.output_path.as_str())),
            Ok(file) => file,
        };

        let compressor = bzip2::write::BzEncoder::new(file, bzip2::Compression::best());
        let mut writer = BufWriter::with_capacity(128 * 1024, compressor);

        let data = json!(self);

        writer.write_all(serde_json::to_string_pretty(&data).unwrap().as_ref())?;

        Ok(())
    }
}
