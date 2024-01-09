use crate::processors::meta::{
    get_default_output_path, get_latest_output_path, ProcessorMeta, RibMeta,
};
use crate::processors::write_output_file;
use crate::MessageProcessor;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prefix2AsCount {
    pub prefix: String,
    pub asn: u32,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prefix2AsCollectorJson {
    pub project: String,
    pub collector: String,
    pub rib_dump_url: String,
    pub pfx2as: Vec<Prefix2AsCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prefix2AsSummaryJson {
    rib_dump_urls: Vec<String>,
    pfx2as: Vec<Prefix2AsCount>,
}

pub struct Prefix2AsProcessor {
    rib_meta: Option<RibMeta>,
    processor_meta: ProcessorMeta,
    pfx2as_map: HashMap<(String, u32), u32>,
}

impl Prefix2AsProcessor {
    pub fn new(output_dir: &str) -> Self {
        let processor_meta = ProcessorMeta {
            name: "pfx2as".to_string(),
            output_dir: output_dir.to_string(),
        };

        Prefix2AsProcessor {
            rib_meta: None,
            processor_meta,
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

impl MessageProcessor for Prefix2AsProcessor {
    fn name(&self) -> String {
        self.processor_meta.name.clone()
    }

    fn output_paths(&self) -> Option<Vec<String>> {
        Some(vec![
            get_default_output_path(self.rib_meta.as_ref().unwrap(), &self.processor_meta),
            get_latest_output_path(self.rib_meta.as_ref().unwrap(), &self.processor_meta),
        ])
    }

    fn reset_processor(&mut self, rib_meta: &RibMeta) {
        self.rib_meta = Some(rib_meta.clone());
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
            if let Some(p) = path.to_u32_vec_opt(false) {
                if let Some(origin) = p.last() {
                    let prefix = elem.prefix.to_string();
                    let count = self.pfx2as_map.entry((prefix, *origin)).or_insert(0);
                    *count += 1;
                }
            }
        }

        Ok(())
    }

    fn to_result_string(&self) -> Option<String> {
        let rib_meta = self.rib_meta.as_ref().unwrap();
        let value = json!(Prefix2AsCollectorJson {
            project: rib_meta.project.clone(),
            collector: rib_meta.collector.clone(),
            rib_dump_url: rib_meta.rib_dump_url.clone(),
            pfx2as: self.get_count_vec(),
        });

        serde_json::to_string_pretty(&value).ok()
    }

    fn summarize_latest(&self, rib_metas: &[RibMeta], ignore_error: bool) -> anyhow::Result<()> {
        let mut pfx2as_map = HashMap::<(String, u32), u32>::new();

        for rib_meta in rib_metas {
            let latest_file_path = get_latest_output_path(rib_meta, &self.processor_meta);
            info!("summarizing {}...", latest_file_path.as_str());
            let data = match oneio::read_json_struct::<Prefix2AsCollectorJson>(
                latest_file_path.as_str(),
            ) {
                Ok(d) => d,
                Err(e) => {
                    if ignore_error {
                        warn!("failed to read {}, skipping...", latest_file_path.as_str());
                        continue;
                    } else {
                        return Err(anyhow::anyhow!(
                            "failed to read {}: {}",
                            latest_file_path.as_str(),
                            e
                        ));
                    }
                }
            };

            for entry in data.pfx2as {
                let count = pfx2as_map.entry((entry.prefix, entry.asn)).or_insert(0);
                *count += entry.count as u32;
            }
        }
        let json_data = Prefix2AsSummaryJson {
            rib_dump_urls: rib_metas
                .iter()
                .map(|rib_meta| rib_meta.rib_dump_url.clone())
                .collect(),
            pfx2as: pfx2as_map
                .iter()
                .map(|((prefix, asn), count)| Prefix2AsCount {
                    prefix: prefix.clone(),
                    asn: *asn,
                    count: *count as usize,
                })
                .collect(),
        };

        let output_file_dir = format!(
            "{}/{}",
            self.processor_meta.output_dir.as_str(),
            self.processor_meta.name.as_str(),
        );
        let output_content = serde_json::to_string_pretty(&json_data)?;
        write_output_file(output_file_dir.as_str(), output_content.as_str(), true)?;

        Ok(())
    }
}
