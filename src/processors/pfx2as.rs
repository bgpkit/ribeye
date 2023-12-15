use crate::processors::meta::{get_default_output_paths, ProcessorMeta, RibMeta};
use crate::MessageProcessor;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prefix2AsCount {
    pub prefix: String,
    pub asn: u32,
    pub count: usize,
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
        Some(get_default_output_paths(
            self.rib_meta.as_ref().unwrap(),
            &self.processor_meta,
        ))
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
        let value = json!({
            "project": rib_meta.project.as_str(),
            "collector": rib_meta.collector.as_str(),
            "rib_dump_url": rib_meta.rib_dump_url.as_str(),
            "pfx2as": &self.get_count_vec(),
        });

        serde_json::to_string_pretty(&value).ok()
    }
}
