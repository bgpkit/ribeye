use crate::processors::meta::{get_default_output_paths, ProcessorMeta, RibMeta};
use crate::MessageProcessor;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

#[derive(Serialize, Deserialize)]
struct As2relEntry {
    pub asn1: u32,
    pub asn2: u32,
    pub paths_count: usize,
    pub peers_count: usize,
    pub rel: u8,
}

pub struct As2relProcessor {
    rib_meta: Option<RibMeta>,
    processor_meta: ProcessorMeta,
    as2rel_map: HashMap<(u32, u32, u8), (usize, HashSet<IpAddr>)>,
}

const TIER1: [u32; 17] = [
    6762, 12956, 2914, 3356, 6453, 1239, 701, 6461, 3257, 1299, 3491, 7018, 3320, 5511, 6830, 174,
    6939,
];

impl As2relProcessor {
    pub fn new(output_dir: &str) -> Self {
        let processor_meta = ProcessorMeta {
            name: "as2rel".to_string(),
            output_dir: output_dir.to_string(),
        };

        Self {
            rib_meta: None,
            processor_meta,
            as2rel_map: HashMap::new(),
        }
    }

    fn get_count_vec(&self) -> Vec<As2relEntry> {
        let res: Vec<As2relEntry> = self
            .as2rel_map
            .iter()
            .map(|((asn1, asn2, rel), (count, peers))| As2relEntry {
                asn1: *asn1,
                asn2: *asn2,
                paths_count: *count,
                peers_count: peers.len(),
                rel: *rel,
            })
            .collect();
        res
    }
}

impl MessageProcessor for As2relProcessor {
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
        // skip processing non-announce messages
        if elem.elem_type != ElemType::ANNOUNCE {
            return Ok(());
        }

        // skip default route
        if elem.prefix.prefix.prefix_len() == 0 {
            return Ok(());
        }

        // skip no-path or non-regular path
        if elem.as_path.is_none() {
            return Ok(());
        }

        let mut u32_path = match elem.as_path.as_ref().unwrap().to_u32_vec_opt(true) {
            None => return Ok(()),
            Some(p) => p,
        };

        // get peers count
        for (asn1, asn2) in u32_path.iter().tuple_windows::<(&u32, &u32)>() {
            let (msg_count, peers) = self
                .as2rel_map
                .entry((*asn1, *asn2, 0))
                .or_insert((0, HashSet::new()));
            *msg_count += 1;
            peers.insert(elem.peer_ip);
        }

        let contains_tier1 = u32_path.iter().any(|x| TIER1.contains(x));

        if !contains_tier1 {
            return Ok(());
        }

        // reverse path order to make origin first
        u32_path.reverse();

        // find the first tier-1 AS index
        let mut first_tier1: usize = usize::MAX;
        for (i, asn) in u32_path.iter().enumerate() {
            if TIER1.contains(asn) && first_tier1 == usize::MAX {
                first_tier1 = i;
                break;
            }
        }

        // origin to first tier 1
        if first_tier1 < u32_path.len() - 1 {
            for i in 0..first_tier1 {
                let (asn1, asn2) = (u32_path.get(i).unwrap(), u32_path.get(i + 1).unwrap());
                let (msg_count, peers) = self
                    .as2rel_map
                    .entry((*asn2, *asn1, 1))
                    .or_insert((0, HashSet::new()));
                *msg_count += 1;
                peers.insert(elem.peer_ip);
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
            "as2rel": &self.get_count_vec(),
        });

        serde_json::to_string_pretty(&value).ok()
    }
}
