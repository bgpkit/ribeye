//! `peer_stats` processor generates basic counting information for route collector peers.
//!
//! Each route collector peer has a corresponding counting struct.

use crate::processors::meta::{
    get_default_output_path, get_latest_output_path, ProcessorMeta, RibMeta,
};
use crate::processors::write_output_file;
use crate::MessageProcessor;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The name of the route collector peer
    pub collector: Option<String>,
    /// The IP address of the route collector peer
    pub ip: IpAddr,
    /// The ASN of the route collector peer
    pub asn: u32,
    /// Number of IPv4 prefixes observed
    pub ipv4_pfxs: HashSet<Ipv4Net>,
    /// Number of IPv6 prefixes observed
    pub ipv6_pfxs: HashSet<Ipv6Net>,
    /// Number of directly connected ASes
    pub num_connected_asns: HashSet<u32>,
    /// Announce IPv4 default route (0.0.0.0/0)
    pub ipv4_default: bool,
    /// Announce IPv6 default route (::/0)
    pub ipv6_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfoEntry {
    pub ip: IpAddr,
    pub collector: Option<String>,
    pub asn: u32,
    pub num_v4_pfxs: usize,
    pub num_v6_pfxs: usize,
    pub num_connected_asns: usize,
    pub has_v4_default: bool,
    pub has_v6_default: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerInfoCollectorJson {
    pub project: String,
    pub collector: String,
    pub rib_dump_url: String,
    pub peers: HashSet<PeerInfoEntry>,
}

impl PartialEq<Self> for PeerInfoEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip
    }
}

impl Hash for PeerInfoEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
    }
}

impl Eq for PeerInfoEntry {}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerInfoSummaryJson {
    pub rib_dump_urls: Vec<String>,
    pub peers: HashSet<PeerInfoEntry>,
}

impl PeerInfo {
    pub fn new_from_ip(ip: IpAddr, asn: u32, collector: Option<String>) -> Self {
        PeerInfo {
            collector,
            ip,
            asn,
            ipv4_pfxs: HashSet::new(),
            ipv6_pfxs: HashSet::new(),
            num_connected_asns: HashSet::new(),
            ipv4_default: false,
            ipv6_default: false,
        }
    }
}

impl From<&PeerInfo> for PeerInfoEntry {
    fn from(peer_info: &PeerInfo) -> Self {
        PeerInfoEntry {
            ip: peer_info.ip,
            collector: peer_info.collector.clone(),
            asn: peer_info.asn,
            num_v4_pfxs: peer_info.ipv4_pfxs.len(),
            num_v6_pfxs: peer_info.ipv6_pfxs.len(),
            num_connected_asns: peer_info.num_connected_asns.len(),
            has_v4_default: peer_info.ipv4_default,
            has_v6_default: peer_info.ipv6_default,
        }
    }
}

pub struct PeerStatsProcessor {
    rib_meta: Option<RibMeta>,
    processor_meta: ProcessorMeta,
    peer_info_map: HashMap<IpAddr, PeerInfo>,
}

impl PeerStatsProcessor {
    pub fn new(output_dir: &str) -> Self {
        let processor_meta = ProcessorMeta {
            name: "peer-stats".to_string(),
            output_dir: output_dir.to_string(),
        };

        PeerStatsProcessor {
            rib_meta: None,
            processor_meta,
            peer_info_map: HashMap::new(),
        }
    }
}

impl MessageProcessor for PeerStatsProcessor {
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
        let collector = self
            .rib_meta
            .as_ref()
            .map(|r| Some(r.collector.clone()))
            .unwrap_or(None);
        let peer_info = self
            .peer_info_map
            .entry(elem.peer_ip)
            .or_insert(PeerInfo::new_from_ip(
                elem.peer_ip,
                elem.peer_asn.to_u32(),
                collector,
            ));

        if elem.elem_type != ElemType::ANNOUNCE {
            // skip processing non-announce messages
            return Ok(());
        }

        if let Some(path) = &elem.as_path {
            if let Some(seq) = path.to_u32_vec_opt(true) {
                if let Some(next_hop) = seq.first() {
                    peer_info.num_connected_asns.insert(*next_hop);
                }
            }
        }

        match elem.prefix.prefix {
            IpNet::V4(p) => {
                if p.prefix_len() == 0 {
                    peer_info.ipv4_default = true;
                }
                peer_info.ipv4_pfxs.insert(p);
            }
            IpNet::V6(p) => {
                if p.prefix_len() == 0 {
                    peer_info.ipv6_default = true;
                }
                peer_info.ipv6_pfxs.insert(p);
            }
        }

        Ok(())
    }

    fn to_result_string(&self) -> Option<String> {
        let rib_meta = self.rib_meta.as_ref().unwrap();
        let value = json!(PeerInfoCollectorJson {
            project: rib_meta.project.clone(),
            collector: rib_meta.collector.clone(),
            rib_dump_url: rib_meta.rib_dump_url.clone(),
            peers: self
                .peer_info_map
                .values()
                .map(|peer_info| peer_info.into())
                .collect(),
        });

        serde_json::to_string_pretty(&value).ok()
    }

    fn summarize_latest(&self, rib_metas: &[RibMeta], ignore_error: bool) -> anyhow::Result<()> {
        let mut peer_info_map = HashMap::<IpAddr, PeerInfoEntry>::new();

        for rib_meta in rib_metas {
            let latest_file_path = get_latest_output_path(rib_meta, &self.processor_meta);
            info!("summarizing {}...", latest_file_path.as_str());
            let data =
                match oneio::read_json_struct::<PeerInfoCollectorJson>(latest_file_path.as_str()) {
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

            for entry in data.peers {
                peer_info_map.insert(entry.ip, entry);
            }
        }

        let peers = self
            .peer_info_map
            .values()
            .map(|peer_info| peer_info.into())
            .collect();

        let json_data = PeerInfoSummaryJson {
            peers,
            rib_dump_urls: rib_metas.iter().map(|r| r.rib_dump_url.clone()).collect(),
        };

        let output_file_dir = format!(
            "{}/{}",
            self.processor_meta.output_dir.as_str(),
            self.processor_meta.name.as_str(),
        );
        let output_content = serde_json::to_string_pretty(&json_data)?;

        write_output_file(output_file_dir.as_str(), output_content.as_str())?;

        Ok(())
    }
}
