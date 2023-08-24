//! `peer_stats` processor generates basic counting information for route collector peers.
//!
//! Each route collector peer has a corresponding counting struct.

use crate::processors::meta::{get_output_path, ProcessorMeta, RibMeta};
use crate::{MessageProcessor, SkipProcessor};
use anyhow::anyhow;
use bgpkit_broker::BrokerItem;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use chrono::NaiveDateTime;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::net::IpAddr;
use tracing::info;

#[derive(Debug, Clone)]
pub struct PeerInfo {
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

impl PeerInfo {
    pub fn new_from_ip(ip: IpAddr, asn: u32) -> Self {
        PeerInfo {
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

impl Serialize for PeerInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PeerInfo", 7)?;
        state.serialize_field("ip", &self.ip.to_string())?;
        state.serialize_field("asn", &self.asn)?;
        state.serialize_field("num_v4_pfxs", &self.ipv4_pfxs.len())?;
        state.serialize_field("num_v6_pfxs", &self.ipv6_pfxs.len())?;
        state.serialize_field("num_connected_asns", &self.num_connected_asns.len())?;
        state.serialize_field("ipv4_default", &self.ipv4_default)?;
        state.serialize_field("ipv6_default", &self.ipv6_default)?;
        state.end()
    }
}

pub struct PeerStatsProcessor {
    rib_meta: RibMeta,
    processor_meta: ProcessorMeta,
    output_path: String,
    peer_info_map: HashMap<IpAddr, PeerInfo>,
}

impl PeerStatsProcessor {
    pub fn new_from_broker_item(item: &BrokerItem, output_dir: &str) -> Self {
        let rib_meta = RibMeta::from(item);
        let processor_meta = ProcessorMeta {
            name: "peer-stats".to_string(),
            output_dir: output_dir.to_string(),
        };
        let output_path = get_output_path(&rib_meta, &processor_meta);
        Self {
            rib_meta,
            processor_meta,
            output_path,
            peer_info_map: HashMap::new(),
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
            name: "peer-stats".to_string(),
            output_dir: output_dir.to_string(),
        };

        let output_path = get_output_path(&rib_meta, &processor_meta);

        PeerStatsProcessor {
            rib_meta,
            processor_meta,
            output_path,
            peer_info_map: HashMap::new(),
        }
    }
}

impl Serialize for PeerStatsProcessor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PeerStats", 4)?;
        state.serialize_field("project", &self.rib_meta.project.as_str())?;
        state.serialize_field("collector", &self.rib_meta.collector.as_str())?;
        state.serialize_field("rib_dump_url", &self.rib_meta.rib_dump_url.as_str())?;
        state.serialize_field("pfx2as", &self.peer_info_map)?;
        state.end()
    }
}

impl MessageProcessor for PeerStatsProcessor {
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
        let peer_info = self
            .peer_info_map
            .entry(elem.peer_ip)
            .or_insert(PeerInfo::new_from_ip(elem.peer_ip, elem.peer_asn.asn));

        if elem.elem_type != ElemType::ANNOUNCE {
            // skip processing non-announce messages
            return Ok(());
        }

        if let Some(path) = &elem.as_path {
            if let Some(seq) = path.to_u32_vec() {
                peer_info.num_connected_asns.extend(seq);
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

    fn finalize(&mut self) -> anyhow::Result<()> {
        info!(
            "finalizing peer-stats processing, writing output to {}",
            self.output_path.as_str()
        );
        let file = match std::fs::File::create(self.output_path.as_str()) {
            Err(_why) => return Err(anyhow!("couldn't open {}", self.output_path.as_str())),
            Ok(file) => file,
        };

        let compressor = BzEncoder::new(file, Compression::best());
        let mut writer = BufWriter::with_capacity(128 * 1024, compressor);

        let data = json!(self);

        writer.write_all(serde_json::to_string_pretty(&data).unwrap().as_ref())?;

        Ok(())
    }
}
