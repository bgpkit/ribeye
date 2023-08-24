use bgpkit_broker::BrokerItem;
use chrono::{Datelike, NaiveDateTime};

/// RibMeta contains the meta information of a RIB dump file.
#[derive(Debug, Default, Clone)]
pub struct RibMeta {
    /// route collector project name (e.g., route-views, riperis)
    pub project: String,
    /// route collector name (e.g., route-views2, rrc00)
    pub collector: String,
    /// RIB dump file URL
    pub rib_dump_url: String,
    /// RIB dump file timestamp
    pub timestamp: NaiveDateTime,
}

impl From<&BrokerItem> for RibMeta {
    fn from(item: &BrokerItem) -> Self {
        let project = match item.collector_id.starts_with("rrc") {
            true => "riperis".to_string(),
            false => "route-views".to_string(),
        };
        RibMeta {
            project,
            collector: item.collector_id.clone(),
            rib_dump_url: item.url.clone(),
            timestamp: item.ts_start,
        }
    }
}

pub fn get_output_path(rib_meta: &RibMeta, processor_meta: &ProcessorMeta) -> String {
    let output_file_dir = format!(
        "{}/{}/{}/{:04}/{:02}",
        processor_meta.output_dir.as_str(),
        processor_meta.name.as_str(),
        rib_meta.collector,
        rib_meta.timestamp.year(),
        rib_meta.timestamp.month(),
    );
    std::fs::create_dir_all(output_file_dir.as_str()).unwrap();
    let output_path = format!(
        "{}/{}_{}_{:04}-{:02}-{:02}_{}.json.bz2",
        output_file_dir.as_str(),
        processor_meta.name.as_str(),
        rib_meta.collector,
        rib_meta.timestamp.year(),
        rib_meta.timestamp.month(),
        rib_meta.timestamp.day(),
        rib_meta.timestamp.timestamp()
    );

    output_path
}

/// ProcessorMeta contains the meta information of a RIB processor.
#[derive(Debug, Clone)]
pub struct ProcessorMeta {
    /// processor name
    pub name: String,

    /// output root directory
    pub output_dir: String,
}
