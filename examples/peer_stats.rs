use ribeye::processors::PeerStatsProcessor;
use ribeye::{MessageProcessor, RibEye};

fn main() {
    tracing_subscriber::fmt().init();

    const RIB_URL: &str = "https://data.ris.ripe.net/rrc18/2023.08/bview.20230806.1600.gz";
    let processor = PeerStatsProcessor::new("riperis", "rrc18", RIB_URL, "test_peer_stats.bz2");
    let mut ribeye = RibEye::new();
    ribeye.add_processor(processor.to_boxed()).unwrap();
    ribeye.process_mrt_file(RIB_URL).unwrap();
}
