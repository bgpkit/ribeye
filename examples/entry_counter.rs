use anyhow::Result;
use bgpkit_parser::models::ElemType;
use bgpkit_parser::BgpElem;
use ribeye::{MessageProcessor, RibEye, RibMeta};

#[derive(Default)]
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

impl MessageProcessor for EntryCounter {
    fn name(&self) -> String {
        "basic_counter".to_string()
    }

    fn output_path(&self) -> Option<String> {
        todo!()
    }

    fn reset_processor(&mut self, rib_meta: &RibMeta) {
        todo!()
    }

    fn process_entry(&mut self, elem: &BgpElem) -> Result<()> {
        match elem.elem_type {
            ElemType::ANNOUNCE => {
                self.a_count += 1;
            }
            ElemType::WITHDRAW => {
                self.w_count += 1;
            }
        }
        Ok(())
    }

    fn output(&mut self) -> Result<()> {
        println!(
            "{}: {} announcements, {} withdrawals",
            self.name(),
            self.a_count,
            self.w_count
        );
        Ok(())
    }
}

fn main() {
    let mut ribeye = RibEye::new();
    ribeye
        .add_processor(EntryCounter::new().to_boxed())
        .unwrap();
    ribeye
        .process_mrt_file("https://data.ris.ripe.net/rrc21/2023.08/updates.20230806.1640.gz")
        .unwrap();
}
