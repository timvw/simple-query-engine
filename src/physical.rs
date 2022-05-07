use arrow2::datatypes::Schema;
use crate::RecordBatch;

pub trait PhyiscalPlan {
    fn schema(&self) -> Schema;
    fn execute(&self) -> Vec<RecordBatch>;
}