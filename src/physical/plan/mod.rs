use crate::physical::plan::projection::Projection;
use crate::physical::plan::scan::Scan;
use crate::RecordBatchStream;
use arrow2::datatypes::Schema;

pub trait PhysicalPlanCapabilities {
    fn schema(&self) -> Schema;
    fn execute(&self) -> RecordBatchStream;
}

pub enum PhyiscalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl PhysicalPlanCapabilities for PhyiscalPlan {
    fn schema(&self) -> Schema {
        match self {
            PhyiscalPlan::Scan(scan) => scan.schema(),
            PhyiscalPlan::Projection(projection) => projection.schema(),
        }
    }
    fn execute(&self) -> RecordBatchStream {
        match self {
            // does a scan need a projection?
            PhyiscalPlan::Scan(scan) => scan.execute(),
            PhyiscalPlan::Projection(projection) => projection.execute(),
        }
    }
}

pub mod projection;
pub mod scan;
