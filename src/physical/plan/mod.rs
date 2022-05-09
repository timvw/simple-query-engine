use crate::physical::plan::projection::ProjectionExec;
use crate::physical::plan::scan::ScanExec;
use crate::RecordBatchStream;
use arrow2::datatypes::Schema;

pub trait PhysicalPlanCapabilities {
    fn schema(&self) -> Schema;
    fn execute(&self) -> RecordBatchStream;
}

pub enum PhyiscalPlan {
    ScanExec(ScanExec),
    ProjectionExec(Box<ProjectionExec>),
}

impl PhysicalPlanCapabilities for PhyiscalPlan {
    fn schema(&self) -> Schema {
        match self {
            PhyiscalPlan::ScanExec(scan) => scan.schema(),
            PhyiscalPlan::ProjectionExec(projection) => projection.schema(),
        }
    }
    fn execute(&self) -> RecordBatchStream {
        match self {
            // does a scan need a projection?
            PhyiscalPlan::ScanExec(scan) => scan.execute(),
            PhyiscalPlan::ProjectionExec(projection) => projection.execute(),
        }
    }
}

pub mod projection;
pub mod scan;
