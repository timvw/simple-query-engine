use crate::logical::plan::projection::Projection;
use crate::logical::plan::scan::Scan;
use arrow2::datatypes::Schema;

pub trait LogicalPlanCapabilities {
    /// Returns the schema of this plan
    fn schema(&self) -> Schema;

    /// Returns the plans on which this plan depends
    fn children(&self) -> Vec<&LogicalPlan>;
}

pub enum LogicalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl LogicalPlanCapabilities for LogicalPlan {
    fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => scan.schema(),
            LogicalPlan::Projection(projection) => projection.schema(),
        }
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(scan) => scan.children(),
            LogicalPlan::Projection(projection) => projection.children(),
        }
    }
}

pub mod projection;
pub mod scan;
