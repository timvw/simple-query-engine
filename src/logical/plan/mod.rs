use crate::datasource::DataSource;
use crate::logical::expression::LogicalExpression;
use crate::logical::plan::projection::Projection;
use crate::logical::plan::scan::Scan;
use arrow2::datatypes::Schema;

pub trait LogicalPlanCapabilities {
    /// Returns the schema of this plan
    fn schema(&self) -> Schema;

    /// Returns the plans on which this plan depends
    fn children(&self) -> Vec<&LogicalPlan>;
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl LogicalPlan {
    pub fn scan(datasource: DataSource, projection: Vec<String>) -> LogicalPlan {
        LogicalPlan::Scan(Scan::some_columns(datasource, projection))
    }
    pub fn scan_all_columns(datasource: DataSource) -> LogicalPlan {
        LogicalPlan::Scan(Scan::all_columns(datasource))
    }
    pub fn projection(input: LogicalPlan, expressions: Vec<LogicalExpression>) -> LogicalPlan {
        LogicalPlan::Projection(Box::new(Projection { input, expressions }))
    }
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
