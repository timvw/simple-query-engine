use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use crate::physical::plan::projection::Projection;
use crate::physical::plan::scan::Scan;
use crate::physical::plan::PhyiscalPlan;

pub struct QueryPlanner {}

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: LogicalPlan) -> PhyiscalPlan {
        match logical_plan {
            LogicalPlan::Scan(scan) => PhyiscalPlan::Scan(Scan {
                datasource: scan.datasource,
                projection: scan.projection,
            }),
            LogicalPlan::Projection(projection) => PhyiscalPlan::Projection(Box::new(Projection {
                schema: projection.input.schema(),
                input: Self::create_physical_plan(projection.input),
                expr: vec![],
            })),
        }
    }
}
