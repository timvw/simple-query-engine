use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use crate::physical::plan::PhyiscalPlan;

#[derive(Debug, Copy, Clone)]
pub struct QueryPlanner {}

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: LogicalPlan) -> PhyiscalPlan {
        match logical_plan {
            LogicalPlan::Scan(scan) => PhyiscalPlan::scan(scan.datasource, scan.field_names),
            LogicalPlan::Projection(projection) => {
                let schema = projection.input.schema();
                PhyiscalPlan::projection(
                    Self::create_physical_plan(projection.input),
                    schema,
                    vec![],
                )
            }
        }
    }
}
