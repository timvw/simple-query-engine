use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use crate::physical::plan::projection::ProjectionExec;
use crate::physical::plan::scan::ScanExec;
use crate::physical::plan::PhyiscalPlan;

pub struct QueryPlanner {}

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: LogicalPlan) -> PhyiscalPlan {
        match logical_plan {
            LogicalPlan::Scan(scan) => PhyiscalPlan::ScanExec(ScanExec {
                datasource: scan.datasource,
                projection: scan.projection,
            }),
            LogicalPlan::Projection(projection) => {
                PhyiscalPlan::ProjectionExec(Box::new(ProjectionExec {
                    schema: projection.input.schema(),
                    input: Self::create_physical_plan(projection.input),
                    expr: vec![],
                }))
            }
        }
    }
}
