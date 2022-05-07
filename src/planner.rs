use crate::logical::LogicalPlan;
use crate::physical::{PhyiscalPlan, ScanExec};

pub struct QueryPlanner {

}

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: LogicalPlan) -> PhyiscalPlan {
        match logical_plan {
            LogicalPlan::Scan(scan) => PhyiscalPlan::ScanExec(ScanExec{
                datasource: scan.datasource,
                projection: scan.projection,
            })
        }
    }
}