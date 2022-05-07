use crate::logical::LogicalPlan;
use crate::physical::PhyiscalPlan;

pub struct QueryPlanner {

}

impl QueryPlanner {
    pub fn create_physical_plan(_logical_plan: &dyn LogicalPlan) -> Box<dyn PhyiscalPlan> {
        todo!();
    }
}