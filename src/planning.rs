use crate::logical::LogicalPlan;
use crate::physical::PhyiscalPlan;

pub struct Planner {

}

impl Planner {
    fn create_physical_plan(_logical_plan: Box<dyn LogicalPlan>) -> Box<dyn PhyiscalPlan> {
        todo!();
    }
}