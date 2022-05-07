use crate::logical::LogicalPlan;

pub struct QueryOptimiser {
}

impl QueryOptimiser {
    pub fn optimize(logical_plan: &dyn LogicalPlan) -> &dyn LogicalPlan {
        logical_plan
    }
}