use crate::logical::LogicalPlan;

pub struct QueryOptimiser {}

impl QueryOptimiser {
    pub fn optimize(logical_plan: LogicalPlan) -> LogicalPlan {
        logical_plan
    }
}
