use crate::logical::plan::LogicalPlan;

pub struct QueryOptimiser {}

impl QueryOptimiser {
    pub fn optimize(logical_plan: LogicalPlan) -> LogicalPlan {
        logical_plan
    }
}
