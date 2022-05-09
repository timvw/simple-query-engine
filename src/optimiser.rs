use crate::logical::plan::LogicalPlan;

#[derive(Debug, Copy, Clone)]
pub struct QueryOptimiser {}

impl QueryOptimiser {
    pub fn optimize(logical_plan: LogicalPlan) -> LogicalPlan {
        logical_plan
    }
}
