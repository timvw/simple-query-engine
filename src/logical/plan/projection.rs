use crate::logical::expression::LogicalExpression;
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use arrow2::datatypes::Schema;

pub struct Projection {
    pub input: LogicalPlan,
    pub expr: Vec<LogicalExpression>,
}

impl LogicalPlanCapabilities for Projection {
    fn schema(&self) -> Schema {
        Schema::from(
            self.expr
                .iter()
                .map(|x| x.to_field(&self.input))
                .collect::<Vec<_>>(),
        )
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
}
