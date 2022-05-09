use crate::logical::expression::{LogicalExpression, LogicalExpressionCapabilities};
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use arrow2::datatypes::Schema;
use crate::logical::expression::column::Column;

#[derive(Debug, Clone)]
pub struct Projection {
    pub input: LogicalPlan,
    pub expressions: Vec<LogicalExpression>,
}

impl LogicalPlanCapabilities for Projection {
    fn schema(&self) -> Schema {
        Schema::from(
            self.expressions
                .iter()
                .map(|x| x.to_field(&self.input))
                .collect::<Vec<_>>(),
        )
    }

    fn extract_columns(&self) -> Vec<Column> {
        self.expressions.iter().filter_map(|x| match x {
            LogicalExpression::Column(column) => Some(column.clone()),
            LogicalExpression::Literal(_) => None,
        }).collect::<Vec<_>>()
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
}
