use crate::logical::expression::LogicalExpressionCapabilities;
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use arrow2::datatypes::Field;

pub struct Column {
    pub name: String,
}

impl LogicalExpressionCapabilities for Column {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        input
            .schema()
            .fields
            .iter()
            .find(|f| f.name == self.name)
            .unwrap()
            .clone()
    }
}
