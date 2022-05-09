use crate::logical::expression::LogicalExpressionCapabilities;
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::{DataType, Field};

#[derive(Debug, Clone)]
pub struct Literal {
    pub name: String,
    pub value: String,
}

impl LogicalExpressionCapabilities for Literal {
    fn to_field(&self, _input: &LogicalPlan) -> Field {
        Field::new(self.name.clone(), DataType::Utf8, false)
    }
}
