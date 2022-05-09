use crate::logical::expression::LogicalExpressionCapabilities;
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::{DataType, Field};

pub struct Literal {
    pub value: String,
}

impl LogicalExpressionCapabilities for Literal {
    fn to_field(&self, _input: &LogicalPlan) -> Field {
        Field::new(self.value.clone(), DataType::Utf8, false)
    }
}
