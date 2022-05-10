use crate::logical::expression::column::Column;
use crate::logical::expression::LogicalExpressionCapabilities;
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::Field;
use crate::datatypes::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct Literal {
    pub name: String,
    pub value: ScalarValue,
}

impl LogicalExpressionCapabilities for Literal {
    fn to_field(&self, _input: &LogicalPlan) -> Field {
        Field::new(self.name.clone(), self.value.data_type(), self.value.is_null())
    }

    fn extract_columns(&self) -> Vec<Column> {
        vec![]
    }
}
