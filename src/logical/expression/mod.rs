use crate::logical::expression::column::Column;
use crate::logical::expression::literal::Literal;
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use arrow2::datatypes::{DataType, Field};

pub trait LogicalExpressionCapabilities {
    fn to_field(&self, input: &LogicalPlan) -> Field;
}

pub enum LogicalExpression {
    Column(Column),
    Liteal(Literal),
}

impl LogicalExpression {
    pub fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => input
                .schema()
                .fields
                .iter()
                .find(|f| f.name == column.name)
                .unwrap()
                .clone(),
            LogicalExpression::Liteal(literal) => {
                Field::new(literal.value.clone(), DataType::Utf8, false)
            }
        }
    }
}

pub mod column;
pub mod literal;
