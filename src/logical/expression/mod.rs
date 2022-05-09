use crate::logical::expression::column::Column;
use crate::logical::expression::literal::Literal;
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::Field;

pub trait LogicalExpressionCapabilities {
    fn to_field(&self, input: &LogicalPlan) -> Field;
}

#[derive(Debug, Clone)]
pub enum LogicalExpression {
    Column(Column),
    Liteal(Literal),
}

impl LogicalExpression {
    pub fn column(name: String) -> LogicalExpression {
        LogicalExpression::Column(Column { name })
    }
    pub fn literal(value: String) -> LogicalExpression {
        LogicalExpression::Liteal(Literal { value })
    }
}

impl LogicalExpressionCapabilities for LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => column.to_field(input),
            LogicalExpression::Liteal(literal) => literal.to_field(input),
        }
    }
}

pub mod column;
pub mod literal;
