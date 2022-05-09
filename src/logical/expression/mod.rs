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
    Literal(Literal),
}

impl LogicalExpression {
    pub fn column(name: String) -> LogicalExpression {
        LogicalExpression::Column(Column { name })
    }
    pub fn literal(name: String, value: String) -> LogicalExpression {
        LogicalExpression::Literal(Literal { name, value })
    }
}

impl LogicalExpressionCapabilities for LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => column.to_field(input),
            LogicalExpression::Literal(literal) => literal.to_field(input),
        }
    }
}

pub mod column;
pub mod literal;
