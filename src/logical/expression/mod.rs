use crate::logical::expression::column::Column;
use crate::logical::expression::concatenate::Concatenate;
use crate::logical::expression::literal::Literal;
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::Field;

pub trait LogicalExpressionCapabilities {
    fn to_field(&self, input: &LogicalPlan) -> Field;
    fn extract_columns(&self) -> Vec<Column>;
}

#[derive(Debug, Clone)]
pub enum LogicalExpression {
    Column(Column),
    Literal(Literal),
    Concatenate(Concatenate),
}

impl LogicalExpression {
    pub fn column(name: String) -> LogicalExpression {
        LogicalExpression::Column(Column { name })
    }
    pub fn literal(name: String, value: String) -> LogicalExpression {
        LogicalExpression::Literal(Literal { name, value })
    }
    pub fn contatenate(name: String, expressions: Vec<LogicalExpression>) -> LogicalExpression {
        LogicalExpression::Concatenate(Concatenate{name, expressions})
    }
}

impl LogicalExpressionCapabilities for LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => column.to_field(input),
            LogicalExpression::Literal(literal) => literal.to_field(input),
            LogicalExpression::Concatenate(concatenate) => concatenate.to_field(input),
        }
    }

    fn extract_columns(&self) -> Vec<Column> {
        match self {
            LogicalExpression::Column(column) => column.extract_columns(),
            LogicalExpression::Literal(literal) => literal.extract_columns(),
            LogicalExpression::Concatenate(concatenate) => concatenate.extract_columns(),
        }
    }
}

pub mod column;
pub mod concatenate;
pub mod literal;
