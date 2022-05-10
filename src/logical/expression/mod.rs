use crate::datatypes::scalar::ScalarValue;
use crate::logical::expression::add::Add;
use crate::logical::expression::column::Column;
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
    Add(Add),
}

impl LogicalExpression {
    pub fn column(name: String) -> LogicalExpression {
        LogicalExpression::Column(Column { name })
    }
    pub fn literal(name: String, value: ScalarValue) -> LogicalExpression {
        LogicalExpression::Literal(Literal { name, value })
    }
    pub fn add(name: String, expressions: Vec<LogicalExpression>) -> LogicalExpression {
        LogicalExpression::Add(Add { name, expressions })
    }
}

impl LogicalExpressionCapabilities for LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => column.to_field(input),
            LogicalExpression::Literal(literal) => literal.to_field(input),
            LogicalExpression::Add(add) => add.to_field(input),
        }
    }

    fn extract_columns(&self) -> Vec<Column> {
        match self {
            LogicalExpression::Column(column) => column.extract_columns(),
            LogicalExpression::Literal(literal) => literal.extract_columns(),
            LogicalExpression::Add(add) => add.extract_columns(),
        }
    }
}

pub mod add;
pub mod column;
pub mod literal;
