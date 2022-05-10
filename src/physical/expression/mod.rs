use crate::physical::expression::column::Column;
use crate::physical::expression::add::Add;
use crate::physical::expression::literal::Utf8Literal;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

pub trait PhysicalExpressionCapabilities {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array>;
}

#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Column(Column),
    Utf8Literal(Utf8Literal),
    Add(Add),
}

impl PhysicalExpression {
    pub fn column(index: usize) -> PhysicalExpression {
        PhysicalExpression::Column(Column { index })
    }
    pub fn utf8_literal(name: String, value: String) -> PhysicalExpression {
        PhysicalExpression::Utf8Literal(Utf8Literal { name, value })
    }
    pub fn add(expressions: Vec<PhysicalExpression>) -> PhysicalExpression {
        PhysicalExpression::Add(Add { expressions })
    }
}

impl PhysicalExpressionCapabilities for PhysicalExpression {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        match self {
            PhysicalExpression::Column(column) => column.evaluate(input),
            PhysicalExpression::Utf8Literal(literal) => literal.evaluate(input),
            PhysicalExpression::Add(add) => add.evaluate(input),
        }
    }
}

pub mod column;
pub mod add;
pub mod literal;
