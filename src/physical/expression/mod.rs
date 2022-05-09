use crate::physical::expression::column::Column;
use crate::physical::expression::literal::Literal;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;
use crate::physical::expression::concatenate::Concatenate;

pub trait PhysicalExpressionCapabilities {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array>;
}

#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Column(Column),
    Literal(Literal),
    Concatenate(Concatenate),
}

impl PhysicalExpression {
    pub fn column(index: usize) -> PhysicalExpression {
        PhysicalExpression::Column(Column { index })
    }
    pub fn literal(name: String, value: String) -> PhysicalExpression {
        PhysicalExpression::Literal(Literal { name, value })
    }
    pub fn concatenate(expressions: Vec<PhysicalExpression>) -> PhysicalExpression {
        PhysicalExpression::Concatenate(Concatenate { expressions})
    }
}

impl PhysicalExpressionCapabilities for PhysicalExpression {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        match self {
            PhysicalExpression::Column(column) => column.evaluate(input),
            PhysicalExpression::Literal(literal) => literal.evaluate(input),
            PhysicalExpression::Concatenate(concatenate)=> concatenate.evaluate(input),
        }
    }
}

pub mod column;
pub mod literal;
pub mod concatenate;
