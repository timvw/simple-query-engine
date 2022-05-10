use crate::physical::expression::column::Column;
use crate::physical::expression::add::Add;
use crate::physical::expression::literal::Literal;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;
use crate::datatypes::scalar::ScalarValue;

pub trait PhysicalExpressionCapabilities {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array>;
}

#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Column(Column),
    Literal(Literal),
    Add(Add),
}

impl PhysicalExpression {
    pub fn column(index: usize) -> PhysicalExpression {
        PhysicalExpression::Column(Column { index })
    }
    pub fn literal(name: String, value: ScalarValue) -> PhysicalExpression {
        PhysicalExpression::Literal(Literal { name, value })
    }
    pub fn add(expressions: Vec<PhysicalExpression>) -> PhysicalExpression {
        PhysicalExpression::Add(Add { expressions })
    }
}

impl PhysicalExpressionCapabilities for PhysicalExpression {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        match self {
            PhysicalExpression::Column(column) => column.evaluate(input),
            PhysicalExpression::Literal(literal) => literal.evaluate(input),
            PhysicalExpression::Add(add) => add.evaluate(input),
        }
    }
}

pub mod column;
pub mod add;
pub mod literal;
