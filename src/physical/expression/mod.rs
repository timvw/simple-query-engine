use crate::physical::expression::column::Column;
use crate::physical::expression::literal::Literal;
use crate::RecordBatch;
use arrow2::array::Array;
use std::sync::Arc;

trait PhysicalExpressionCapabilities {
    fn evaluate(&self, input: RecordBatch) -> &Arc<dyn Array>;
}

#[derive(Debug, Clone)]
pub enum PhysicalExpression {
    Column(Column),
    Literal(Literal),
}

impl PhysicalExpression {
    pub fn column(index: usize) -> PhysicalExpression {
        PhysicalExpression::Column(Column { index })
    }
    pub fn literal(name: String, value: String) -> PhysicalExpression {
        PhysicalExpression::Literal(Literal { name, value })
    }
}

impl PhysicalExpressionCapabilities for PhysicalExpression {
    fn evaluate(&self, input: RecordBatch) -> &Arc<dyn Array> {
        match self {
            PhysicalExpression::Column(column) => column.evaluate(input),
            PhysicalExpression::Literal(literal) => literal.evaluate(input),
        }
    }
}

pub mod column;
pub mod literal;
