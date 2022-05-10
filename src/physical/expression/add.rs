use crate::physical::expression::{PhysicalExpression, PhysicalExpressionCapabilities};
use crate::RecordBatch;
use arrow2::array::Array;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Add {
    pub expressions: Vec<PhysicalExpression>,
}

impl PhysicalExpressionCapabilities for Add {
    fn evaluate(&self, input: RecordBatch) -> Arc<dyn Array> {
        println!(
            "i see {} rows and {} colums, each of len {}",
            input.len(),
            input.columns().len(),
            input.columns().get(0).unwrap().len()
        );
        // so we start with the first array.. and then invoke add for each next expression...
        let mut evaluated_expression_columns = Vec::new();
        for e in &self.expressions {
            let evaluated_expression_column = e.evaluate(input.clone());
            evaluated_expression_columns.push(evaluated_expression_column);
        }

        let mut result_column = evaluated_expression_columns.clone().get(0).unwrap().clone();
        for c in &evaluated_expression_columns[1..] {
            result_column = Arc::from(arrow2::compute::arithmetics::add(
                result_column.deref(),
                c.deref(),
            ));
        }

        result_column
    }
}
