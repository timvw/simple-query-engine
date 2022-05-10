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
        println!("i see {} rows and {} colums, each of len {}", input.len(), input.columns().len(), input.columns().get(0).unwrap().len());
        // so we start with the first array.. and then invoke add for each next expression...
        let mut columns = Vec::new();
        for e in self.expressions.clone() {
            let column = e.evaluate(input.clone());
            println!("evaluating a column of len: {}", column.len());
            columns.push(column);
        }
        //let arrays = columns.iter().map(|c| c.deref()).collect::<Vec<_>>();
        let result_array = arrow2::compute::arithmetics::add(columns.get(0).unwrap().deref(), columns.get(1).unwrap().deref());
            //arrow2::compute::concatenate::concatenate(&arrays).unwrap();
        println!("the result array is of len: {}", result_array.len());
        Arc::from(result_array)
    }
}
