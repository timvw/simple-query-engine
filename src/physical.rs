use std::sync::Arc;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use crate::datasource::DataSource;
use crate::RecordBatch;

pub trait PhyiscalPlan {
    fn schema(&self) -> Schema;
    fn execute(&self) -> Vec<RecordBatch>;
}

pub trait PhysicalExpression {
    fn evalute(&self, input: RecordBatch) -> &Arc<dyn Array>;
}

pub struct ColumnExpression {
    pub idx: usize,
}

impl PhysicalExpression for ColumnExpression {
    fn evalute(&self, input: RecordBatch) -> &Arc<dyn Array> {
        //input.columns()[self.idx]
        todo!();
    }
}

pub struct ScanExec {
    datasource: Box<dyn DataSource>,
    projection: Vec<String>,
}

impl PhyiscalPlan for ScanExec {
    fn schema(&self) -> Schema {
        let retained: Vec<Field> = self.datasource.schema().fields.into_iter().filter(|f|self.projection.contains(&f.name)).collect();
        Schema::from(retained)
    }

    fn execute(&self) -> Vec<RecordBatch> {
        self.datasource.scan(self.projection.clone())
    }
}

