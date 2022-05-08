use std::sync::Arc;
use arrow2::array::Array;
use arrow2::datatypes::Schema;
use crate::datasource::DataSource;
use crate::{RecordBatch, RecordBatchStream, schema_projected};

pub enum  PhyiscalPlan {
    ScanExec(ScanExec),
    ProjectionExec(Box<ProjectionExec>),
}

impl PhyiscalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            PhyiscalPlan::ScanExec(scan) => schema_projected(scan.datasource.schema(), scan.projection.clone()),
            PhyiscalPlan::ProjectionExec(projection) => projection.schema.clone(),
        }
    }
    pub fn execute(&self) -> RecordBatchStream {
        match self {
            PhyiscalPlan::ScanExec(scan) => scan.datasource.scan(scan.projection.clone()),
            PhyiscalPlan::ProjectionExec(projection) => {
                let mut rbs = projection.input.execute();
                // TODO implement evalution of all expressions..
                rbs
            }
        }
    }
}

pub struct ScanExec {
    pub datasource: Box<dyn DataSource>,
    pub projection: Vec<String>,
}

pub struct ProjectionExec {
    pub input: PhyiscalPlan,
    pub schema: Schema,
    pub expr: Vec<Box<dyn PhysicalExpression>>,
}

pub trait PhysicalExpression {
    fn evalute(&self, input: RecordBatch) -> &Arc<dyn Array>;
}

pub struct ColumnExpression {
    pub idx: usize,
}

impl PhysicalExpression for ColumnExpression {
    fn evalute(&self, _input: RecordBatch) -> &Arc<dyn Array> {
        //input.columns()[self.idx]
        todo!();
    }
}

