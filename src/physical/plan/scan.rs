use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::physical::plan::PhysicalPlanCapabilities;
use crate::{schema_projected, RecordBatchStream};
use arrow2::datatypes::Schema;

pub struct ScanExec {
    pub datasource: DataSource,
    pub projection: Vec<String>,
}

impl PhysicalPlanCapabilities for ScanExec {
    fn schema(&self) -> Schema {
        schema_projected(self.datasource.schema(), self.projection.clone())
    }
    fn execute(&self) -> RecordBatchStream {
        self.datasource.scan(Some(self.projection.clone()))
    }
}
