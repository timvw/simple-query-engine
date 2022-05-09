use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::physical::plan::PhysicalPlanCapabilities;
use crate::{schema_projected, RecordBatchStream};
use arrow2::datatypes::Schema;

pub struct Scan {
    pub datasource: DataSource,
    pub projection: Vec<String>,
}

impl PhysicalPlanCapabilities for Scan {
    fn schema(&self) -> Schema {
        schema_projected(self.datasource.schema(), self.projection.clone())
    }
    fn execute(&self) -> RecordBatchStream {
        self.datasource.scan(Some(self.projection.clone()))
    }
}
