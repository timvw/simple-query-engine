use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::physical::plan::PhysicalPlanCapabilities;
use crate::{schema_projected, RecordBatchStream};
use arrow2::datatypes::Schema;

pub struct Scan {
    pub datasource: DataSource,
    pub field_names: Vec<String>,
}

impl PhysicalPlanCapabilities for Scan {
    fn schema(&self) -> Schema {
        schema_projected(self.datasource.schema(), self.field_names.clone())
    }
    fn execute(&self) -> RecordBatchStream {
        self.datasource.scan(Some(self.field_names.clone()))
    }
}
