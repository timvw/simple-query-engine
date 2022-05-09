use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::physical::plan::PhysicalPlanCapabilities;
use crate::{schema_projected, RecordBatchStream};
use arrow2::datatypes::Schema;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Scan {
    pub datasource: DataSource,
    pub field_names: Vec<String>,
}

#[async_trait]
impl PhysicalPlanCapabilities for Scan {
    fn schema(&self) -> Schema {
        schema_projected(self.datasource.schema(), self.field_names.clone())
    }
    async fn execute(&self) -> RecordBatchStream {
        self.datasource.scan(Some(self.field_names.clone()))
    }
}
