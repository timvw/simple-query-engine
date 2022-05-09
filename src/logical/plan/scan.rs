use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use crate::schema_projected;
use arrow2::datatypes::Schema;

#[derive(Debug, Clone)]
pub struct Scan {
    pub datasource: DataSource,
    pub field_names: Vec<String>,
}

impl Scan {
    pub fn some_columns(datasource: DataSource, projection: Vec<String>) -> Scan {
        Scan {
            datasource,
            field_names: projection,
        }
    }
    pub fn all_columns(datasource: DataSource) -> Scan {
        let ds_schema = datasource.schema();
        let projection = ds_schema.fields.iter().map(|f| f.name.clone()).collect();
        Scan {
            datasource,
            field_names: projection,
        }
    }
}

impl LogicalPlanCapabilities for Scan {
    fn schema(&self) -> Schema {
        schema_projected(self.datasource.schema(), self.field_names.to_vec())
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
}
