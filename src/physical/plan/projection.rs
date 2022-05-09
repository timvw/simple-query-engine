use crate::physical::expression::PhysicalExpression;
use crate::physical::plan::{PhyiscalPlan, PhysicalPlanCapabilities};
use crate::RecordBatchStream;
use arrow2::datatypes::Schema;

#[derive(Debug, Clone)]
pub struct Projection {
    pub input: PhyiscalPlan,
    pub schema: Schema,
    pub expressions: Vec<PhysicalExpression>,
}

impl PhysicalPlanCapabilities for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
    fn execute(&self) -> RecordBatchStream {
        // TODO implement evalution of all expressions..
        self.input.execute()
    }
}
