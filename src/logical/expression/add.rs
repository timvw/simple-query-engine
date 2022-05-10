use crate::logical::expression::column::Column;
use crate::logical::expression::{LogicalExpression, LogicalExpressionCapabilities};
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::Field;

#[derive(Debug, Clone)]
pub struct Add {
    pub name: String,
    pub expressions: Vec<LogicalExpression>,
}

impl LogicalExpressionCapabilities for Add {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        let fields = self
            .expressions
            .iter()
            .map(|f| f.to_field(input))
            .collect::<Vec<Field>>();
        let data_type = &fields.first().unwrap().data_type.clone();
        let is_nullable = fields
            .iter()
            .find(|f| f.is_nullable)
            .map(|f| f.is_nullable)
            .unwrap_or(false);
        Field::new(self.name.clone(), data_type.to_owned(), is_nullable)
    }

    fn extract_columns(&self) -> Vec<Column> {
        self.expressions
            .iter()
            .flat_map(|e| e.extract_columns())
            .collect()
    }
}
