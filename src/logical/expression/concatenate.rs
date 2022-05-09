use crate::logical::expression::{LogicalExpression, LogicalExpressionCapabilities};
use crate::logical::plan::LogicalPlan;
use arrow2::datatypes::Field;
use crate::logical::expression::column::Column;

#[derive(Debug, Clone)]
pub struct Concatenate {
    pub name: String,
    pub expressions: Vec<LogicalExpression>,
}

impl LogicalExpressionCapabilities for Concatenate {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        let fields = self.expressions.iter().map(|f|f.to_field(input)).collect::<Vec<Field>>();
        let data_type = &fields.clone().first().unwrap().data_type.clone();
        let is_nullable = fields.clone().iter().find(|f| f.is_nullable).map(|f|f.is_nullable).unwrap_or(false);
        Field::new(self.name.clone(), data_type.to_owned(), is_nullable)
    }

    fn extract_columns(&self) -> Vec<Column> {
        self.expressions.iter().flat_map(|e| e.extract_columns()).collect()
    }
}