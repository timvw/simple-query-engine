use arrow2::datatypes::{DataType, Field, Schema};
use crate::datasource::DataSource;

trait LogicalPlan {
    fn schema(&self) -> Schema;
    fn children(&self) -> Vec<Box<dyn LogicalPlan>>;
}

struct Scan {
    datasource: Box<dyn DataSource>,
    projection: Vec<String>,
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Schema {
        if self.projection.is_empty() {
            self.datasource.schema()
        } else {
            let retained: Vec<Field> = self.datasource.schema().fields.into_iter().filter(|f|self.projection.contains(&f.name)).collect();
            Schema::from(retained)
        }
    }

    fn children(&self) -> Vec<Box<dyn LogicalPlan>> {
        vec![]
    }
}

trait LogicalExpression {
    fn to_field(&self, input: Box<dyn LogicalPlan>) -> Field;
}

struct Column {
    name: String,
}

impl LogicalExpression for Column {
    fn to_field(&self, input: Box<dyn LogicalPlan>) -> Field {
        input.schema().fields.iter().find(|f| f.name == self.name).unwrap().clone()
    }
}

struct Literal {
    value: String,
}

impl LogicalExpression for Literal {
    fn to_field(&self, _input: Box<dyn LogicalPlan>) -> Field {
        Field::new(&self.value, DataType::Utf8, false)
    }
}