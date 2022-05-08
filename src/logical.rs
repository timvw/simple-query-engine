use arrow2::datatypes::{DataType, Field, Schema};
use crate::datasource::DataSource;
use crate::schema_projected;

pub enum LogicalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl LogicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => schema_projected(scan.datasource.schema(), scan.projection.clone()),
            LogicalPlan::Projection(projection) => Schema::from(projection.expr.iter().map(|x|x.to_field(&projection.input)).collect::<Vec<_>>()),
        }
    }

    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(_scan) => vec![],
            LogicalPlan::Projection(projection) => vec![&projection.input],
        }
    }
}

pub struct Scan {
    pub datasource: Box<dyn DataSource>,
    pub projection: Vec<String>,
}

impl Scan {
    pub fn new(datasource: Box<dyn DataSource>, projection: Vec<String>) -> Scan {
        Scan {
            datasource,
            projection,
        }
    }
}

pub struct Projection {
    pub input: LogicalPlan,
    pub expr: Vec<Box<dyn LogicalExpression>>,
}

pub trait LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field;
}

struct Column {
    name: String,
}

impl LogicalExpression for Column {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        input.schema().fields.iter().find(|f| f.name == self.name).unwrap().clone()
    }
}

struct Literal {
    value: String,
}

impl LogicalExpression for Literal {
    fn to_field(&self, _input: &LogicalPlan) -> Field {
        Field::new(&self.value, DataType::Utf8, false)
    }
}