use crate::datasource::{DataSource, DataSourceCapabilities};
use crate::schema_projected;
use arrow2::datatypes::{DataType, Field, Schema};

pub enum LogicalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl LogicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => {
                schema_projected(scan.datasource.schema(), scan.projection.to_vec())
            }
            LogicalPlan::Projection(projection) => Schema::from(
                projection
                    .expr
                    .iter()
                    .map(|x| x.to_field(&projection.input))
                    .collect::<Vec<_>>(),
            ),
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
    pub datasource: DataSource,
    pub projection: Vec<String>,
}

impl Scan {
    pub fn some_columns(datasource: DataSource, projection: Vec<String>) -> Scan {
        Scan {
            datasource,
            projection,
        }
    }
    pub fn all_columns(datasource: DataSource) -> Scan {
        let ds_schema = datasource.schema();
        let projection = ds_schema.fields.iter().map(|f| f.name.clone()).collect();
        Scan {
            datasource,
            projection,
        }
    }
}

pub struct Projection {
    pub input: LogicalPlan,
    pub expr: Vec<LogicalExpression>,
}

pub enum LogicalExpression {
    Column(Column),
    Liteal(Literal),
}

impl LogicalExpression {
    fn to_field(&self, input: &LogicalPlan) -> Field {
        match self {
            LogicalExpression::Column(column) => input
                .schema()
                .fields
                .iter()
                .find(|f| f.name == column.name)
                .unwrap()
                .clone(),
            LogicalExpression::Liteal(literal) => {
                Field::new(literal.value.clone(), DataType::Utf8, false)
            }
        }
    }
}

pub struct Column {
    pub name: String,
}

pub struct Literal {
    pub value: String,
}
