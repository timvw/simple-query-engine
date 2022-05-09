use crate::datasource::DataSource;
use crate::physical::expression::PhysicalExpression;
use crate::physical::plan::projection::Projection;
use crate::physical::plan::scan::Scan;
use crate::RecordBatchStream;
use arrow2::datatypes::Schema;
use std::fmt;

pub trait PhysicalPlanCapabilities {
    fn schema(&self) -> Schema;
    fn execute(&self) -> RecordBatchStream;
}

#[derive(Debug, Clone)]
pub enum PhyiscalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl PhyiscalPlan {
    pub fn scan(datasource: DataSource, field_names: Vec<String>) -> PhyiscalPlan {
        PhyiscalPlan::Scan(Scan {
            datasource,
            field_names,
        })
    }
    pub fn projection(
        input: PhyiscalPlan,
        schema: Schema,
        expressions: Vec<PhysicalExpression>,
    ) -> PhyiscalPlan {
        PhyiscalPlan::Projection(Box::new(Projection {
            input,
            schema,
            expressions,
        }))
    }

    pub fn fmt_indent(&self, indent: usize) -> String {
        match self {
            PhyiscalPlan::Scan(scan) => format!(
                "{:indent$}Scan: {}; field_names={:?}",
                "", scan.datasource, scan.field_names
            ),
            PhyiscalPlan::Projection(projection) => format!(
                "{:indent$}Projection: {:?}\n{}",
                "",
                projection.expressions,
                projection.input.fmt_indent(indent + 1)
            ),
        }
    }
}

impl PhysicalPlanCapabilities for PhyiscalPlan {
    fn schema(&self) -> Schema {
        match self {
            PhyiscalPlan::Scan(scan) => scan.schema(),
            PhyiscalPlan::Projection(projection) => projection.schema(),
        }
    }
    fn execute(&self) -> RecordBatchStream {
        match self {
            // does a scan need a projection?
            PhyiscalPlan::Scan(scan) => scan.execute(),
            PhyiscalPlan::Projection(projection) => projection.execute(),
        }
    }
}

impl fmt::Display for PhyiscalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.fmt_indent(0))
    }
}

pub mod projection;
pub mod scan;
