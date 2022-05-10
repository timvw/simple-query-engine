use crate::datasource::DataSource;
use crate::physical::expression::PhysicalExpression;
use crate::physical::plan::projection::Projection;
use crate::physical::plan::scan::Scan;
use crate::RecordBatchStream;
use arrow2::datatypes::Schema;
use async_trait::async_trait;
use std::fmt;

#[async_trait]
pub trait PhysicalPlanCapabilities {
    fn schema(&self) -> Schema;
    async fn execute(&self) -> RecordBatchStream;
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

#[async_trait]
impl PhysicalPlanCapabilities for PhyiscalPlan {
    fn schema(&self) -> Schema {
        match self {
            PhyiscalPlan::Scan(scan) => scan.schema(),
            PhyiscalPlan::Projection(projection) => projection.schema(),
        }
    }
    async fn execute(&self) -> RecordBatchStream {
        match self {
            // does a scan need a projection?
            PhyiscalPlan::Scan(scan) => scan.execute().await,
            PhyiscalPlan::Projection(projection) => projection.execute().await,
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

#[cfg(test)]
mod tests {
    use crate::datasource::DataSource;
    use crate::datatypes::scalar::ScalarValue;
    use crate::logical::expression::LogicalExpression;
    use crate::logical::plan::LogicalPlan;
    use crate::optimiser::logical::QueryOptimiser;
    use crate::physical::plan::PhysicalPlanCapabilities;
    use crate::planner::QueryPlanner;
    use crate::util::test::parquet_test_data;
    use crate::Result;
    use futures::stream::StreamExt;

    #[tokio::test]
    async fn test_e2e() -> Result<()> {
        let test_file = format!("{}/alltypes_plain.parquet", parquet_test_data());
        let datasource = DataSource::parquet(test_file)?;

        let logical_plan = LogicalPlan::projection(
            LogicalPlan::scan_all_columns(datasource),
            vec![
                LogicalExpression::column("id".to_string()),
                LogicalExpression::literal("name".to_string(), ScalarValue::utf8("Mr. Literal")),
            ],
        );

        let optimized_plan = QueryOptimiser::optimize(logical_plan);
        let phyiscal_plan = QueryPlanner::create_physical_plan(optimized_plan);

        let schema = phyiscal_plan.schema();
        assert_eq!(schema.fields.len(), 2);

        let mut rbs = phyiscal_plan.execute().await;
        let mut counter = 0;
        while let Some(rrb) = rbs.next().await {
            let rb = rrb?;
            // expect 2 columns
            assert_eq!(rb.columns().len(), 2);
            counter += rb.len();
        }
        assert_eq!(counter, 8); // expect 8 rows

        Ok(())
    }
}
