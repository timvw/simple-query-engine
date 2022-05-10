use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};

#[derive(Debug, Copy, Clone)]
pub struct QueryOptimiser {}

impl QueryOptimiser {
    pub fn optimize(logical_plan: LogicalPlan) -> LogicalPlan {
        pushdown_projection_columns_to_scan_field_names(logical_plan)
    }
}

fn pushdown_projection_columns_to_scan_field_names(logical_plan: LogicalPlan) -> LogicalPlan {
    match logical_plan {
        LogicalPlan::Projection(ref projection) => match &projection.input {
            LogicalPlan::Scan(scan) => {
                let field_names = projection
                    .extract_columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<String>>();
                let updated_scan_plan = LogicalPlan::scan(scan.datasource.clone(), field_names);
                LogicalPlan::projection(updated_scan_plan, projection.expressions.clone())
            }
            _ => logical_plan,
        },
        _ => logical_plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::DataSource;
    use crate::datatypes::scalar::ScalarValue;
    use crate::logical::expression::LogicalExpression;
    use crate::logical::plan::scan::Scan;
    use crate::Result;

    #[test]
    fn test_pushdown_projection_expressions_to_scan_field_names() -> Result<()> {
        let test_file = "./parquet-testing/data/alltypes_plain.parquet";
        let datasource = DataSource::parquet(test_file.to_string())?;
        let scan = Scan::all_columns(datasource);
        let logical_plan = LogicalPlan::projection(
            LogicalPlan::Scan(scan.clone()),
            vec![
                LogicalExpression::column("id".to_string()),
                LogicalExpression::literal("test".to_string(), ScalarValue::utf8("Mr Literalis")),
            ],
        );

        // initially 11 fields are scanned
        assert_eq!(scan.field_names.len(), 11);

        let optimized_plan = QueryOptimiser::optimize(logical_plan);

        match optimized_plan {
            LogicalPlan::Projection(ref projection) => {
                match &projection.input {
                    LogicalPlan::Scan(scan) => assert_eq!(scan.field_names.len(), 1), // only a single column should be scanned
                    _ => unreachable!(), // not expected
                }
            }
            _ => unreachable!(), // not expected
        }

        Ok(())
    }
}
