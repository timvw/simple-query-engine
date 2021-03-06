use simply_query_engine::datasource::{DataSource, DataSourceCapabilities};
use simply_query_engine::datatypes::scalar::ScalarValue;
use simply_query_engine::error::*;
use simply_query_engine::logical::expression::LogicalExpression;
use simply_query_engine::logical::plan::LogicalPlan;
use simply_query_engine::optimiser::logical::QueryOptimiser;
use simply_query_engine::physical::plan::PhysicalPlanCapabilities;
use simply_query_engine::planner::QueryPlanner;
use simply_query_engine::pretty_print;

#[tokio::main]
async fn main() -> Result<()> {
    let test_file = "./parquet-testing/data/alltypes_plain.parquet";
    let datasource = DataSource::parquet(test_file.to_string())?;
    println!("schema of datasource: {:?}", datasource.schema());

    let logical_plan = LogicalPlan::projection(
        LogicalPlan::scan_all_columns(datasource),
        vec![
            LogicalExpression::column("id".to_string()),
            LogicalExpression::literal("name".to_string(), ScalarValue::utf8("Mr. Literal")),
            LogicalExpression::add(
                "add".to_string(),
                vec![
                    LogicalExpression::column("id".to_string()),
                    LogicalExpression::column("id".to_string()),
                    LogicalExpression::column("id".to_string()),
                    LogicalExpression::literal("".to_string(), ScalarValue::Int32(Some(200))),
                    //LogicalExpression::literal("".to_string(), ScalarValue::Int32(None)),
                ],
            ),
        ],
    );

    //println!("{:?}", logical_plan.schema());
    println!("{}", logical_plan);

    let optimized_plan = QueryOptimiser::optimize(logical_plan);
    println!("{}", optimized_plan);

    let phyiscal_plan = QueryPlanner::create_physical_plan(optimized_plan);
    println!("{}", phyiscal_plan);

    let result = phyiscal_plan.execute().await;
    let schema = phyiscal_plan.schema();
    println!("physical plan schema: {:?}", schema);
    pretty_print(result, schema).await;

    Ok(())
}
