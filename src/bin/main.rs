use simply_query_engine::datasource::parquet::*;
use simply_query_engine::error::*;
use simply_query_engine::logical::*;
use simply_query_engine::optimiser::QueryOptimiser;
use simply_query_engine::planner::QueryPlanner;
use simply_query_engine::pretty_print;

#[tokio::main]
async fn main() -> Result<()> {
    let test_file = "./parquet-testing/data/alltypes_plain.parquet";
    let datasource = ParquetDataSource::new(test_file.to_string())?;
    let logical_plan = LogicalPlan::Scan(Scan::all_columns(Box::new(datasource)));
    print!("{:?}", logical_plan.schema());

    let optimized_plan = QueryOptimiser::optimize(logical_plan);

    let phyiscal_plan = QueryPlanner::create_physical_plan(optimized_plan);

    let result = phyiscal_plan.execute();
    let schema = phyiscal_plan.schema();
    pretty_print(result, schema).await;

    Ok(())
}
