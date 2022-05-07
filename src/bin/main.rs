use simply_query_engine::error::*;
use simply_query_engine::datasource::parquet::*;
use simply_query_engine::logical::*;
use simply_query_engine::optimiser::QueryOptimiser;
use simply_query_engine::planner::QueryPlanner;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {

    let test_file = "/Users/timvw/src/github/simply-query-engine/test-data/alltypes_plain.parquet";
    let datasource = ParquetDataSource::new(test_file.to_string())?;
    let logical_plan = LogicalPlan::Scan(Scan::new(Box::new(datasource), vec![]));
    print!("{:?}", logical_plan.schema());

    let optimized_plan = QueryOptimiser::optimize(logical_plan);

    let phyiscal_plan = QueryPlanner::create_physical_plan(optimized_plan);

    let mut result = phyiscal_plan.execute();
    for item in result.next().await {
        println!("here is a batch: {:?}", item);
    }
    Ok(())
}