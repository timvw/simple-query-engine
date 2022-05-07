use simply_query_engine::error::*;
use simply_query_engine::datasource::*;
use simply_query_engine::logical::*;
//use simply_query_engine::physical::*;


fn main() -> Result<()> {

    let test_file = "/Users/timvw/src/github/simply-query-engine/test-data/alltypes_plain.parquet";
    let datasource = ParquetDataSource::new(test_file.to_string())?;
    let scan = Scan::new(Box::new(datasource), vec![]);
    print!("{:?}", scan.schema());

    Ok(())
}



/*
trait QueryPlanner {
    fn create_physical_plan(logical_plan: dyn LogicalPlan) -> dyn PhyiscalPlan;
}*/
