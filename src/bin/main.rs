use simply_query_engine::error::*;
use simply_query_engine::datasource::*;
use simply_query_engine::logical::*;
//use simply_query_engine::physical::*;


fn main() -> Result<()> {

    let datasource = ParquetDataSource::new("".to_string())?;
    let scan = Scan::new(Box::new(datasource), vec![]);

    Ok(())
}



/*
trait QueryPlanner {
    fn create_physical_plan(logical_plan: dyn LogicalPlan) -> dyn PhyiscalPlan;
}*/
