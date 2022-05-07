use arrow2::datatypes::Schema;

fn main() {
    println!("Hello, world!");
}

trait DataSource {
    fn schema(&self) -> Schema;
}

trait LogicalPlan {
    fn schema(&self) -> Schema;
}

trait PhyiscalPlan {
}

trait QueryPlanner {
    fn create_physical_plan(logical_plan: dyn LogicalPlan) -> dyn PhyiscalPlan;
}
