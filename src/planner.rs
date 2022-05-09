use crate::logical::expression::LogicalExpression;
use crate::logical::plan::{LogicalPlan, LogicalPlanCapabilities};
use crate::physical::expression::PhysicalExpression;
use crate::physical::plan::PhyiscalPlan;

#[derive(Debug, Copy, Clone)]
pub struct QueryPlanner {}

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: LogicalPlan) -> PhyiscalPlan {
        match logical_plan {
            LogicalPlan::Scan(scan) => PhyiscalPlan::scan(scan.datasource, scan.field_names),
            LogicalPlan::Projection(projection) => {
                let schema = projection.input.schema();
                let expressions = projection
                    .expressions
                    .iter()
                    .map(|e| Self::create_physical_expression(e, &projection.input))
                    .collect::<Vec<PhysicalExpression>>();
                PhyiscalPlan::projection(
                    Self::create_physical_plan(projection.input),
                    schema,
                    expressions,
                )
            }
        }
    }

    pub fn create_physical_expression(
        logical_expression: &LogicalExpression,
        input: &LogicalPlan,
    ) -> PhysicalExpression {
        match logical_expression {
            LogicalExpression::Column(column) => {
                let input_schema = input.schema();
                let column_index = input_schema
                    .fields
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name.eq(&column.name))
                    .map(|(index, _)| index)
                    .unwrap();
                PhysicalExpression::column(column_index)
            }
            LogicalExpression::Literal(literal) => {
                PhysicalExpression::literal(literal.name.clone(), literal.value.clone())
            }
        }
    }
}
