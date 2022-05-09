use crate::datasource::DataSource;
use crate::logical::expression::LogicalExpression;
use crate::logical::plan::projection::Projection;
use crate::logical::plan::scan::Scan;
use arrow2::datatypes::Schema;
use std::fmt;
use crate::logical::expression::column::Column;

pub trait LogicalPlanCapabilities {
    /// Returns the schema of this plan
    fn schema(&self) -> Schema;

    /// Returns the plans on which this plan depends
    fn children(&self) -> Vec<&LogicalPlan>;

    /// Returns the columns that are used in this plan
    fn extract_columns(&self) -> Vec<Column>;
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Box<Projection>),
}

impl LogicalPlan {
    pub fn scan(datasource: DataSource, projection: Vec<String>) -> LogicalPlan {
        LogicalPlan::Scan(Scan::some_columns(datasource, projection))
    }
    pub fn scan_all_columns(datasource: DataSource) -> LogicalPlan {
        LogicalPlan::Scan(Scan::all_columns(datasource))
    }
    pub fn projection(input: LogicalPlan, expressions: Vec<LogicalExpression>) -> LogicalPlan {
        LogicalPlan::Projection(Box::new(Projection { input, expressions }))
    }

    pub fn fmt_indent(&self, indent: usize) -> String {
        match self {
            LogicalPlan::Scan(scan) => format!(
                "{:indent$}Scan: {}; field_names={:?}",
                "", scan.datasource, scan.field_names
            ),
            LogicalPlan::Projection(projection) => format!(
                "{:indent$}Projection: {:?}\n{}",
                "",
                projection.expressions,
                projection.input.fmt_indent(indent + 1)
            ),
        }
    }
}

impl LogicalPlanCapabilities for LogicalPlan {
    fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => scan.schema(),
            LogicalPlan::Projection(projection) => projection.schema(),
        }
    }

    fn extract_columns(&self) -> Vec<Column> {
        match self {
            LogicalPlan::Scan(scan) => scan.extract_columns(),
            LogicalPlan::Projection(projection) => projection.extract_columns(),
        }
    }

    fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan(scan) => scan.children(),
            LogicalPlan::Projection(projection) => projection.children(),
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.fmt_indent(0))
    }
}

pub mod projection;
pub mod scan;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test::parquet_test_data;
    use crate::Result;
    use regex::Regex;

    #[test]
    fn test_format_scan() -> Result<()> {
        let test_file = format!("{}/alltypes_plain.parquet", parquet_test_data());
        let datasource = DataSource::parquet(test_file)?;

        let expected_re = Regex::new(r"Scan: Parquet(.*?); field_names=(.*?)").unwrap();

        let scan_all_columns = LogicalPlan::scan_all_columns(datasource.clone());
        println!("{}", scan_all_columns);
        assert!(expected_re.is_match(&format!("{}", scan_all_columns)));

        let scan_some_columns = LogicalPlan::scan(datasource, vec!["id".to_string()]);
        println!("{}", scan_some_columns);
        assert!(expected_re.is_match(&format!("{}", scan_some_columns)));

        Ok(())
    }

    #[test]
    fn test_format_projection() -> Result<()> {
        let test_file = format!("{}/alltypes_plain.parquet", parquet_test_data());
        let datasource = DataSource::parquet(test_file)?;
        let scan_all_columns = LogicalPlan::scan_all_columns(datasource);
        let projection = LogicalPlan::projection(
            scan_all_columns,
            vec![LogicalExpression::column("id".to_string())],
        );

        let expected_re = Regex::new(r"Projection: (.*?)\n\s+Scan").unwrap();

        println!("{}", projection);
        assert!(expected_re.is_match(&format!("{}", projection)));

        Ok(())
    }
}
