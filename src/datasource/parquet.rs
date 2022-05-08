use crate::datasource::DataSource;
use crate::error::Result;
use crate::RecordBatchStream;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::*;
use async_stream::stream;
use std::fs::File;

pub struct ParquetDataSource {
    file_path: String,
}

impl ParquetDataSource {
    pub fn new(file_path: String) -> Result<ParquetDataSource> {
        let file = File::open(file_path.clone())?;
        let _ = FileReader::try_new(file, None, None, None, None)?;
        Ok(ParquetDataSource { file_path })
    }

    fn get_reader(&self) -> Result<FileReader<File>> {
        let file = File::open(self.file_path.clone())?;
        let reader = FileReader::try_new(file, None, None, None, None)?;
        Ok(reader)
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Schema {
        let reader = self.get_reader().unwrap();
        reader.schema().clone()
    }

    fn scan(&self, projection: Vec<String>) -> RecordBatchStream {
        let reader = self.get_reader().unwrap();

        let indexes = projection
            .iter()
            .map(|p| {
                self.schema()
                    .clone()
                    .fields
                    .iter()
                    .enumerate()
                    .find(|(_idx, field)| field.name.eq(p))
                    .map(|(idx, _field)| idx)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        // need to consider only relevant columns
        let output = stream! {
            for maybe_chunk in reader {
                let chunk = maybe_chunk?;

                let arrays = chunk.arrays();
                let mut r: Vec<std::sync::Arc<dyn Array>> = Vec::new();
                for idx in &indexes {
                    let array = arrays.get(*idx).unwrap();
                    r.push(array.clone());
                }

                yield Ok(Chunk::new(r));
            }
        };
        Box::pin(output) as RecordBatchStream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::datatypes::{DataType, Field, TimeUnit};
    use futures::StreamExt;

    #[test]
    fn test_parquet_schema() -> Result<()> {
        let test_file = "./parquet-testing/data/alltypes_plain.parquet";
        let parquet_datasource = ParquetDataSource::new(test_file.to_string())?;

        let actual_schema = parquet_datasource.schema();

        let expected_schema = Schema::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("smallint_col", DataType::Int32, true),
            Field::new("int_col", DataType::Int32, true),
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
            Field::new("double_col", DataType::Float64, true),
            Field::new("date_string_col", DataType::Binary, true),
            Field::new("string_col", DataType::Binary, true),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]);

        assert_eq!(actual_schema, expected_schema);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet() -> Result<()> {
        let test_file = "./parquet-testing/data/alltypes_plain.parquet";
        let parquet_datasource = ParquetDataSource::new(test_file.to_string())?;

        let mut rbs = parquet_datasource.scan(vec!["id".to_string()]);
        let mut actual_row_count = 0;

        for rrb in rbs.next().await {
            let rb = rrb?;
            assert_eq!(rb.columns().len(), 1); // only a single column is requested
            actual_row_count += rb.columns().get(0).unwrap().len();
        }
        assert_eq!(actual_row_count, 8);

        let x = parquet_datasource.scan(vec!["id".to_string()]);
        crate::pretty_print(x, parquet_datasource.schema()).await;

        Ok(())
    }
}
