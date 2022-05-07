use std::fs::File;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::*;
use async_stream::stream;
use crate::error::Result;
use crate::datasource::DataSource;
use crate::RecordBatchStream;

pub struct ParquetDataSource {
    file_path: String,
    //file_reader: FileReader<File>,
}

impl ParquetDataSource {
    pub fn new(file_path: String) -> Result<ParquetDataSource> {
        let file = File::open(file_path.clone())?;
        let _ = FileReader::try_new(file, None, None, None, None)?;
        Ok(ParquetDataSource {
            file_path,
        })
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

    fn scan(&self, _projection: Vec<String>) -> RecordBatchStream {
        let reader = self.get_reader().unwrap();
        let output = stream! {
            for maybe_chunk in reader {
                let chunk = maybe_chunk?;
                yield Ok(chunk);
            }
        };
        Box::pin(output) as RecordBatchStream
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::StreamExt;
    use crate::RecordBatch;

    #[test]
    fn test_parquet_schema() -> Result<()> {
        let test_file = "/Users/timvw/src/github/simply-query-engine/test-data/alltypes_plain.parquet";
        let parquet_datasource = ParquetDataSource::new(test_file.to_string())?;

        let actual_schema = parquet_datasource.schema();
        println!("actual schema: {:?}", actual_schema);
        //let expected_schema = Schema::

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_parquet() -> Result<()> {
        let test_file = "/Users/timvw/src/github/simply-query-engine/test-data/alltypes_plain.parquet";
        let parquet_datasource = ParquetDataSource::new(test_file.to_string())?;

        let schema = parquet_datasource.schema();
        let mut rbs = parquet_datasource.scan(vec![]);
        for rb in rbs.next().await {
            println!("batch: {:?}", rb);
        }

        Ok(())
    }

    /*
    fn pretty_print(rbs: RecordBatchStream, schema: Schema) {
        let names = schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>();
        let c = rbs.collect()
        println!("{}", arrow2::io::print::write(&rbs, &names));
    }*/
}