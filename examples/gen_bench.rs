// Temporary generator for memory-usage measurements. Not part of the CLI.
use std::fs::File;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    std::fs::create_dir_all("/tmp/opq-test")?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let orc_file = File::create("/tmp/opq-test/big.orc")?;
    let mut orc_writer =
        orc_rust::arrow_writer::ArrowWriterBuilder::new(orc_file, schema.clone()).try_build()?;
    let pq_file = File::create("/tmp/opq-test/big.parquet")?;
    let mut pq_writer = parquet::arrow::ArrowWriter::try_new(pq_file, schema.clone(), None)?;

    let step = 100_000i64;
    let mut start = 0i64;
    while start < n {
        let end = (start + step).min(n);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(start..end)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values((start..end).map(|i| n - 1 - i))),
                Arc::new(StringArray::from_iter_values((start..end).map(|i| {
                    format!("{:0>60}", i) // 60-byte payload per row
                }))),
            ],
        )?;
        orc_writer.write(&batch)?;
        pq_writer.write(&batch)?;
        start = end;
    }
    orc_writer.close()?;
    pq_writer.close()?;
    println!("wrote /tmp/opq-test/big.orc + big.parquet ({} rows)", n);
    Ok(())
}
