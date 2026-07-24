//! End-to-end tests for the `opq` CLI.
//!
//! Fixtures are generated once into `CARGO_TARGET_TMPDIR`:
//! - test.parquet: 3000 rows, `value` strictly descending (2999..0) so the
//!   globally smallest values live in the LAST reader batch. Catches any sort
//!   implementation that only sorts within a batch or stops reading early.
//! - test.orc: 120_000 rows with the same descending pattern, large enough to
//!   span many reader batches.
//! - .gz variants of both.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, OnceLock};

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

const PARQUET_ROWS: i64 = 3000;
const ORC_ROWS: i64 = 120_000;
const BIG_SORT_ROWS: i64 = 1_000_000;

fn fixture_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("opq-fixtures");
        std::fs::create_dir_all(&dir).unwrap();
        if !dir.join("test.parquet").exists() {
            write_parquet_fixture(&dir.join("test.parquet"));
            gzip_file(&dir.join("test.parquet"), &dir.join("test.parquet.gz"));
        }
        if !dir.join("test.orc").exists() {
            write_orc_fixture(&dir.join("test.orc"));
            gzip_file(&dir.join("test.orc"), &dir.join("test.orc.gz"));
        }
        if !dir.join("big_sort.orc").exists() {
            write_big_sort_fixture(&dir.join("big_sort.orc"));
        }
        dir
    })
}

fn fixture(name: &str) -> String {
    fixture_dir().join(name).to_str().unwrap().to_string()
}

fn write_parquet_fixture(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("d", DataType::Date32, false),
    ]));

    let n = PARQUET_ROWS;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..n)) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((0..n).map(|i| n - 1 - i))),
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("홍길동_{:04}", i)),
            )),
            Arc::new(Date32Array::from_iter_values(
                (0..n).map(|i| 19000 + (i % 100) as i32),
            )),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_orc_fixture(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let file = File::create(path).unwrap();
    let mut writer = orc_rust::arrow_writer::ArrowWriterBuilder::new(file, schema.clone())
        .try_build()
        .unwrap();

    let m = ORC_ROWS;
    let step = 10_000;
    let mut start = 0;
    while start < m {
        let end = (start + step).min(m);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(start..end)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|i| m - 1 - i),
                )),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        start = end;
    }
    writer.close().unwrap();
}

/// ~84MB of in-memory Arrow data (1M rows with a 60-char payload), descending
/// `value` so a correct full sort must reorder the entire file.
fn write_big_sort_fixture(path: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let file = File::create(path).unwrap();
    let mut writer = orc_rust::arrow_writer::ArrowWriterBuilder::new(file, schema.clone())
        .try_build()
        .unwrap();

    let n = BIG_SORT_ROWS;
    let step = 100_000;
    let mut start = 0;
    while start < n {
        let end = (start + step).min(n);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(start..end)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|i| n - 1 - i),
                )),
                Arc::new(StringArray::from_iter_values(
                    (start..end).map(|i| format!("{:0>60}", i)),
                )),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        start = end;
    }
    writer.close().unwrap();
}

fn gzip_file(src: &Path, dst: &Path) {
    let data = std::fs::read(src).unwrap();
    let out = File::create(dst).unwrap();
    let mut encoder = flate2::write::GzEncoder::new(out, flate2::Compression::fast());
    encoder.write_all(&data).unwrap();
    encoder.finish().unwrap();
}

fn opq(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_opq"))
        .args(args)
        .output()
        .expect("failed to run opq binary")
}

/// Run opq expecting success; returns stdout.
fn opq_ok(args: &[&str]) -> String {
    let out = opq(args);
    assert!(
        out.status.success(),
        "opq {:?} failed.\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap()
}

/// Extract an integer field from each NDJSON line.
fn ndjson_field(stdout: &str, key: &str) -> Vec<i64> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("invalid ndjson line {:?}: {}", line, e));
            v[key]
                .as_i64()
                .unwrap_or_else(|| panic!("missing field '{}' in {:?}", key, line))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Sort correctness
// ---------------------------------------------------------------------------

#[test]
fn parquet_sort_asc_returns_global_minimum() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "value",
        "--limit",
        "5",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2, 3, 4]);
}

#[test]
fn parquet_sort_desc_returns_global_maximum() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "value:desc",
        "--limit",
        "3",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![2999, 2998, 2997]);
}

#[test]
fn parquet_multi_column_sort_is_correct() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "value,id",
        "--limit",
        "3",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2]);
}

#[test]
fn orc_single_column_sort_returns_global_minimum() {
    let out = opq_ok(&[
        "view",
        &fixture("test.orc"),
        "--sort",
        "value",
        "--limit",
        "5",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2, 3, 4]);
}

#[test]
fn orc_multi_column_sort_reads_whole_file() {
    // 120k rows spans multiple internal chunks; the smallest values are at the
    // very end of the file, so any early-termination bug returns wrong rows.
    let out = opq_ok(&[
        "view",
        &fixture("test.orc"),
        "--sort",
        "value,id",
        "--limit",
        "5",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2, 3, 4]);
}

#[test]
fn orc_sort_with_large_limit_is_correct() {
    // limit >= 1000 previously took the "external sort" path.
    let out = opq_ok(&[
        "view",
        &fixture("test.orc"),
        "--sort",
        "value",
        "--limit",
        "1500",
        "--format",
        "ndjson",
    ]);
    let values = ndjson_field(&out, "value");
    assert_eq!(values.len(), 1500);
    assert_eq!(values, (0..1500).collect::<Vec<i64>>());
}

#[test]
fn compressed_parquet_sort_works() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet.gz"),
        "--sort",
        "value,id",
        "--limit",
        "3",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2]);
}

#[test]
fn compressed_orc_sort_works() {
    let out = opq_ok(&[
        "view",
        &fixture("test.orc.gz"),
        "--sort",
        "value",
        "--limit",
        "3",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2]);
}

#[test]
fn sort_column_does_not_need_to_be_selected() {
    // Sorting by `value` while only displaying `name` should work: the row
    // with the largest value is id 0 -> 홍길동_0000.
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--columns",
        "name",
        "--sort",
        "value:desc",
        "--limit",
        "1",
        "--format",
        "ndjson",
    ]);
    let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
    assert_eq!(v["name"], "홍길동_0000");
    assert!(v.get("value").is_none(), "value column must not be in output");
}

#[test]
fn sort_with_column_selection_keeps_selection() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--columns",
        "id,value",
        "--sort",
        "value",
        "--limit",
        "2",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1]);
    assert_eq!(ndjson_field(&out, "id"), vec![2999, 2998]);
}

#[test]
fn unknown_sort_column_is_a_friendly_error() {
    let out = opq(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "nope",
        "--limit",
        "5",
    ]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("nope"),
        "stderr should mention the bad column: {}",
        stderr
    );
}

// ---------------------------------------------------------------------------
// External (spilling) sort
// ---------------------------------------------------------------------------

#[test]
fn full_sort_larger_than_memory_limit_spills_and_completes() {
    // The dataset (~84MB in memory) exceeds the 64M budget, so the sort must
    // spill to disk to finish; without spilling it would abort with a
    // resources-exhausted error.
    let out = opq_ok(&[
        "view",
        &fixture("big_sort.orc"),
        "--sort",
        "value",
        "--limit",
        "0",
        "--memory-limit",
        "64M",
        "--format",
        "ndjson",
    ]);
    let values = ndjson_field(&out, "value");
    assert_eq!(values.len(), BIG_SORT_ROWS as usize);
    assert_eq!(values[0], 0);
    assert_eq!(*values.last().unwrap(), BIG_SORT_ROWS - 1);
    assert!(
        values.windows(2).all(|w| w[0] <= w[1]),
        "output must be globally sorted"
    );
}

#[test]
fn sort_with_memory_limit_on_small_file_stays_in_memory() {
    // Small sorts must still work (and stay in memory) under a budget.
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "value",
        "--limit",
        "3",
        "--memory-limit",
        "64M",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "value"), vec![0, 1, 2]);
}

#[test]
fn invalid_memory_limit_is_rejected() {
    let out = opq(&[
        "view",
        &fixture("test.parquet"),
        "--sort",
        "value",
        "--memory-limit",
        "banana",
    ]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("banana"),
        "stderr should mention the invalid value: {}",
        stderr
    );
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

#[test]
fn truncate_does_not_panic_on_multibyte_utf8() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--limit",
        "1",
        "--truncate",
        "5",
    ]);
    // 5 chars of "홍길동_0000" = "홍길동_0"
    assert!(
        out.contains("홍길동_0..."),
        "expected char-boundary truncation, got: {}",
        out
    );
}

#[test]
fn date_columns_render_as_dates_in_table() {
    let out = opq_ok(&["view", &fixture("test.parquet"), "--limit", "1"]);
    // Row id=0 has d = 19000 days since epoch = 2022-01-08.
    assert!(
        out.contains("2022-01-08"),
        "expected formatted date, got: {}",
        out
    );
    assert!(
        !out.contains("<Date32>"),
        "date must not fall back to a type placeholder: {}",
        out
    );
}

#[test]
fn limit_zero_shows_all_rows() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--limit",
        "0",
        "--format",
        "ndjson",
    ]);
    assert_eq!(out.lines().count(), PARQUET_ROWS as usize);
}

#[test]
fn ndjson_respects_limit() {
    let out = opq_ok(&[
        "view",
        &fixture("test.parquet"),
        "--limit",
        "7",
        "--format",
        "ndjson",
    ]);
    assert_eq!(out.lines().count(), 7);
}

#[test]
fn view_compressed_orc_without_sort_works() {
    let out = opq_ok(&[
        "view",
        &fixture("test.orc.gz"),
        "--limit",
        "2",
        "--format",
        "ndjson",
    ]);
    assert_eq!(ndjson_field(&out, "id"), vec![0, 1]);
}

// ---------------------------------------------------------------------------
// Schema / metadata smoke tests
// ---------------------------------------------------------------------------

#[test]
fn schema_tree_shows_columns() {
    let out = opq_ok(&["schema", &fixture("test.parquet"), "--format", "tree"]);
    assert!(out.contains("value: INT64"), "got: {}", out);
    assert!(out.contains("name: UTF8"), "got: {}", out);
}

#[test]
fn metadata_shows_row_count() {
    let out = opq_ok(&["metadata", &fixture("test.parquet")]);
    assert!(out.contains("Number of rows: 3000"), "got: {}", out);

    let out = opq_ok(&["metadata", &fixture("test.orc")]);
    assert!(out.contains("Number of rows: 120000"), "got: {}", out);
}
