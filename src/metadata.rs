//! Rich metadata output built purely from file footers — no data is scanned,
//! so this is instant even on multi-GB files.

use crate::reader::{DataSource, FileType, get_file_type};
use crate::schema::format_arrow_data_type;
use anyhow::{Error, Result};
use comfy_table::{Cell, Table};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::statistics::TypeStatistics;
use parquet::basic::LogicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

pub fn print_metadata(file_path: &str) -> Result<()> {
    match get_file_type(file_path)? {
        FileType::Parquet => print_parquet_metadata(file_path),
        FileType::Orc => print_orc_metadata(file_path),
    }
}

// ---------------------------------------------------------------------------
// Parquet
// ---------------------------------------------------------------------------

fn print_parquet_metadata(file_path: &str) -> Result<()> {
    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);
    let source = DataSource::open(file_path)?;
    let reader = SerializedFileReader::new(source.reader_file()?).map_err(Error::new)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    println!("File type: Parquet");
    println!("File size: {}", human_size(file_size));
    println!("Number of row groups: {}", metadata.num_row_groups());
    println!("Number of rows: {}", file_metadata.num_rows());
    println!(
        "Created by: {}",
        file_metadata.created_by().unwrap_or("N/A")
    );

    if let Some(key_value_metadata) = file_metadata.key_value_metadata() {
        println!("Key-value metadata:");
        for kv in key_value_metadata {
            println!("  {}: {}", kv.key, kv.value.as_deref().unwrap_or(""));
        }
    }

    println!();
    for (i, rg) in metadata.row_groups().iter().enumerate() {
        let uncompressed = rg.total_byte_size().max(0) as u64;
        let compressed = rg.compressed_size().max(0) as u64;
        let pct = if uncompressed > 0 {
            compressed as f64 / uncompressed as f64 * 100.0
        } else {
            100.0
        };
        println!(
            "Row group #{}: {} rows, {} uncompressed -> {} compressed ({:.1}%)",
            i,
            rg.num_rows(),
            human_size(uncompressed),
            human_size(compressed),
            pct
        );
    }

    // Aggregate per-column statistics across row groups.
    let num_columns = file_metadata.schema_descr().num_columns();
    let mut table = Table::new();
    table.set_header(vec!["Column", "Type", "Compression", "Nulls", "Min", "Max"]);

    for col_idx in 0..num_columns {
        let mut nulls: Option<u64> = Some(0);
        let mut min: Option<StatValue> = None;
        let mut max: Option<StatValue> = None;

        for rg in metadata.row_groups() {
            let column = rg.column(col_idx);
            match column.statistics() {
                Some(stats) => {
                    match (nulls, stats.null_count_opt()) {
                        (Some(acc), Some(n)) => nulls = Some(acc + n),
                        _ => nulls = None,
                    }
                    let (group_min, group_max) = stat_min_max(stats);
                    min = merge(min, group_min, false);
                    max = merge(max, group_max, true);
                }
                None => nulls = None,
            }
        }

        let first = metadata.row_groups().first();
        let (type_str, codec, logical) = match first {
            Some(rg) => {
                let column = rg.column(col_idx);
                let codec = format!("{:?}", column.compression());
                let codec = codec.split('(').next().unwrap_or(&codec).to_string();
                (
                    column.column_type().to_string(),
                    codec,
                    column.column_descr().logical_type(),
                )
            }
            None => ("-".to_string(), "-".to_string(), None),
        };

        table.add_row(vec![
            Cell::new(file_metadata.schema_descr().column(col_idx).path().string()),
            Cell::new(type_str),
            Cell::new(codec),
            Cell::new(nulls.map_or("-".to_string(), |n| n.to_string())),
            Cell::new(display_stat(&min, &logical)),
            Cell::new(display_stat(&max, &logical)),
        ]);
    }

    println!("{table}");
    Ok(())
}

/// A parquet min/max value in a comparable, display-friendly form.
#[derive(Clone, PartialEq)]
enum StatValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

fn stat_min_max(stats: &Statistics) -> (Option<StatValue>, Option<StatValue>) {
    use StatValue::*;
    match stats {
        Statistics::Boolean(s) => (
            s.min_opt().map(|v| Bool(*v)),
            s.max_opt().map(|v| Bool(*v)),
        ),
        Statistics::Int32(s) => (
            s.min_opt().map(|v| Int(*v as i64)),
            s.max_opt().map(|v| Int(*v as i64)),
        ),
        Statistics::Int64(s) => (s.min_opt().map(|v| Int(*v)), s.max_opt().map(|v| Int(*v))),
        Statistics::Float(s) => (
            s.min_opt().map(|v| Float(*v as f64)),
            s.max_opt().map(|v| Float(*v as f64)),
        ),
        Statistics::Double(s) => (
            s.min_opt().map(|v| Float(*v)),
            s.max_opt().map(|v| Float(*v)),
        ),
        Statistics::ByteArray(s) => (
            s.min_opt()
                .and_then(|v| v.as_utf8().ok())
                .map(|v| Text(v.to_string())),
            s.max_opt()
                .and_then(|v| v.as_utf8().ok())
                .map(|v| Text(v.to_string())),
        ),
        // Int96 timestamps and fixed-len binary aren't displayed.
        _ => (None, None),
    }
}

/// Fold a row group's min/max into the running value.
fn merge(acc: Option<StatValue>, next: Option<StatValue>, want_max: bool) -> Option<StatValue> {
    use StatValue::*;
    match (acc, next) {
        (None, next) => next,
        (acc, None) => acc,
        (Some(a), Some(b)) => {
            let b_wins = match (&a, &b) {
                (Bool(x), Bool(y)) => (y > x) == want_max,
                (Int(x), Int(y)) => (y > x) == want_max,
                (Float(x), Float(y)) => (y > x) == want_max,
                (Text(x), Text(y)) => (y > x) == want_max,
                _ => false,
            };
            Some(if b_wins { b } else { a })
        }
    }
}

fn display_stat(value: &Option<StatValue>, logical: &Option<LogicalType>) -> String {
    let Some(value) = value else {
        return "-".to_string();
    };
    match value {
        StatValue::Bool(v) => v.to_string(),
        StatValue::Int(v) => {
            if matches!(logical, Some(LogicalType::Date)) {
                days_to_date(*v as i32)
            } else {
                v.to_string()
            }
        }
        StatValue::Float(v) => v.to_string(),
        StatValue::Text(v) => v.clone(),
    }
}

// ---------------------------------------------------------------------------
// ORC
// ---------------------------------------------------------------------------

const MAX_STRIPES_SHOWN: usize = 12;

fn print_orc_metadata(file_path: &str) -> Result<()> {
    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);
    let source = DataSource::open(file_path)?;
    let builder = ArrowReaderBuilder::try_new(source.reader_file()?).map_err(Error::new)?;
    let metadata = builder.file_metadata();

    println!("File type: ORC");
    println!("File size: {}", human_size(file_size));
    println!("Number of rows: {}", metadata.number_of_rows());
    println!("Format version: {}", metadata.file_format_version());
    match metadata.compression() {
        Some(compression) => println!("Compression: {}", compression),
        None => println!("Compression: None"),
    }

    if !metadata.user_custom_metadata().is_empty() {
        println!("User metadata:");
        for (key, value) in metadata.user_custom_metadata() {
            println!("  {}: {:?}", key, value);
        }
    }

    println!();
    let stripes = metadata.stripe_metadatas();
    println!("Stripes: {}", stripes.len());
    for (i, stripe) in stripes.iter().take(MAX_STRIPES_SHOWN).enumerate() {
        println!(
            "Stripe #{}: {} rows, data {}",
            i,
            stripe.number_of_rows(),
            human_size(stripe.data_length())
        );
    }
    if stripes.len() > MAX_STRIPES_SHOWN {
        println!("  ... and {} more", stripes.len() - MAX_STRIPES_SHOWN);
    }

    let total_rows = metadata.number_of_rows();
    let file_stats = metadata.column_file_statistics();
    let mut table = Table::new();
    table.set_header(vec!["Column", "Type", "Nulls", "Min", "Max"]);

    for column in metadata.root_data_type().children() {
        let type_str = format_arrow_data_type(&column.data_type().to_arrow_data_type());
        let stats = file_stats.get(column.data_type().column_index());

        let (nulls, min, max) = match stats {
            Some(stats) => {
                let nulls = total_rows.saturating_sub(stats.number_of_values());
                let (min, max) = orc_min_max(stats.type_statistics());
                (nulls.to_string(), min, max)
            }
            None => ("-".to_string(), "-".to_string(), "-".to_string()),
        };

        table.add_row(vec![
            Cell::new(column.name()),
            Cell::new(type_str),
            Cell::new(nulls),
            Cell::new(min),
            Cell::new(max),
        ]);
    }

    println!("{table}");
    Ok(())
}

fn orc_min_max(stats: Option<&TypeStatistics>) -> (String, String) {
    match stats {
        Some(TypeStatistics::Integer { min, max, .. }) => (min.to_string(), max.to_string()),
        Some(TypeStatistics::Double { min, max, .. }) => (min.to_string(), max.to_string()),
        Some(TypeStatistics::String { min, max, .. }) => (min.clone(), max.clone()),
        Some(TypeStatistics::Decimal { min, max, .. }) => (min.clone(), max.clone()),
        Some(TypeStatistics::Date { min, max }) => (days_to_date(*min), days_to_date(*max)),
        Some(TypeStatistics::Timestamp {
            min_utc, max_utc, ..
        }) => (millis_to_datetime(*min_utc), millis_to_datetime(*max_utc)),
        Some(TypeStatistics::Bucket { true_count }) => {
            (format!("true count: {}", true_count), "-".to_string())
        }
        _ => ("-".to_string(), "-".to_string()),
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Days since the Unix epoch -> "YYYY-MM-DD" (Howard Hinnant's civil_from_days).
fn days_to_date(days: i32) -> String {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe as i64 + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Milliseconds since the Unix epoch -> "YYYY-MM-DD HH:MM:SS UTC".
fn millis_to_datetime(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86400);
    let tod = secs.rem_euclid(86400);
    format!(
        "{} {:02}:{:02}:{:02} UTC",
        days_to_date(days as i32),
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_to_date_covers_epoch_and_fixture_dates() {
        assert_eq!(days_to_date(0), "1970-01-01");
        assert_eq!(days_to_date(-1), "1969-12-31");
        assert_eq!(days_to_date(19000), "2022-01-08");
        assert_eq!(days_to_date(19099), "2022-04-17");
    }

    #[test]
    fn human_size_uses_binary_units() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2.0 KB");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MB");
    }
}
