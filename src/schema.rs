use crate::cli::SchemaFormat;
use crate::reader::{FileType, get_arrow_schema, get_file_type, get_raw_schema_string};
use anyhow::Result;
use arrow_schema::{DataType, Field, Schema};

pub fn show_schema(file: &str, format: &SchemaFormat) -> Result<()> {
    println!("=== {} ===", file);
    let file_type = get_file_type(file)?;

    match format {
        SchemaFormat::Raw => {
            println!("{}", get_raw_schema_string(file)?);
        }
        SchemaFormat::Tree => {
            let arrow_schema = get_arrow_schema(file)?;
            let label = match file_type {
                FileType::Parquet => "parquet",
                FileType::Orc => "orc",
            };
            print_arrow_schema_tree(&arrow_schema, label);
        }
    }

    Ok(())
}

fn print_arrow_schema_tree(schema: &Schema, file_type: &str) {
    println!("Schema Tree ({}):", file_type);
    println!("└── root");

    for (i, field) in schema.fields().iter().enumerate() {
        let is_last = i == schema.fields().len() - 1;
        let prefix = if is_last {
            "    └── "
        } else {
            "    ├── "
        };
        print_arrow_field_recursive(field, prefix, "    ", is_last);
    }
}

fn print_arrow_field_recursive(
    field: &Field,
    prefix: &str,
    base_indent: &str,
    is_last_sibling: bool,
) {
    let type_str = format_arrow_data_type(field.data_type());
    let nullable_str = if field.is_nullable() {
        " (nullable)"
    } else {
        ""
    };

    println!("{}{}: {}{}", prefix, field.name(), type_str, nullable_str);

    match field.data_type() {
        DataType::Struct(sub_fields) => {
            for (i, sub_field) in sub_fields.iter().enumerate() {
                let is_last_child = i == sub_fields.len() - 1;
                let child_prefix = if is_last_child {
                    "└── "
                } else {
                    "├── "
                };
                // struct의 자식들에 대해서는 항상 일관된 들여쓰기 사용
                let child_indent = if is_last_sibling { "    " } else { "│   " };
                let full_prefix = format!("{}{}{}", base_indent, child_indent, child_prefix);
                let next_base_indent = format!("{}{}", base_indent, child_indent);
                print_arrow_field_recursive(
                    sub_field,
                    &full_prefix,
                    &next_base_indent,
                    is_last_child,
                );
            }
        }
        DataType::List(list_field) => {
            let child_prefix = "└── ";
            let child_indent = if is_last_sibling { "    " } else { "│   " };
            let full_prefix = format!("{}{}{}", base_indent, child_indent, child_prefix);
            let next_base_indent = format!("{}{}", base_indent, child_indent);
            print_arrow_field_recursive(list_field, &full_prefix, &next_base_indent, true);
        }
        DataType::Map(map_field, _) => {
            let child_prefix = "└── ";
            let child_indent = if is_last_sibling { "    " } else { "│   " };
            let full_prefix = format!("{}{}{}", base_indent, child_indent, child_prefix);
            let next_base_indent = format!("{}{}", base_indent, child_indent);
            print_arrow_field_recursive(map_field, &full_prefix, &next_base_indent, true);
        }
        _ => {}
    }
}

fn format_arrow_data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "INT8".to_string(),
        DataType::Int16 => "INT16".to_string(),
        DataType::Int32 => "INT32".to_string(),
        DataType::Int64 => "INT64".to_string(),
        DataType::UInt8 => "UINT8".to_string(),
        DataType::UInt16 => "UINT16".to_string(),
        DataType::UInt32 => "UINT32".to_string(),
        DataType::UInt64 => "UINT64".to_string(),
        DataType::Float16 => "FLOAT16".to_string(),
        DataType::Float32 => "FLOAT32".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Timestamp(unit, tz) => match tz {
            Some(tz) => format!("TIMESTAMP({:?}, {})", unit, tz),
            None => format!("TIMESTAMP({:?})", unit),
        },
        DataType::Date32 => "DATE32".to_string(),
        DataType::Date64 => "DATE64".to_string(),
        DataType::Time32(unit) => format!("TIME32({:?})", unit),
        DataType::Time64(unit) => format!("TIME64({:?})", unit),
        DataType::Duration(unit) => format!("DURATION({:?})", unit),
        DataType::Interval(unit) => format!("INTERVAL({:?})", unit),
        DataType::Binary => "BINARY".to_string(),
        DataType::FixedSizeBinary(size) => format!("FIXED_SIZE_BINARY({})", size),
        DataType::LargeBinary => "LARGE_BINARY".to_string(),
        DataType::Utf8 => "UTF8".to_string(),
        DataType::LargeUtf8 => "LARGE_UTF8".to_string(),
        DataType::BinaryView => "BINARY_VIEW".to_string(),
        DataType::Utf8View => "UTF8_VIEW".to_string(),
        DataType::List(_) => "LIST".to_string(),
        DataType::ListView(_) => "LIST_VIEW".to_string(),
        DataType::FixedSizeList(_, size) => format!("FIXED_SIZE_LIST({})", size),
        DataType::LargeList(_) => "LARGE_LIST".to_string(),
        DataType::LargeListView(_) => "LARGE_LIST_VIEW".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Union(_, _) => "UNION".to_string(),
        DataType::Dictionary(key_type, value_type) => {
            format!(
                "DICTIONARY({}, {})",
                format_arrow_data_type(key_type),
                format_arrow_data_type(value_type)
            )
        }
        DataType::Decimal128(precision, scale) => format!("DECIMAL128({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("DECIMAL256({}, {})", precision, scale),
        DataType::Map(_, sorted) => format!("MAP({})", if *sorted { "sorted" } else { "unsorted" }),
        DataType::RunEndEncoded(_, _) => "RUN_END_ENCODED".to_string(),
    }
}
