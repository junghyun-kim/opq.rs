# opq.rs (ORC Parquet Query built with Rust)
A fast and efficient command-line tool for viewing, inspecting, and querying ORC and Parquet files with comprehensive compression support.

## Features

### File Format Support
- **ORC** - Optimized Row Columnar format
- **Parquet** - Apache Parquet columnar storage format

### Compression Support
- **gzip** (`.gz`)
- **zstd** (`.zst`, `.zstd`)
- **snappy** (`.snappy`, `.sz`)
- **zlib** (`.zlib`, `.z`)
- **Uncompressed** files

### Output Formats
- **Table** - Pretty-printed table format (default)
- **NDJSON** - Newline-delimited JSON format

Note: Vertical format has been removed. Use table format for structured output or NDJSON for data export.

### Commands
- **schema** - View file schema and structure with tree visualization support
- **metadata** - View file metadata and statistics 
- **view** - View file contents with filtering, sorting, column selection, and customizable output formats

### Advanced Features
- **Filtering** - Filter rows with SQL expressions via `--filter`
  - Comparisons, boolean logic, string matching: `"age > 30 AND city = 'Seoul'"`
  - Works on any format (ORC, Parquet, compressed) and combines with sorting and column selection
  - Filter columns don't need to be included in `--columns`
  - On uncompressed Parquet, predicates prune row groups for fast scans
- **Tree Schema Visualization** - Hierarchical display of complex nested structures
- **Multiple File Support** - Process multiple files with schema and metadata commands
- **Column Selection** - Choose specific columns for better performance and focused analysis
  - Supports top-level columns only (e.g., `name`, `age`, `address`)
  - Nested field selection (e.g., `address.city`) is not currently supported
  - Complex nested structures are returned as complete objects
- **Sorting** - Sort data by single or multiple columns with flexible syntax
  - Single DataFusion-based engine for ORC, Parquet, and compressed files
  - With a row limit, the sort runs as a streaming TopK (only `limit` rows kept in memory)
  - Full sorts larger than `--memory-limit` (default 1G) automatically spill to disk (external sort)
  - Multiple sort formats: `column`, `column+/-`, `column:asc/desc`
  - Sort columns don't need to be included in `--columns`
- **Streaming Processing** - Memory-efficient handling of large files
- **Auto-detection** - Automatic file type and compression format detection

## Installation


### From Source
```bash
git clone https://github.com/junghyun-kim/opq.rs.git
cd opq.rs
cargo build --release
```

The binary will be available at `target/release/opq`.

## Usage

### Basic Commands

#### View Schema
```bash
# View schema of an ORC file
opq schema data.orc

# View schema of a compressed Parquet file in tree format
opq schema data.parquet.gz --format tree

# View schema of multiple files
opq schema data1.parquet data2.orc samples/*.parquet

# Tree format shows hierarchical structure for nested data
opq schema nested_data.parquet --format tree
```

#### View Metadata
```bash
# View metadata of a Parquet file
opq metadata data.parquet

# View metadata of multiple files
opq metadata data.parquet data.orc samples/*.parquet

# View metadata of a compressed ORC file
opq metadata data.orc.zst
```

#### View Data Content
```bash
# View first 10 rows in table format (default)
opq view data.parquet

# View first 20 rows
opq view data.orc --limit 20

# View all rows
opq view data.orc --limit 0

# Export to NDJSON format
opq view data.parquet.gz --format ndjson --limit 100

# Select specific columns for better performance
opq view data.parquet --columns "id,name,email" --limit 10

# Single column selection
opq view data.parquet --columns "name" --limit 5

# Select nested structures as complete objects
opq view nested_data.parquet --columns "id,address,metadata" --limit 5

# Filter rows with a SQL expression
opq view data.parquet --filter "age > 30" --limit 10

# Combine filtering, sorting, and column selection
opq view data.parquet --columns "name,age" --filter "city = 'Seoul'" --sort "age-" --limit 10

# Sort data by columns (ascending by default)
opq view data.parquet --sort "name" --limit 10

# Sort by multiple columns with different orders
opq view data.parquet --sort "age-,name,salary:desc" --limit 20

# Combine sorting with column selection
opq view data.parquet --columns "name,age,salary" --sort "age-" --limit 15

# Note: view command processes one file at a time
# For multiple files, use separate commands:
# opq view file1.parquet --limit 5
# opq view file2.orc --limit 5
```

### Output Format Examples

#### Table Format (Default)
```
+-------------+----------+--------+-------------------------+
| PassengerId | Survived | Pclass | Name                    |
+-------------+----------+--------+-------------------------+
| 1           | 0        | 3      | Braund, Mr. Owen Harris |
| 2           | 1        | 1      | Cumings, Mrs. John...   |
+-------------+----------+--------+-------------------------+
```

#### Tree Schema Format
```
Schema Tree (parquet):
└── root
    ├── id: INT64
    ├── name: UTF8
    ├── address: STRUCT
    │   ├── street: UTF8
    │   ├── city: UTF8
    │   └── coordinates: STRUCT
    │       ├── lat: FLOAT64
    │       └── lng: FLOAT64
    └── metadata: STRUCT
        ├── created_at: UTF8
        └── preferences: STRUCT
            ├── theme: UTF8
            └── language: UTF8
```

#### NDJSON Format
```json
{"PassengerId":1,"Survived":0,"Pclass":3,"Name":"Braund, Mr. Owen Harris","Sex":"male","Age":22.0}
{"PassengerId":2,"Survived":1,"Pclass":1,"Name":"Cumings, Mrs. John Bradley (Florence Briggs Thayer)","Sex":"female","Age":38.0}
```

## Compression Support

OPQ.RS automatically detects compression based on file extensions:

| Compression | Extensions | Example |
|-------------|------------|---------|
| gzip | `.gz` | `data.parquet.gz`, `data.orc.gz` |
| zstd | `.zst`, `.zstd` | `data.parquet.zst`, `data.orc.zstd` |
| snappy | `.snappy`, `.sz` | `data.parquet.snappy`, `data.orc.sz` |
| zlib | `.zlib`, `.z` | `data.parquet.zlib`, `data.orc.z` |

## Examples

### Working with Compressed Files
```bash
# Schema of gzip-compressed Parquet with tree view
opq schema sales_data.parquet.gz --format tree

# Metadata of zstd-compressed ORC
opq metadata logs.orc.zst

# View snappy-compressed data
opq view user_events.parquet.snappy --limit 5
```

### Data Exploration Workflow
```bash
# 1. Check the schema first with tree visualization
opq schema dataset.parquet --format tree

# 2. View metadata for size info
opq metadata dataset.parquet

# 3. Preview data in table format
opq view dataset.parquet --limit 10

# 4. Select specific columns for analysis
opq view dataset.parquet --columns "user_id,timestamp,event_type" --limit 20

# 5. Sort data for better insights
opq view dataset.parquet --columns "name,age,salary" --sort "age-,salary:desc" --limit 15

# 6. Export sample to JSON for further processing
opq view dataset.parquet --columns "id,name,email" --format ndjson --limit 1000 > sample.jsonl
```

### Filtering Examples

```bash
# Numeric comparison
opq view employees.parquet --filter "salary >= 50000" --limit 20

# String equality and boolean logic
opq view employees.parquet --filter "department = 'Engineering' AND age < 40"

# Filter on a column that is not displayed
opq view employees.parquet --columns "name" --filter "salary > 100000"

# Filter then sort: show the 10 largest recent transactions
opq view transactions.orc --filter "amount > 0" --sort "amount:desc" --limit 10

# Export all matching rows
opq view logs.parquet.gz --filter "level = 'ERROR'" --limit 0 --format ndjson > errors.jsonl
```

Filter expressions use SQL syntax: comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`),
boolean logic (`AND`, `OR`, `NOT`), `IN (...)`, `BETWEEN`, `LIKE`, and `IS [NOT] NULL`.
String literals use single quotes. Column names are matched exactly (case-sensitive).

### Sorting Examples

#### Basic Sorting
```bash
# Sort by single column (ascending by default)
opq view employees.parquet --sort "name" --limit 20

# Sort by single column (explicit ascending)
opq view employees.parquet --sort "name+" --limit 20

# Sort by single column (descending)
opq view employees.parquet --sort "age-" --limit 20

# Sort using explicit syntax
opq view employees.parquet --sort "salary:desc" --limit 20

# Full sort of a large file within a memory budget (spills to disk when exceeded)
opq view huge_dataset.parquet --sort "timestamp" --limit 0 --memory-limit 512M
```

#### Multi-Column Sorting
```bash
# Sort by multiple columns (mixed orders)
opq view employees.parquet --sort "department,age-,salary:desc" --limit 25

# Priority: department (asc) → age (desc) → salary (desc)
opq view employees.parquet --sort "department:asc,age:desc,salary:desc" --limit 30
```

#### Sorting with Column Selection
```bash
# Combine column selection and sorting
opq view sales.parquet --columns "date,product,amount,region" --sort "date-,amount:desc" --limit 50

# Focus on specific columns with relevant sorting
opq view transactions.parquet --columns "timestamp,user_id,amount" --sort "timestamp-" --limit 100
```

#### Supported Sort Formats
| Format | Description | Example |
|--------|-------------|---------|
| `column` | Ascending (default) | `--sort "name"` |
| `column+` | Explicit ascending | `--sort "name+"` |
| `column-` | Descending | `--sort "age-"` |
| `column:asc` | Explicit ascending | `--sort "salary:asc"` |
| `column:desc` | Explicit descending | `--sort "date:desc"` |

#### Sorting Performance
- All formats share one DataFusion-based sort pipeline (scan → sort → project → limit)
- With a `--limit`, DataFusion executes the sort as a streaming TopK, keeping only `limit` rows in memory
- Full sorts (`--limit 0`) run in memory while they fit within `--memory-limit` (default `1G`);
  beyond that, sorted runs spill to temporary disk files and are merged (external sort),
  so peak memory stays bounded regardless of file size
- `--memory-limit 0` disables the budget (everything stays in memory)
- Uncompressed Parquet is scanned natively by DataFusion (parallel, with projection pushdown)
- ORC and compressed files stream through opq's own readers into DataFusion

### Working with Multiple Files
```bash
# View schema of multiple files
opq schema data/*.parquet --format tree

# Check metadata for all ORC files in a directory
opq metadata logs/*.orc

# Process multiple files with the same command
opq schema samples/sample_parquet_1/titanic.parquet samples/sample_orc_2/iris.orc

# Note: view command only supports one file at a time
# For viewing multiple files, run separate commands:
for file in data/*.parquet; do
  echo "=== Processing $file ==="
  opq view "$file" --limit 5
done
```

## Supported Data Types

OPQ.RS handles complex nested data structures including:
- Primitive types (integers, floats, strings, booleans)
- Nested structs with hierarchical tree visualization
- Arrays and lists
- Maps and dictionaries
- Timestamp and date types

### Schema Visualization
- **Raw format**: Shows native schema representation
- **Tree format**: Hierarchical visualization perfect for understanding nested structures

### Column Selection Limitations
- **Supported**: Top-level column selection (`id`, `name`, `address`)
- **Not supported**: Nested field paths (`address.city`, `metadata.preferences.theme`)
- **Workaround**: Select the entire nested structure and use post-processing tools

## Performance

- **Fast startup** - Optimized for quick data inspection
- **Memory efficient** - Streams data without loading entire files into memory
- **Large file support** - Handles multi-GB files efficiently
- **Compression aware** - Automatic decompression without temporary files
- **Arrow-based** - Uses Apache Arrow for unified data processing

### Large File Handling

OPQ.RS is designed to handle large files efficiently:

#### ✅ **Uncompressed Files (Recommended for large datasets)**
- **Streaming processing** - Only loads data as needed
- **Memory usage** - Constant memory usage regardless of file size
- **5GB+ files** - No memory issues, processes in batches

#### ⚠️ **Compressed Files (Memory considerations)**
- **Memory requirement** - Decompressed file size + processing overhead
- **5GB compressed file** - May require 6-8GB available memory
- **Recommendation** - For very large files, use uncompressed format when possible

#### 💡 **Best Practices for Large Files**
```bash
# Check file size and metadata first
opq metadata large_dataset.parquet

# Preview small sample before processing large amounts
opq view large_dataset.parquet --limit 10

# View schema with tree format to understand structure
opq schema large_dataset.parquet --format tree

# For very large files, consider using limit parameter
opq view huge_file.parquet --limit 1000 --format ndjson > sample.jsonl
```

## Requirements

- Rust 1.89.0 or later
- macOS, Linux, or Windows

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request


## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Changelog

### v0.4.0 (Latest)
- **New**: `--filter` — filter rows with SQL expressions (`--filter "age > 30 AND city = 'Seoul'"`); works on all formats, combines with sorting/column selection, and prunes Parquet row groups for fast scans
- **Fixed**: Sorting now returns globally correct results (previously data was only sorted within each batch, and large ORC files stopped being read early)
- **Fixed**: Sorting works on compressed files (previously failed with a file-extension error)
- **Fixed**: `--truncate` no longer panics on multi-byte UTF-8 text (now truncates by characters)
- **Fixed**: Dates, timestamps, decimals, and nested types render properly in table output (previously showed placeholders like `<Date32>`)
- **Fixed**: Build failure caused by two incompatible arrow versions being resolved; `Cargo.lock` is now committed for reproducible builds
- **New**: `--limit 0` displays all rows
- **New**: `--memory-limit` (default `1G`) — full sorts larger than the budget automatically spill to temporary disk files (true external sort); smaller sorts stay in memory
- **New**: Sort columns no longer need to be included in `--columns`
- **Changed**: All sorting paths unified into a single DataFusion pipeline; with `--limit` the sort executes as a streaming TopK
- **Changed**: ORC reading is now lazy/streaming instead of loading the whole file up front
- **Removed**: Dead `main_new.rs` and unused vertical-format code
- **Added**: End-to-end CLI test suite covering sorting, compression, formatting, and limits

### v0.3.0
- **New Feature**: Intelligent sorting functionality with adaptive strategy
- **New Feature**: Support for multiple sort formats (`column`, `column+/-`, `column:asc/desc`)
- **New Feature**: Multi-column sorting with mixed ascending/descending orders
- **Enhancement**: TopK optimization for small result sets (< 1000 rows)
- **Enhancement**: External sorting with DataFusion for large datasets
- **Enhancement**: Memory-efficient streaming processing for sorted data
- **Enhancement**: Async runtime with tokio for better performance
- **Technical**: Added DataFusion integration for advanced query processing
- **Technical**: Comprehensive sort validation and error handling

### v0.2.0
- **Breaking Changes**: Simplified CLI interface - removed `--file` flag, files are now positional arguments
- **New Feature**: Tree schema visualization with `--format tree`
- **New Feature**: Multiple file support for schema and metadata commands
- **Enhancement**: Arrow-based unified schema processing for consistent output
- **Enhancement**: Improved nested structure support with proper hierarchical display
- **Removed**: Vertical output format (use table format instead)
- **Fixed**: Command renamed from `meta` to `metadata` for clarity

### v0.1.0
- Initial release
- Support for ORC and Parquet files
- Multiple output formats (table, vertical, ndjson)
- Comprehensive compression support (gzip, zstd, snappy, zlib)
- Schema and metadata inspection
- Auto-detection of file types and compression formats
