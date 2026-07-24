use clap::{Parser, Subcommand, ValueEnum};

#[derive(ValueEnum, Clone, Debug)]
pub enum SchemaFormat {
    Raw,
    Tree,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Ndjson,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Display metadata about ORC/Parquet files
    Metadata {
        /// Input file paths
        files: Vec<String>,
    },
    /// Display schema information
    Schema {
        /// Input file paths
        files: Vec<String>,
        /// Schema output format
        #[arg(short, long, value_enum, default_value_t = SchemaFormat::Raw)]
        format: SchemaFormat,
    },
    /// View file contents with optional column selection and row limit
    View {
        /// Input file path
        file: String,
        /// Column names to select (if not specified, selects all)
        #[arg(short, long, value_delimiter = ',')]
        columns: Option<Vec<String>>,
        /// Number of rows to display (0 = all rows)
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        /// Output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
        /// Truncate column values longer than this many characters (0 to disable)
        #[arg(short, long, default_value_t = 0)]
        truncate: usize,
        /// Sort columns. Format: "col1,col2-,col3:desc" (default: ascending)
        #[arg(short, long, value_delimiter = ',')]
        sort: Option<Vec<String>>,
    },
}
