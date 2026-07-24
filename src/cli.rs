use clap::{Parser, Subcommand, ValueEnum};

/// Parse a human-readable byte size: plain bytes ("1048576") or with a
/// K/M/G suffix, optionally followed by "B" ("64M", "2GB", case-insensitive).
/// "0" means unlimited.
pub fn parse_memory_limit(s: &str) -> Result<u64, String> {
    let normalized = s.trim().to_uppercase();
    let digits_end = normalized
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(normalized.len());
    let (number, suffix) = normalized.split_at(digits_end);

    let number: u64 = number
        .parse()
        .map_err(|_| format!("invalid size '{}': expected e.g. 512M, 2G, or 0 for unlimited", s))?;

    let multiplier: u64 = match suffix {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        _ => {
            return Err(format!(
                "invalid size suffix '{}': expected K, M, or G",
                suffix
            ));
        }
    };

    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size '{}' is too large", s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_bytes() {
        assert_eq!(parse_memory_limit("0").unwrap(), 0);
        assert_eq!(parse_memory_limit("512").unwrap(), 512);
    }

    #[test]
    fn parses_suffixes() {
        assert_eq!(parse_memory_limit("64K").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_limit("32M").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_memory_limit("2G").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parses_case_insensitive_and_b_suffix() {
        assert_eq!(parse_memory_limit("64k").unwrap(), 64 * 1024);
        assert_eq!(parse_memory_limit("32mb").unwrap(), 32 * 1024 * 1024);
        assert_eq!(parse_memory_limit("1GB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_memory_limit("banana").is_err());
        assert!(parse_memory_limit("").is_err());
        assert!(parse_memory_limit("12T").is_err());
        assert!(parse_memory_limit("M").is_err());
    }
}

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
        /// Filter rows with a SQL expression, e.g. "age > 30 AND city = 'Seoul'"
        #[arg(long)]
        filter: Option<String>,
        /// Memory budget for sorting (e.g. 512M, 2G; 0 = unlimited).
        /// Sorts larger than the budget spill to temporary disk files.
        #[arg(long, default_value = "1G", value_parser = parse_memory_limit)]
        memory_limit: u64,
    },
}
