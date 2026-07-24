use anyhow::{Error, Result, anyhow};
use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use flate2::read::{GzDecoder, ZlibDecoder};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask as OrcProjectionMask;
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};
use snap::read::FrameDecoder as SnappyDecoder;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use zstd::stream::read::Decoder as ZstdDecoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Orc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Zlib,
    Snappy,
    Zstd,
}

pub fn detect_compression_from_extension(file_path: &str) -> CompressionType {
    let path = Path::new(file_path);
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    if file_name.ends_with(".gz") {
        CompressionType::Gzip
    } else if file_name.ends_with(".zlib") || file_name.ends_with(".z") {
        CompressionType::Zlib
    } else if file_name.ends_with(".snappy") || file_name.ends_with(".sz") {
        CompressionType::Snappy
    } else if file_name.ends_with(".zst") || file_name.ends_with(".zstd") {
        CompressionType::Zstd
    } else {
        CompressionType::None
    }
}

pub fn get_file_type(file_path: &str) -> Result<FileType> {
    let path = Path::new(file_path);
    let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    // Remove compression extensions to get the actual file type
    let mut core_name = file_name;

    if let Some(name) = core_name.strip_suffix(".gz") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zlib") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".z") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".snappy") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".sz") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zst") {
        core_name = name;
    } else if let Some(name) = core_name.strip_suffix(".zstd") {
        core_name = name;
    }

    if core_name.ends_with(".parquet") {
        Ok(FileType::Parquet)
    } else if core_name.ends_with(".orc") {
        Ok(FileType::Orc)
    } else {
        Err(anyhow!("Unsupported file type for file: {}", file_path))
    }
}

/// Where batches are read from.
///
/// Uncompressed files are opened in place. Compressed files are streamed
/// through the decoder into a temporary file once (constant memory), and all
/// readers then get random access to that file; it is deleted when the last
/// `DataSource` clone drops.
#[derive(Clone)]
pub enum DataSource {
    File(PathBuf),
    Decompressed(Arc<NamedTempFile>),
}

impl DataSource {
    pub fn open(file_path: &str) -> Result<Self> {
        let path = Path::new(file_path);
        if !path.exists() {
            return Err(anyhow!("File not found: {}", file_path));
        }

        let compression = detect_compression_from_extension(file_path);
        if matches!(compression, CompressionType::None) {
            return Ok(DataSource::File(path.to_path_buf()));
        }

        let file = BufReader::new(File::open(path).map_err(Error::new)?);
        let mut decoder: Box<dyn Read> = match compression {
            CompressionType::None => unreachable!(),
            CompressionType::Gzip => Box::new(GzDecoder::new(file)),
            CompressionType::Zlib => Box::new(ZlibDecoder::new(file)),
            CompressionType::Snappy => Box::new(SnappyDecoder::new(file)),
            CompressionType::Zstd => Box::new(ZstdDecoder::new(file).map_err(Error::new)?),
        };

        let mut temp = NamedTempFile::new().map_err(Error::new)?;
        std::io::copy(&mut decoder, temp.as_file_mut()).map_err(Error::new)?;
        Ok(DataSource::Decompressed(Arc::new(temp)))
    }

    /// Open a fresh read handle; can be called repeatedly (e.g. once per scan).
    fn reader_file(&self) -> Result<File> {
        match self {
            DataSource::File(path) => File::open(path).map_err(Error::new),
            DataSource::Decompressed(temp) => temp.reopen().map_err(Error::new),
        }
    }
}

pub type BatchReader = Box<dyn RecordBatchReader + Send>;

/// Open a streaming batch reader over `source`, optionally projecting to the
/// given top-level columns. Can be called repeatedly on the same source.
pub fn open_batch_reader(
    source: &DataSource,
    file_type: FileType,
    columns: Option<&[String]>,
) -> Result<BatchReader> {
    let file = source.reader_file()?;
    match file_type {
        FileType::Parquet => open_parquet_reader(file, columns),
        FileType::Orc => open_orc_reader(file, columns),
    }
}

fn open_parquet_reader(file: File, columns: Option<&[String]>) -> Result<BatchReader> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(Error::new)?;

    if let Some(column_names) = columns {
        let indices = column_indices(&builder.schema().clone(), column_names)?;
        let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(projection);
    }

    Ok(Box::new(builder.build().map_err(Error::new)?))
}

fn open_orc_reader(file: File, columns: Option<&[String]>) -> Result<BatchReader> {
    let builder = ArrowReaderBuilder::try_new(file).map_err(Error::new)?;

    let builder = if let Some(column_names) = columns {
        column_indices(&builder.schema(), column_names)?; // validate names
        let root_data_type = builder.file_metadata().root_data_type();
        let projection = OrcProjectionMask::named_roots(root_data_type, column_names);
        builder.with_projection(projection)
    } else {
        builder
    };

    Ok(Box::new(builder.build()))
}

fn column_indices(schema: &SchemaRef, column_names: &[String]) -> Result<Vec<usize>> {
    column_names
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| anyhow!("Column '{}' not found in schema", name))
        })
        .collect()
}

/// Arrow schema of the file (used for tree display and validation).
pub fn get_arrow_schema(file_path: &str) -> Result<SchemaRef> {
    let file_type = get_file_type(file_path)?;
    let source = DataSource::open(file_path)?;
    Ok(open_batch_reader(&source, file_type, None)?.schema())
}

/// Native schema representation as a debug string.
pub fn get_raw_schema_string(file_path: &str) -> Result<String> {
    let file_type = get_file_type(file_path)?;
    let source = DataSource::open(file_path)?;

    match file_type {
        FileType::Parquet => {
            let reader = SerializedFileReader::new(source.reader_file()?).map_err(Error::new)?;
            let schema = reader.metadata().file_metadata().schema();
            Ok(format!("{:#?}", schema))
        }
        FileType::Orc => Ok(format!(
            "{:#?}",
            open_batch_reader(&source, file_type, None)?.schema()
        )),
    }
}

pub fn get_parquet_metadata(file_path: &str) -> Result<()> {
    let source = DataSource::open(file_path)?;
    let reader = SerializedFileReader::new(source.reader_file()?).map_err(Error::new)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

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
    Ok(())
}

pub fn get_orc_metadata(file_path: &str) -> Result<()> {
    let source = DataSource::open(file_path)?;
    let reader_builder = ArrowReaderBuilder::try_new(source.reader_file()?).map_err(Error::new)?;
    let metadata = reader_builder.file_metadata();

    println!("Number of rows: {}", metadata.number_of_rows());

    if !metadata.user_custom_metadata().is_empty() {
        println!("User metadata:");
        for (key, value) in metadata.user_custom_metadata() {
            println!("  {}: {:?}", key, value);
        }
    }
    Ok(())
}
