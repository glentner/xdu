//! CLI argument definitions for all xdu binaries.
//!
//! These are extracted into a shared module so that `gen-completions` can
//! access the `clap::Command` objects via `CommandFactory::command()`.

use std::path::PathBuf;

use clap::Parser;

/// Arguments for `xdu` — the filesystem indexer.
#[derive(Parser, Debug)]
#[command(
    name = "xdu",
    about = "Build a distributed file metadata index in Parquet format",
    after_help = "\
Examples:
  xdu /data/scratch -o /index/scratch -j 8
  xdu /data/scratch -o /index/scratch --partition alice,bob
  xdu /data/scratch -o /index/scratch --apparent-size
  xdu /data/scratch -o /index/scratch --block-size 128K"
)]
pub struct XduArgs {
    /// Top-level directory to index
    #[arg(value_name = "DIR")]
    pub dir: PathBuf,

    /// Output directory for the Parquet index
    #[arg(short, long, value_name = "DIR")]
    pub outdir: PathBuf,

    /// Number of parallel threads
    #[arg(short, long, default_value = "4", env = "XDU_JOBS")]
    pub jobs: usize,

    /// Number of records per output chunk
    #[arg(short = 'B', long, default_value = "100000")]
    pub buffsize: usize,

    /// Report apparent sizes (file length) rather than disk usage.
    /// By default, xdu reports actual disk usage from st_blocks.
    #[arg(long)]
    pub apparent_size: bool,

    /// Round sizes up to this block size (e.g., 128K, 1M).
    /// Useful when st_blocks is inaccurate (e.g., over NFS) and you know the filesystem block size.
    /// Implies --apparent-size for the base size, then rounds up.
    #[arg(short = 'k', long, value_name = "SIZE")]
    pub block_size: Option<String>,

    /// Index only specific partitions (top-level subdirectory names, comma-separated).
    #[arg(short, long, value_name = "NAMES", value_delimiter = ',')]
    pub partition: Option<Vec<String>>,
}

/// Arguments for `xdu-find` — the index query tool.
#[derive(Parser, Debug)]
#[command(
    name = "xdu-find",
    about = "Query a file metadata index for matching paths",
    after_help = "Examples:
  xdu-find -i /index/scratch -p '\\.py$' --min-size 1M
  xdu-find -i /index/scratch --older-than 90 -f size
  xdu-find -i /index/scratch -u alice --count
  xdu-find -i /index/scratch --top 10"
)]
pub struct XduFindArgs {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR", env = "XDU_INDEX")]
    pub index: PathBuf,

    /// Regular expression pattern to match paths
    #[arg(short, long, value_name = "REGEX")]
    pub pattern: Option<String>,

    /// Filter by partition (user directory name)
    #[arg(short = 'u', long, value_name = "NAME")]
    pub partition: Option<String>,

    /// Minimum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    /// Maximum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Files not accessed in N days
    #[arg(long, value_name = "DAYS")]
    pub older_than: Option<u64>,

    /// Files accessed within N days
    #[arg(long, value_name = "DAYS")]
    pub newer_than: Option<u64>,

    /// Output format: path (default), size, atime, csv, json
    #[arg(short, long, default_value = "path")]
    pub format: String,

    /// Limit number of results
    #[arg(short, long)]
    pub limit: Option<usize>,

    /// Count matching records instead of listing them
    #[arg(short, long)]
    pub count: bool,

    /// Show top N partitions by file count (for identifying large partitions)
    #[arg(long, value_name = "N")]
    pub top: Option<usize>,
}

/// Arguments for `xdu-view` — the interactive TUI explorer.
#[derive(Parser, Debug)]
#[command(
    name = "xdu-view",
    about = "Interactive TUI for exploring a file metadata index",
    after_help = "\
Examples:
  xdu-view -i /index/scratch
  xdu-view -i /index/scratch -u alice
  xdu-view -i /index/scratch --older-than 90 --sort size"
)]
pub struct XduViewArgs {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR", env = "XDU_INDEX")]
    pub index: PathBuf,

    /// Initial partition to view (optional, shows partition list if omitted)
    #[arg(short = 'u', long, value_name = "NAME")]
    pub partition: Option<String>,

    /// Regular expression pattern to match paths
    #[arg(short, long, value_name = "REGEX")]
    pub pattern: Option<String>,

    /// Minimum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    /// Maximum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Files not accessed in N days
    #[arg(long, value_name = "DAYS")]
    pub older_than: Option<u64>,

    /// Files accessed within N days
    #[arg(long, value_name = "DAYS")]
    pub newer_than: Option<u64>,

    /// Sort order: name, size-asc, size-desc, count-asc, count-desc, age-asc, age-desc
    #[arg(short, long, default_value = "name")]
    pub sort: String,
}

/// Arguments for `xdu-rm` — the bulk deletion tool.
#[derive(Parser, Debug)]
#[command(
    name = "xdu-rm",
    about = "Delete files matching index query criteria",
    after_help = "Examples:
  xdu-rm -i /index/scratch -u alice --older-than 180 -n
  xdu-rm -i /index/scratch -p '\\.tmp$' --force
  xdu-rm -i /index/scratch --min-size 1G --safe"
)]
pub struct XduRmArgs {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR", env = "XDU_INDEX")]
    pub index: PathBuf,

    /// Regular expression pattern to match paths
    #[arg(short, long, value_name = "REGEX")]
    pub pattern: Option<String>,

    /// Filter by partition (user directory name)
    #[arg(short = 'u', long, value_name = "NAME")]
    pub partition: Option<String>,

    /// Minimum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub min_size: Option<String>,

    /// Maximum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    pub max_size: Option<String>,

    /// Files not accessed in N days
    #[arg(long, value_name = "DAYS")]
    pub older_than: Option<u64>,

    /// Files accessed within N days
    #[arg(long, value_name = "DAYS")]
    pub newer_than: Option<u64>,

    /// Limit number of files to delete
    #[arg(short, long)]
    pub limit: Option<usize>,

    /// Show what would be deleted without deleting
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Verify file metadata before deletion (re-stat to check atime/size)
    #[arg(long)]
    pub safe: bool,

    /// Skip confirmation prompt
    #[arg(short, long)]
    pub force: bool,

    /// Show detailed output for each file
    #[arg(short, long)]
    pub verbose: bool,

    /// Number of parallel threads for deletion
    #[arg(short, long, default_value = "4", env = "XDU_JOBS")]
    pub jobs: usize,
}
