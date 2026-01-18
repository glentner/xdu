use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use duckdb::Connection;

use xdu::{parse_size, QueryFilters};

#[derive(Parser, Debug)]
#[command(name = "xdu-rm", about = "Delete files matching index query criteria")]
struct Args {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR")]
    index: PathBuf,

    /// Regular expression pattern to match paths
    #[arg(short, long, value_name = "REGEX")]
    pattern: Option<String>,

    /// Filter by partition (user directory name)
    #[arg(short = 'u', long, value_name = "NAME")]
    partition: Option<String>,

    /// Minimum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    min_size: Option<String>,

    /// Maximum file size (e.g., 1K, 10M, 1G)
    #[arg(long, value_name = "SIZE")]
    max_size: Option<String>,

    /// Files not accessed in N days
    #[arg(long, value_name = "DAYS")]
    older_than: Option<u64>,

    /// Files accessed within N days
    #[arg(long, value_name = "DAYS")]
    newer_than: Option<u64>,

    /// Limit number of files to delete
    #[arg(short, long)]
    limit: Option<usize>,

    /// Show what would be deleted without deleting
    #[arg(short = 'n', long)]
    dry_run: bool,

    /// Verify file metadata before deletion (re-stat to check atime/size)
    #[arg(long)]
    safe: bool,

    /// Skip confirmation prompt
    #[arg(short, long)]
    force: bool,

    /// Show detailed output for each file
    #[arg(short, long)]
    verbose: bool,
}

/// File info from the index query
#[allow(dead_code)]
struct FileInfo {
    path: String,
    size: i64,
    atime: i64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Resolve index path
    let index_path = args.index.canonicalize()
        .with_context(|| format!("Index directory not found: {}", args.index.display()))?;

    // Build the glob pattern for Parquet files
    let glob_pattern = if let Some(ref partition) = args.partition {
        format!("{}/{}/*.parquet", index_path.display(), partition)
    } else {
        format!("{}/*/*.parquet", index_path.display())
    };

    // Connect to DuckDB (in-memory)
    let conn = Connection::open_in_memory()?;

    // Build filters using shared QueryFilters
    let filters = QueryFilters::new()
        .with_pattern(args.pattern.clone())
        .with_older_than(args.older_than)
        .with_newer_than(args.newer_than)
        .with_min_size(args.min_size.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?
        .with_max_size(args.max_size.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?;

    let where_clause = filters.to_full_where_clause();

    let limit_clause = if let Some(n) = args.limit {
        format!("LIMIT {}", n)
    } else {
        String::new()
    };

    // Query for matching files
    let sql = format!(
        "SELECT path, size, atime FROM read_parquet('{}') {} {}",
        glob_pattern, where_clause, limit_clause
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;

    let mut files: Vec<FileInfo> = Vec::new();
    while let Some(row) = rows.next()? {
        files.push(FileInfo {
            path: row.get(0)?,
            size: row.get(1)?,
            atime: row.get(2)?,
        });
    }

    if files.is_empty() {
        println!("No matching files found.");
        return Ok(());
    }

    // Dry run mode: just print paths
    if args.dry_run {
        for file in &files {
            println!("{}", file.path);
        }
        println!("\n{} file(s) would be deleted.", files.len());
        return Ok(());
    }

    // Confirmation prompt
    if !args.force {
        print!("Delete {} file(s)? [y/N] ", files.len());
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input != "y" && input != "yes" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Compute thresholds for safe mode
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let atime_threshold = args.older_than.map(|days| now - (days as i64 * 86400));
    let max_size_bytes = args.max_size.as_deref()
        .map(|s| parse_size(s))
        .transpose()
        .map_err(|e| anyhow::anyhow!(e))?;

    // Delete files
    let mut deleted = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;
    let mut missing = 0u64;

    for file in &files {
        let path = std::path::Path::new(&file.path);

        // Safe mode: re-stat the file and verify conditions
        if args.safe {
            match fs::metadata(path) {
                Ok(meta) => {
                    // Check atime if --older-than was specified
                    if let Some(threshold) = atime_threshold {
                        let current_atime = meta.atime();
                        if current_atime >= threshold {
                            if args.verbose {
                                println!("SKIP (accessed since index): {}", file.path);
                            }
                            skipped += 1;
                            continue;
                        }
                    }

                    // Check size if --max-size was specified
                    if let Some(max_size) = max_size_bytes {
                        let current_size = meta.len() as i64;
                        if current_size > max_size {
                            if args.verbose {
                                println!("SKIP (size changed): {}", file.path);
                            }
                            skipped += 1;
                            continue;
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    if args.verbose {
                        println!("SKIP (not found): {}", file.path);
                    }
                    missing += 1;
                    continue;
                }
                Err(e) => {
                    if args.verbose {
                        eprintln!("FAIL (stat error): {}: {}", file.path, e);
                    }
                    failed += 1;
                    continue;
                }
            }
        }

        // Attempt deletion
        match fs::remove_file(path) {
            Ok(()) => {
                if args.verbose {
                    println!("DELETE: {}", file.path);
                }
                deleted += 1;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if args.verbose {
                    println!("SKIP (not found): {}", file.path);
                }
                missing += 1;
            }
            Err(e) => {
                if args.verbose {
                    eprintln!("FAIL: {}: {}", file.path, e);
                }
                failed += 1;
            }
        }
    }

    // Print summary
    println!("\nDeleted: {}", deleted);
    if missing > 0 {
        println!("Missing: {}", missing);
    }
    if skipped > 0 {
        println!("Skipped (safe mode): {}", skipped);
    }
    if failed > 0 {
        println!("Failed: {}", failed);
    }

    Ok(())
}
