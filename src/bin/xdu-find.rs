use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use duckdb::Connection;

#[derive(Parser, Debug)]
#[command(name = "xdu-find", about = "Query a file metadata index for matching paths")]
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

    /// Output format: path (default), size, atime, csv, json
    #[arg(short, long, default_value = "path")]
    format: String,

    /// Limit number of results
    #[arg(short, long)]
    limit: Option<usize>,

    /// Count matching records instead of listing them
    #[arg(short, long)]
    count: bool,
}

/// Parse a human-readable size string into bytes.
fn parse_size(s: &str) -> Result<i64> {
    let s = s.trim().to_uppercase();
    let (num, mult) = if let Some(n) = s.strip_suffix("TIB") {
        (n, 1024_i64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("T") {
        (n, 1024_i64 * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("GIB") {
        (n, 1024_i64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("G") {
        (n, 1024_i64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MIB") {
        (n, 1024_i64 * 1024)
    } else if let Some(n) = s.strip_suffix("M") {
        (n, 1024_i64 * 1024)
    } else if let Some(n) = s.strip_suffix("KIB") {
        (n, 1024_i64)
    } else if let Some(n) = s.strip_suffix("K") {
        (n, 1024_i64)
    } else if let Some(n) = s.strip_suffix("B") {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };

    let num: f64 = num.trim().parse()
        .with_context(|| format!("Invalid size: {}", s))?;
    Ok((num * mult as f64) as i64)
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Resolve index path
    let index_path = args.index.canonicalize()
        .with_context(|| format!("Index directory not found: {}", args.index.display()))?;

    // Build the glob pattern for Parquet files
    let glob_pattern = if let Some(ref partition) = args.partition {
        format!("{}/**/{}/**/*.parquet", index_path.display(), partition)
    } else {
        format!("{}/**/*.parquet", index_path.display())
    };

    // Connect to DuckDB (in-memory)
    let conn = Connection::open_in_memory()?;

    // Build WHERE clauses
    let mut conditions = Vec::new();

    if let Some(ref pattern) = args.pattern {
        // Escape single quotes in the pattern
        let escaped = pattern.replace('\'', "''");
        conditions.push(format!("regexp_matches(path, '{}')", escaped));
    }

    if let Some(ref min_size) = args.min_size {
        let bytes = parse_size(min_size)?;
        conditions.push(format!("size >= {}", bytes));
    }

    if let Some(ref max_size) = args.max_size {
        let bytes = parse_size(max_size)?;
        conditions.push(format!("size <= {}", bytes));
    }

    // Calculate atime thresholds
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    if let Some(days) = args.older_than {
        let threshold = now - (days as i64 * 86400);
        conditions.push(format!("atime < {}", threshold));
    }

    if let Some(days) = args.newer_than {
        let threshold = now - (days as i64 * 86400);
        conditions.push(format!("atime >= {}", threshold));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let limit_clause = if let Some(n) = args.limit {
        format!("LIMIT {}", n)
    } else {
        String::new()
    };

    // Build and execute query based on format
    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Handle count mode
    if args.count {
        let sql = format!(
            "SELECT COUNT(*) FROM read_parquet('{}') {}",
            glob_pattern, where_clause
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let count: i64 = row.get(0)?;
            writeln!(out, "{}", count)?;
        }
        return Ok(());
    }

    match args.format.as_str() {
        "path" => {
            let sql = format!(
                "SELECT path FROM read_parquet('{}') {} {}",
                glob_pattern, where_clause, limit_clause
            );
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                writeln!(out, "{}", path)?;
            }
        }
        "size" => {
            let sql = format!(
                "SELECT path, size FROM read_parquet('{}') {} ORDER BY size DESC {}",
                glob_pattern, where_clause, limit_clause
            );
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let size: i64 = row.get(1)?;
                writeln!(out, "{}\t{}", size, path)?;
            }
        }
        "atime" => {
            let sql = format!(
                "SELECT path, atime FROM read_parquet('{}') {} ORDER BY atime ASC {}",
                glob_pattern, where_clause, limit_clause
            );
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let atime: i64 = row.get(1)?;
                writeln!(out, "{}\t{}", atime, path)?;
            }
        }
        "csv" => {
            let sql = format!(
                "SELECT path, size, atime FROM read_parquet('{}') {} {}",
                glob_pattern, where_clause, limit_clause
            );
            writeln!(out, "path,size,atime")?;
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let size: i64 = row.get(1)?;
                let atime: i64 = row.get(2)?;
                // Escape commas and quotes in path for CSV
                if path.contains(',') || path.contains('"') {
                    writeln!(out, "\"{}\",{},{}", path.replace('"', "\"\""), size, atime)?;
                } else {
                    writeln!(out, "{},{},{}", path, size, atime)?;
                }
            }
        }
        "json" => {
            let sql = format!(
                "SELECT path, size, atime FROM read_parquet('{}') {} {}",
                glob_pattern, where_clause, limit_clause
            );
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query([])?;
            let mut first = true;
            writeln!(out, "[")?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let size: i64 = row.get(1)?;
                let atime: i64 = row.get(2)?;
                if !first {
                    writeln!(out, ",")?;
                }
                first = false;
                // Escape JSON special characters in path
                let escaped_path = path
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                write!(out, "  {{\"path\":\"{}\",\"size\":{},\"atime\":{}}}", escaped_path, size, atime)?;
            }
            writeln!(out)?;
            writeln!(out, "]")?;
        }
        _ => {
            anyhow::bail!("Unknown format: {}. Use: path, size, atime, csv, json", args.format);
        }
    }

    Ok(())
}
