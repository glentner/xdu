use std::io::{self, Write};

use anyhow::{Context, Result};
use clap::Parser;
use duckdb::Connection;

use xdu::cli::XduFindArgs;
use xdu::QueryFilters;

fn main() -> Result<()> {
    let args = XduFindArgs::parse();

    // Resolve index path
    let index_path = args.index.canonicalize()
        .with_context(|| format!("Index directory not found: {}", args.index.display()))?;

    // Build the glob pattern for Parquet files
    // Partitions are direct children of the index directory
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

    // Build and execute query based on format
    let stdout = io::stdout();
    let mut out = stdout.lock();

    // Handle --top mode: show top N partitions by file count
    if let Some(n) = args.top {
        // Extract partition name from the path (parent directory of the parquet file)
        // The glob pattern is index/*/*.parquet, so we extract the partition from the path
        let sql = format!(
            "SELECT 
                regexp_extract(filename, '.*/([^/]+)/[^/]+\\.parquet$', 1) as partition,
                COUNT(*) as file_count
            FROM read_parquet('{}', filename=true) {}
            GROUP BY partition
            ORDER BY file_count DESC
            LIMIT {}",
            glob_pattern, where_clause, n
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let partition: String = row.get(0)?;
            writeln!(out, "{}", partition)?;
        }
        return Ok(());
    }

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
