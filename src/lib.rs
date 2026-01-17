//! Shared types and utilities for xdu tools.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};

/// A single file metadata record.
#[derive(Clone, Debug, PartialEq)]
pub struct FileRecord {
    pub path: String,
    pub size: i64,
    pub atime: i64,
}

/// Round size up to the nearest block boundary.
pub fn round_to_block(size: u64, block_size: u64) -> u64 {
    if block_size == 0 || size == 0 {
        return size;
    }
    size.div_ceil(block_size) * block_size
}

/// Determines how to calculate file size
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SizeMode {
    /// Use st_blocks * 512 (actual disk usage)
    DiskUsage,
    /// Use st_size (apparent/logical size)
    ApparentSize,
    /// Use st_size rounded up to block size
    BlockRounded(u64),
}

impl SizeMode {
    /// Calculate the size based on the mode.
    /// For DiskUsage, provide (blocks * 512, file_len).
    /// For ApparentSize and BlockRounded, only file_len is used.
    pub fn calculate(&self, disk_usage: u64, file_len: u64) -> u64 {
        match self {
            SizeMode::DiskUsage => disk_usage,
            SizeMode::ApparentSize => file_len,
            SizeMode::BlockRounded(block_size) => round_to_block(file_len, *block_size),
        }
    }
}

/// A work unit to crawl: directory path + display label
#[derive(Clone, Debug, PartialEq)]
pub struct CrawlUnit {
    pub path: PathBuf,
    pub label: String,
}

impl CrawlUnit {
    pub fn new(path: PathBuf, label: String) -> Self {
        Self { path, label }
    }
}

/// Parse a human-readable size string into bytes.
/// Supports suffixes: K, M, G, T (and KiB, MiB, GiB, TiB variants).
pub fn parse_size(s: &str) -> Result<i64, String> {
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
        .map_err(|_| format!("Invalid size: {}", s))?;
    Ok((num * mult as f64) as i64)
}

/// Sort mode for directory listings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SortMode {
    /// Alphabetical by name (directories first)
    #[default]
    Name,
    /// By total size, descending
    SizeDesc,
    /// By total size, ascending
    SizeAsc,
    /// By file count, descending
    CountDesc,
    /// By file count, ascending
    CountAsc,
    /// By age (oldest first - least recent access)
    AgeDesc,
    /// By age (newest first - most recent access)
    AgeAsc,
}

impl SortMode {
    /// All sort modes in display order.
    pub const ALL: [SortMode; 7] = [
        SortMode::Name,
        SortMode::SizeDesc,
        SortMode::SizeAsc,
        SortMode::CountDesc,
        SortMode::CountAsc,
        SortMode::AgeDesc,
        SortMode::AgeAsc,
    ];

    /// Returns the next sort mode in the cycle.
    pub fn next(self) -> Self {
        match self {
            SortMode::Name => SortMode::SizeDesc,
            SortMode::SizeDesc => SortMode::SizeAsc,
            SortMode::SizeAsc => SortMode::CountDesc,
            SortMode::CountDesc => SortMode::CountAsc,
            SortMode::CountAsc => SortMode::AgeDesc,
            SortMode::AgeDesc => SortMode::AgeAsc,
            SortMode::AgeAsc => SortMode::Name,
        }
    }

    /// Returns the previous sort mode in the cycle.
    pub fn prev(self) -> Self {
        match self {
            SortMode::Name => SortMode::AgeAsc,
            SortMode::SizeDesc => SortMode::Name,
            SortMode::SizeAsc => SortMode::SizeDesc,
            SortMode::CountDesc => SortMode::SizeAsc,
            SortMode::CountAsc => SortMode::CountDesc,
            SortMode::AgeDesc => SortMode::CountAsc,
            SortMode::AgeAsc => SortMode::AgeDesc,
        }
    }

    /// Returns the SQL ORDER BY clause for this sort mode.
    /// When sorting by Name, directories are grouped first.
    pub fn to_order_by(&self, dirs_first: bool) -> &'static str {
        match self {
            SortMode::Name if dirs_first => "bool_or(is_dir) DESC, component",
            SortMode::Name => "component",
            SortMode::SizeDesc => "total_size DESC",
            SortMode::SizeAsc => "total_size ASC",
            SortMode::CountDesc => "file_count DESC",
            SortMode::CountAsc => "file_count ASC",
            SortMode::AgeDesc => "latest_atime ASC",   // oldest first = smallest atime
            SortMode::AgeAsc => "latest_atime DESC",   // newest first = largest atime
        }
    }

    /// Returns the ORDER BY clause for partition listing.
    pub fn to_partition_order_by(&self) -> &'static str {
        match self {
            SortMode::Name => "partition",
            SortMode::SizeDesc => "total_size DESC",
            SortMode::SizeAsc => "total_size ASC",
            SortMode::CountDesc => "file_count DESC",
            SortMode::CountAsc => "file_count ASC",
            SortMode::AgeDesc => "latest_atime ASC",   // oldest first
            SortMode::AgeAsc => "latest_atime DESC",   // newest first
        }
    }
}

impl fmt::Display for SortMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortMode::Name => write!(f, "name"),
            SortMode::SizeDesc => write!(f, "size-desc"),
            SortMode::SizeAsc => write!(f, "size-asc"),
            SortMode::CountDesc => write!(f, "count-desc"),
            SortMode::CountAsc => write!(f, "count-asc"),
            SortMode::AgeDesc => write!(f, "age-desc"),
            SortMode::AgeAsc => write!(f, "age-asc"),
        }
    }
}

impl FromStr for SortMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "name" => Ok(SortMode::Name),
            "size-desc" | "size" => Ok(SortMode::SizeDesc),
            "size-asc" => Ok(SortMode::SizeAsc),
            "count-desc" | "count" => Ok(SortMode::CountDesc),
            "count-asc" => Ok(SortMode::CountAsc),
            "age-desc" | "age" | "oldest" => Ok(SortMode::AgeDesc),
            "age-asc" | "newest" => Ok(SortMode::AgeAsc),
            _ => Err(format!("Invalid sort mode: {}. Use: name, size-desc, size-asc, count-desc, count-asc, age-desc, age-asc", s)),
        }
    }
}

/// Query filters for file metadata searches.
#[derive(Clone, Debug, Default)]
pub struct QueryFilters {
    /// Regex pattern to match file paths.
    pub pattern: Option<String>,
    /// Minimum file size in bytes.
    pub min_size: Option<i64>,
    /// Maximum file size in bytes.
    pub max_size: Option<i64>,
    /// Files not accessed since this epoch timestamp.
    pub older_than: Option<i64>,
    /// Files accessed since this epoch timestamp.
    pub newer_than: Option<i64>,
}

impl QueryFilters {
    /// Create a new empty filter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set pattern filter from regex string.
    pub fn with_pattern(mut self, pattern: Option<String>) -> Self {
        self.pattern = pattern;
        self
    }

    /// Set minimum size filter from human-readable string (e.g., "1M").
    pub fn with_min_size(mut self, size: Option<&str>) -> Result<Self, String> {
        self.min_size = size.map(parse_size).transpose()?;
        Ok(self)
    }

    /// Set maximum size filter from human-readable string (e.g., "1G").
    pub fn with_max_size(mut self, size: Option<&str>) -> Result<Self, String> {
        self.max_size = size.map(parse_size).transpose()?;
        Ok(self)
    }

    /// Set older-than filter from days.
    pub fn with_older_than(mut self, days: Option<u64>) -> Self {
        if let Some(d) = days {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            self.older_than = Some(now - (d as i64 * 86400));
        }
        self
    }

    /// Set newer-than filter from days.
    pub fn with_newer_than(mut self, days: Option<u64>) -> Self {
        if let Some(d) = days {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            self.newer_than = Some(now - (d as i64 * 86400));
        }
        self
    }

    /// Returns true if any filter is active.
    pub fn is_active(&self) -> bool {
        self.pattern.is_some()
            || self.min_size.is_some()
            || self.max_size.is_some()
            || self.older_than.is_some()
            || self.newer_than.is_some()
    }

    /// Returns individual WHERE clause conditions.
    pub fn to_conditions(&self) -> Vec<String> {
        let mut conditions = Vec::new();

        if let Some(ref pattern) = self.pattern {
            let escaped = pattern.replace('\'', "''");
            conditions.push(format!("regexp_matches(path, '{}')", escaped));
        }

        if let Some(min_size) = self.min_size {
            conditions.push(format!("size >= {}", min_size));
        }

        if let Some(max_size) = self.max_size {
            conditions.push(format!("size <= {}", max_size));
        }

        if let Some(threshold) = self.older_than {
            conditions.push(format!("atime < {}", threshold));
        }

        if let Some(threshold) = self.newer_than {
            conditions.push(format!("atime >= {}", threshold));
        }

        conditions
    }

    /// Returns a WHERE clause string (without "WHERE" prefix).
    /// Returns empty string if no filters are active.
    pub fn to_where_clause(&self) -> String {
        let conditions = self.to_conditions();
        if conditions.is_empty() {
            String::new()
        } else {
            conditions.join(" AND ")
        }
    }

    /// Returns a full WHERE clause string (with "WHERE" prefix).
    /// Returns empty string if no filters are active.
    pub fn to_full_where_clause(&self) -> String {
        let clause = self.to_where_clause();
        if clause.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clause)
        }
    }

    /// Clear all filters.
    pub fn clear(&mut self) {
        self.pattern = None;
        self.min_size = None;
        self.max_size = None;
        self.older_than = None;
        self.newer_than = None;
    }

    /// Format active filters for display (e.g., "[older:30d] [min:1M]").
    pub fn format_display(&self) -> String {
        let mut parts = Vec::new();

        if let Some(ref pattern) = self.pattern {
            parts.push(format!("[/{}]", pattern));
        }

        if let Some(min_size) = self.min_size {
            parts.push(format!("[min:{}]", format_bytes(min_size as u64)));
        }

        if let Some(max_size) = self.max_size {
            parts.push(format!("[max:{}]", format_bytes(max_size as u64)));
        }

        if let Some(threshold) = self.older_than {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let days = (now - threshold) / 86400;
            parts.push(format!("[older:{}d]", days));
        }

        if let Some(threshold) = self.newer_than {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let days = (now - threshold) / 86400;
            parts.push(format!("[newer:{}d]", days));
        }

        parts.join(" ")
    }
}

/// Returns the Arrow schema for file metadata records.
pub fn get_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("atime", DataType::Int64, false),
    ]))
}

/// Format a count with human-readable suffixes (K, M, B).
pub fn format_count(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

/// Format bytes with binary suffixes (KiB, MiB, GiB, TiB).
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_count_units() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(1), "1");
        assert_eq!(format_count(999), "999");
    }

    #[test]
    fn test_format_count_thousands() {
        assert_eq!(format_count(1_000), "1.0K");
        assert_eq!(format_count(1_500), "1.5K");
        assert_eq!(format_count(999_999), "1000.0K");
    }

    #[test]
    fn test_format_count_millions() {
        assert_eq!(format_count(1_000_000), "1.0M");
        assert_eq!(format_count(2_500_000), "2.5M");
        assert_eq!(format_count(999_999_999), "1000.0M");
    }

    #[test]
    fn test_format_count_billions() {
        assert_eq!(format_count(1_000_000_000), "1.0B");
        assert_eq!(format_count(5_500_000_000), "5.5B");
    }

    #[test]
    fn test_format_bytes_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
    }

    #[test]
    fn test_format_bytes_kib() {
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1536), "1.50 KiB");
        assert_eq!(format_bytes(1024 * 1023), "1023.00 KiB");
    }

    #[test]
    fn test_format_bytes_mib() {
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 2 + 1024 * 512), "2.50 MiB");
    }

    #[test]
    fn test_format_bytes_gib() {
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 3 + 1024 * 1024 * 512), "3.50 GiB");
    }

    #[test]
    fn test_format_bytes_tib() {
        assert_eq!(format_bytes(1024_u64 * 1024 * 1024 * 1024), "1.00 TiB");
        assert_eq!(format_bytes(1024_u64 * 1024 * 1024 * 1024 * 2 + 1024_u64 * 1024 * 1024 * 512), "2.50 TiB");
    }

    #[test]
    fn test_schema_fields() {
        let schema = get_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "path");
        assert_eq!(schema.field(1).name(), "size");
        assert_eq!(schema.field(2).name(), "atime");
    }

    // SizeMode::calculate() tests
    #[test]
    fn test_size_mode_disk_usage() {
        let mode = SizeMode::DiskUsage;
        // disk_usage = 8192 (16 blocks * 512), file_len = 5000
        assert_eq!(mode.calculate(8192, 5000), 8192);
        assert_eq!(mode.calculate(0, 1000), 0);
        assert_eq!(mode.calculate(512, 100), 512);
    }

    #[test]
    fn test_size_mode_apparent_size() {
        let mode = SizeMode::ApparentSize;
        // Should always return file_len regardless of disk_usage
        assert_eq!(mode.calculate(8192, 5000), 5000);
        assert_eq!(mode.calculate(0, 1000), 1000);
        assert_eq!(mode.calculate(512, 100), 100);
    }

    #[test]
    fn test_size_mode_block_rounded() {
        // 4K block size
        let mode = SizeMode::BlockRounded(4096);
        // 5000 bytes rounds up to 8192 (2 blocks)
        assert_eq!(mode.calculate(8192, 5000), 8192);
        // 4096 exactly stays at 4096
        assert_eq!(mode.calculate(4096, 4096), 4096);
        // 1 byte rounds up to 4096
        assert_eq!(mode.calculate(512, 1), 4096);
        // 0 bytes stays 0
        assert_eq!(mode.calculate(0, 0), 0);
    }

    #[test]
    fn test_size_mode_block_rounded_various_sizes() {
        let mode = SizeMode::BlockRounded(1024); // 1K blocks
        assert_eq!(mode.calculate(0, 1), 1024);
        assert_eq!(mode.calculate(0, 1024), 1024);
        assert_eq!(mode.calculate(0, 1025), 2048);
        assert_eq!(mode.calculate(0, 2048), 2048);

        // 128K blocks (common HPC block size)
        let mode = SizeMode::BlockRounded(131072);
        assert_eq!(mode.calculate(0, 1), 131072);
        assert_eq!(mode.calculate(0, 131072), 131072);
        assert_eq!(mode.calculate(0, 131073), 262144);
    }

    // round_to_block() tests
    #[test]
    fn test_round_to_block_basic() {
        assert_eq!(round_to_block(0, 4096), 0);
        assert_eq!(round_to_block(1, 4096), 4096);
        assert_eq!(round_to_block(4096, 4096), 4096);
        assert_eq!(round_to_block(4097, 4096), 8192);
    }

    #[test]
    fn test_round_to_block_zero_block_size() {
        // Zero block size should return size unchanged
        assert_eq!(round_to_block(100, 0), 100);
        assert_eq!(round_to_block(0, 0), 0);
    }

    #[test]
    fn test_round_to_block_large_sizes() {
        // 1 MiB block size
        let mb = 1024 * 1024;
        assert_eq!(round_to_block(1, mb), mb);
        assert_eq!(round_to_block(mb, mb), mb);
        assert_eq!(round_to_block(mb + 1, mb), 2 * mb);
    }

    // CrawlUnit tests
    #[test]
    fn test_crawl_unit_stores_path_and_label() {
        let path = PathBuf::from("/data/users/alice");
        let label = "alice".to_string();
        let unit = CrawlUnit::new(path.clone(), label.clone());

        assert_eq!(unit.path, path);
        assert_eq!(unit.label, label);
    }

    #[test]
    fn test_crawl_unit_with_subdirectory_label() {
        let path = PathBuf::from("/data/users/alice/projects");
        let label = "alice:projects".to_string();
        let unit = CrawlUnit::new(path.clone(), label.clone());

        assert_eq!(unit.path, PathBuf::from("/data/users/alice/projects"));
        assert_eq!(unit.label, "alice:projects");
    }

    #[test]
    fn test_crawl_unit_equality() {
        let unit1 = CrawlUnit::new(
            PathBuf::from("/data/users/alice"),
            "alice".to_string(),
        );
        let unit2 = CrawlUnit::new(
            PathBuf::from("/data/users/alice"),
            "alice".to_string(),
        );
        let unit3 = CrawlUnit::new(
            PathBuf::from("/data/users/bob"),
            "bob".to_string(),
        );

        assert_eq!(unit1, unit2);
        assert_ne!(unit1, unit3);
    }

    #[test]
    fn test_crawl_unit_root_marker() {
        // Test the pattern used for loose files in partition root
        let path = PathBuf::from("/data/users/alice");
        let label = "alice:.".to_string();
        let unit = CrawlUnit::new(path, label);

        assert_eq!(unit.label, "alice:.");
    }

    // FileRecord tests
    #[test]
    fn test_file_record_creation() {
        let record = FileRecord {
            path: "/data/users/alice/file.txt".to_string(),
            size: 1024,
            atime: 1700000000,
        };

        assert_eq!(record.path, "/data/users/alice/file.txt");
        assert_eq!(record.size, 1024);
        assert_eq!(record.atime, 1700000000);
    }

    #[test]
    fn test_file_record_equality() {
        let record1 = FileRecord {
            path: "/data/file.txt".to_string(),
            size: 100,
            atime: 1000,
        };
        let record2 = FileRecord {
            path: "/data/file.txt".to_string(),
            size: 100,
            atime: 1000,
        };
        let record3 = FileRecord {
            path: "/data/other.txt".to_string(),
            size: 100,
            atime: 1000,
        };

        assert_eq!(record1, record2);
        assert_ne!(record1, record3);
    }

    #[test]
    fn test_file_record_clone() {
        let record = FileRecord {
            path: "/data/file.txt".to_string(),
            size: 2048,
            atime: 1600000000,
        };
        let cloned = record.clone();

        assert_eq!(record, cloned);
    }

    // parse_size() tests
    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("100B").unwrap(), 100);
        assert_eq!(parse_size("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_size_kilobytes() {
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_size("2.5K").unwrap(), 2560);
    }

    #[test]
    fn test_parse_size_megabytes() {
        assert_eq!(parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("10M").unwrap(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gigabytes() {
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_terabytes() {
        assert_eq!(parse_size("1T").unwrap(), 1024_i64 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1TiB").unwrap(), 1024_i64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1k").unwrap(), 1024);
        assert_eq!(parse_size("1m").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("K").is_err());
    }

    // SortMode tests
    #[test]
    fn test_sort_mode_default() {
        let mode = SortMode::default();
        assert_eq!(mode, SortMode::Name);
    }

    #[test]
    fn test_sort_mode_cycle() {
        assert_eq!(SortMode::Name.next(), SortMode::SizeDesc);
        assert_eq!(SortMode::SizeDesc.next(), SortMode::SizeAsc);
        assert_eq!(SortMode::SizeAsc.next(), SortMode::CountDesc);
        assert_eq!(SortMode::CountDesc.next(), SortMode::CountAsc);
        assert_eq!(SortMode::CountAsc.next(), SortMode::AgeDesc);
        assert_eq!(SortMode::AgeDesc.next(), SortMode::AgeAsc);
        assert_eq!(SortMode::AgeAsc.next(), SortMode::Name);
    }

    #[test]
    fn test_sort_mode_from_str() {
        assert_eq!("name".parse::<SortMode>().unwrap(), SortMode::Name);
        assert_eq!("size-desc".parse::<SortMode>().unwrap(), SortMode::SizeDesc);
        assert_eq!("size".parse::<SortMode>().unwrap(), SortMode::SizeDesc);
        assert_eq!("size-asc".parse::<SortMode>().unwrap(), SortMode::SizeAsc);
        assert_eq!("count-desc".parse::<SortMode>().unwrap(), SortMode::CountDesc);
        assert_eq!("count".parse::<SortMode>().unwrap(), SortMode::CountDesc);
        assert_eq!("count-asc".parse::<SortMode>().unwrap(), SortMode::CountAsc);
        assert_eq!("age-desc".parse::<SortMode>().unwrap(), SortMode::AgeDesc);
        assert_eq!("age".parse::<SortMode>().unwrap(), SortMode::AgeDesc);
        assert_eq!("oldest".parse::<SortMode>().unwrap(), SortMode::AgeDesc);
        assert_eq!("age-asc".parse::<SortMode>().unwrap(), SortMode::AgeAsc);
        assert_eq!("newest".parse::<SortMode>().unwrap(), SortMode::AgeAsc);
    }

    #[test]
    fn test_sort_mode_from_str_invalid() {
        assert!("invalid".parse::<SortMode>().is_err());
    }

    #[test]
    fn test_sort_mode_display() {
        assert_eq!(SortMode::Name.to_string(), "name");
        assert_eq!(SortMode::SizeDesc.to_string(), "size-desc");
        assert_eq!(SortMode::SizeAsc.to_string(), "size-asc");
        assert_eq!(SortMode::CountDesc.to_string(), "count-desc");
        assert_eq!(SortMode::CountAsc.to_string(), "count-asc");
        assert_eq!(SortMode::AgeDesc.to_string(), "age-desc");
        assert_eq!(SortMode::AgeAsc.to_string(), "age-asc");
    }

    #[test]
    fn test_sort_mode_order_by() {
        assert_eq!(SortMode::Name.to_order_by(true), "bool_or(is_dir) DESC, component");
        assert_eq!(SortMode::Name.to_order_by(false), "component");
        assert_eq!(SortMode::SizeDesc.to_order_by(true), "total_size DESC");
        assert_eq!(SortMode::SizeAsc.to_order_by(false), "total_size ASC");
        assert_eq!(SortMode::AgeDesc.to_order_by(false), "latest_atime ASC");
        assert_eq!(SortMode::AgeAsc.to_order_by(false), "latest_atime DESC");
    }

    // QueryFilters tests
    #[test]
    fn test_query_filters_empty() {
        let filters = QueryFilters::new();
        assert!(!filters.is_active());
        assert_eq!(filters.to_where_clause(), "");
        assert_eq!(filters.to_full_where_clause(), "");
    }

    #[test]
    fn test_query_filters_pattern() {
        let filters = QueryFilters::new().with_pattern(Some("\\.py$".to_string()));
        assert!(filters.is_active());
        assert!(filters.to_where_clause().contains("regexp_matches"));
        assert!(filters.to_where_clause().contains(".py$"));
    }

    #[test]
    fn test_query_filters_size() {
        let filters = QueryFilters::new()
            .with_min_size(Some("1M")).unwrap()
            .with_max_size(Some("1G")).unwrap();
        assert!(filters.is_active());
        let clause = filters.to_where_clause();
        assert!(clause.contains("size >= 1048576"));
        assert!(clause.contains("size <= 1073741824"));
    }

    #[test]
    fn test_query_filters_combined() {
        let filters = QueryFilters::new()
            .with_pattern(Some("test".to_string()))
            .with_min_size(Some("1K")).unwrap();
        let clause = filters.to_where_clause();
        assert!(clause.contains("AND"));
        assert!(clause.contains("regexp_matches"));
        assert!(clause.contains("size >= 1024"));
    }

    #[test]
    fn test_query_filters_clear() {
        let mut filters = QueryFilters::new()
            .with_pattern(Some("test".to_string()))
            .with_min_size(Some("1K")).unwrap();
        assert!(filters.is_active());
        filters.clear();
        assert!(!filters.is_active());
    }

    #[test]
    fn test_query_filters_full_where_clause() {
        let filters = QueryFilters::new()
            .with_min_size(Some("1M")).unwrap();
        let clause = filters.to_full_where_clause();
        assert!(clause.starts_with("WHERE "));
    }
}
