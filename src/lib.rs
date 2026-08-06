//! Shared types and utilities for xdu tools.

pub mod cli;
pub mod crawl;

use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};

/// Partition name reserved for files lying directly in the indexed root.
///
/// Part of the on-disk layout contract rather than of the crawl, so the writer and every
/// reader name it from one place: two copies of this string could drift apart and quietly
/// break the depth-1 partition.
pub const ROOT_PARTITION: &str = "__root__";

/// Run-level completion marker written at the index root when a crawl succeeds.
///
/// A dotfile, so the readers' `*/*.parquet` glob never mistakes it for a partition. The
/// reverse direction — a partition directory landing *on* the marker path — is guarded by
/// this name's membership in `RESERVED_INDEX_NAMES`.
pub const COMPLETION_MARKER: &str = ".xdu-complete";

/// Every name the index root already claims, paired with what claims it.
///
/// `<index>/` holds exactly two kinds of entry: partition directories named after the
/// source tree's top-level subdirectories, and the reserved `COMPLETION_MARKER` dotfile.
/// A top-level source directory whose name is already claimed would be written *as* that
/// entry — clobbering the synthetic loose-file partition's chunk ids, or occupying the
/// marker path so the run cannot attest itself and no later run can clear it. So
/// `crawl::build_work_queue` rejects one, naming what the collision is with.
///
/// The guard iterates this list rather than testing names one by one, so reserving a new
/// name at the index root extends the rejection by construction: the collision class stays
/// closed instead of depending on someone remembering the second half of it.
pub const RESERVED_INDEX_NAMES: &[(&str, &str)] = &[
    (
        ROOT_PARTITION,
        "the partition holding loose top-level files",
    ),
    (COMPLETION_MARKER, "the run-completion marker"),
];

/// Most a reader will ever read from a completion marker. The writer emits ~100 bytes; the
/// cap exists so a reader cannot be made to pull an arbitrarily large file into memory by
/// whatever left a file of that name in a group-writable index directory.
const MARKER_READ_LIMIT: u64 = 64 * 1024;

/// The tolerated-error count a completion marker records, if its body states one.
///
/// `None` for a missing or unparseable `errors=` key. An unrecognized marker body says
/// nothing about errors, so a reader stays exactly as quiet as it was before the marker
/// existed rather than inventing a warning out of a format it does not understand. The
/// first key trimming to `errors` decides the answer.
pub fn completion_marker_errors(body: &str) -> Option<u64> {
    for line in body.lines() {
        if let Some((key, value)) = line.split_once('=')
            && key.trim() == "errors"
        {
            return value.trim().parse().ok();
        }
    }
    None
}

/// The `read_parquet` glob for an index, optionally scoped to a single partition.
///
/// Every reader goes through here, so the index layout (`<index>/<partition>/*.parquet`)
/// is expressed once. It is also the single seam where index paths and partition names
/// reach SQL, which is where escaping belongs when it is added.
pub fn index_glob(index: &Path, partition: Option<&str>) -> String {
    match partition {
        Some(partition) => format!("{}/{}/*.parquet", index.display(), partition),
        None => format!("{}/*/*.parquet", index.display()),
    }
}

/// A warning to print when an index cannot be trusted to be complete, else `None`.
///
/// Two ways an index falls short. It may carry **no marker**: the crawler writes one only
/// when a run finishes, so absence means a failed or interrupted run — or an index that
/// predates the marker entirely. Or it may carry a marker that records **tolerated errors**:
/// an `xdu --allow-errors` run finishes and is marked complete, yet knowingly skipped
/// whatever it could not read. That second case matters most to `xdu-rm`, whose risk is
/// precisely the files an index does not know about, and the operator running it weeks later
/// never gave the consent the build-time `--allow-errors` expressed.
///
/// Readers warn and carry on rather than refusing: every index built before the marker
/// existed is still perfectly queryable, and breaking those would be worse than the risk
/// being flagged.
pub fn index_completion_warning(index: &Path) -> Option<String> {
    let marker = index.join(COMPLETION_MARKER);

    // One `stat`, not two: `Path::exists()` *is* `metadata().is_ok()`, so an absent marker
    // behaves exactly as it did before this function read bodies at all.
    let meta = match fs::metadata(&marker) {
        Ok(meta) => meta,
        Err(_) => {
            return Some(format!(
                "warning: {} has no completion marker ({}); it may be from an interrupted \
                 run or predate the marker, so results may be incomplete",
                index.display(),
                COMPLETION_MARKER
            ));
        }
    };

    // Presence alone already attests that the run finished; the body only ever adds detail.
    // So consulting it must never cost more than not consulting it: opening a FIFO, socket
    // or device node of this name would block the reader forever, and an oversized file
    // would be pulled into memory. Neither is worth a detail, so neither is opened.
    if !meta.is_file() || meta.len() > MARKER_READ_LIMIT {
        return None;
    }

    // Anything unreadable here — a permission change, non-UTF-8 bytes, or the marker being
    // deleted since the stat above — degrades to an empty body and therefore to silence,
    // which is the behavior a marker-present index had before.
    let body = fs::read_to_string(&marker).unwrap_or_default();

    match completion_marker_errors(&body) {
        Some(errors) if errors > 0 => Some(format!(
            "warning: {} was indexed with {} tolerated error(s) (xdu --allow-errors); the \
             affected paths were skipped, so results may be incomplete",
            index.display(),
            errors
        )),
        _ => None,
    }
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

    let num: f64 = num
        .trim()
        .parse()
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
            SortMode::AgeDesc => "latest_atime ASC", // oldest first = smallest atime
            SortMode::AgeAsc => "latest_atime DESC", // newest first = largest atime
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
            SortMode::AgeDesc => "latest_atime ASC", // oldest first
            SortMode::AgeAsc => "latest_atime DESC", // newest first
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
            _ => Err(format!(
                "Invalid sort mode: {}. Use: name, size-desc, size-asc, count-desc, count-asc, age-desc, age-asc",
                s
            )),
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

/// Build the deterministic `ORDER BY path LIMIT n` tail for a capped query.
///
/// A bare `LIMIT` returns an arbitrary, unstable subset, so ordering and limiting are
/// inseparable: whenever a limit is present the query orders by `path` — the unique key of
/// the index — so a `--dry-run` preview and the subsequent real deletion select identical
/// rows. With no limit the tail is empty: every match is acted on regardless of order, and
/// ordering the whole match set would be wasted work.
pub fn deterministic_limit_clause(limit: Option<usize>) -> String {
    match limit {
        Some(n) => format!("ORDER BY path LIMIT {n}"),
        None => String::new(),
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

/// Format crawl speed as human-readable files/s.
pub fn format_speed(files_per_sec: f64) -> String {
    if files_per_sec >= 1_000_000.0 {
        format!("{:.1}M files/s", files_per_sec / 1_000_000.0)
    } else if files_per_sec >= 1_000.0 {
        format!("{:.1}k files/s", files_per_sec / 1_000.0)
    } else {
        format!("{:.0} files/s", files_per_sec)
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
    fn test_format_speed_low() {
        assert_eq!(format_speed(0.0), "0 files/s");
        assert_eq!(format_speed(1.0), "1 files/s");
        assert_eq!(format_speed(500.0), "500 files/s");
        assert_eq!(format_speed(999.0), "999 files/s");
    }

    #[test]
    fn test_format_speed_thousands() {
        assert_eq!(format_speed(1_000.0), "1.0k files/s");
        assert_eq!(format_speed(42_500.0), "42.5k files/s");
        assert_eq!(format_speed(127_000.0), "127.0k files/s");
        assert_eq!(format_speed(999_999.0), "1000.0k files/s");
    }

    #[test]
    fn test_format_speed_millions() {
        assert_eq!(format_speed(1_000_000.0), "1.0M files/s");
        assert_eq!(format_speed(2_500_000.0), "2.5M files/s");
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
        assert_eq!(
            format_bytes(1024 * 1024 * 1024 * 3 + 1024 * 1024 * 512),
            "3.50 GiB"
        );
    }

    #[test]
    fn test_format_bytes_tib() {
        assert_eq!(format_bytes(1024_u64 * 1024 * 1024 * 1024), "1.00 TiB");
        assert_eq!(
            format_bytes(1024_u64 * 1024 * 1024 * 1024 * 2 + 1024_u64 * 1024 * 1024 * 512),
            "2.50 TiB"
        );
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
        assert_eq!(
            "count-desc".parse::<SortMode>().unwrap(),
            SortMode::CountDesc
        );
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
        assert_eq!(
            SortMode::Name.to_order_by(true),
            "bool_or(is_dir) DESC, component"
        );
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
            .with_min_size(Some("1M"))
            .unwrap()
            .with_max_size(Some("1G"))
            .unwrap();
        assert!(filters.is_active());
        let clause = filters.to_where_clause();
        assert!(clause.contains("size >= 1048576"));
        assert!(clause.contains("size <= 1073741824"));
    }

    #[test]
    fn test_query_filters_combined() {
        let filters = QueryFilters::new()
            .with_pattern(Some("test".to_string()))
            .with_min_size(Some("1K"))
            .unwrap();
        let clause = filters.to_where_clause();
        assert!(clause.contains("AND"));
        assert!(clause.contains("regexp_matches"));
        assert!(clause.contains("size >= 1024"));
    }

    #[test]
    fn test_query_filters_clear() {
        let mut filters = QueryFilters::new()
            .with_pattern(Some("test".to_string()))
            .with_min_size(Some("1K"))
            .unwrap();
        assert!(filters.is_active());
        filters.clear();
        assert!(!filters.is_active());
    }

    #[test]
    fn test_query_filters_full_where_clause() {
        let filters = QueryFilters::new().with_min_size(Some("1M")).unwrap();
        let clause = filters.to_full_where_clause();
        assert!(clause.starts_with("WHERE "));
    }

    // deterministic_limit_clause() tests
    #[test]
    fn test_deterministic_limit_clause_none() {
        assert_eq!(deterministic_limit_clause(None), "");
    }

    #[test]
    fn test_deterministic_limit_clause_some() {
        assert_eq!(deterministic_limit_clause(Some(1)), "ORDER BY path LIMIT 1");
        assert_eq!(deterministic_limit_clause(Some(5)), "ORDER BY path LIMIT 5");
    }

    // index layout: the glob every reader shares

    #[test]
    fn test_index_glob_all_partitions() {
        assert_eq!(
            index_glob(Path::new("/index/scratch"), None),
            "/index/scratch/*/*.parquet"
        );
    }

    #[test]
    fn test_index_glob_single_partition() {
        assert_eq!(
            index_glob(Path::new("/index/scratch"), Some("alice")),
            "/index/scratch/alice/*.parquet"
        );
        // The reserved loose-file partition is addressed like any other.
        assert_eq!(
            index_glob(Path::new("/index/scratch"), Some(ROOT_PARTITION)),
            "/index/scratch/__root__/*.parquet"
        );
    }

    #[test]
    fn test_index_completion_warning() {
        let dir = tempfile::TempDir::new().unwrap();
        let index = dir.path();

        // No marker: a reader should say so, naming the index and the marker.
        let warning = index_completion_warning(index).expect("markerless index must warn");
        assert!(warning.contains(&index.display().to_string()));
        assert!(warning.contains(COMPLETION_MARKER));

        // Marker present: silence.
        std::fs::write(index.join(COMPLETION_MARKER), "xdu=test\n").unwrap();
        assert_eq!(index_completion_warning(index), None);

        // A marker recording tolerated errors is present but still not trustworthy: warn, and
        // do it without the substring the markerless case owns so the two stay distinguishable.
        std::fs::write(index.join(COMPLETION_MARKER), "xdu=test\nerrors=2\n").unwrap();
        let warning = index_completion_warning(index).expect("a tolerated-error index must warn");
        assert!(warning.contains(&index.display().to_string()));
        assert!(warning.contains("2 tolerated error(s)"));
        assert!(warning.contains("--allow-errors"));
        assert!(
            !warning.contains("completion marker"),
            "must stay distinguishable from the markerless warning: {warning}"
        );

        // A clean run records zero, which is nothing to report.
        std::fs::write(index.join(COMPLETION_MARKER), "xdu=test\nerrors=0\n").unwrap();
        assert_eq!(index_completion_warning(index), None);

        // Something of that name that is not a file still attests presence, and offers no
        // body worth reading.
        std::fs::remove_file(index.join(COMPLETION_MARKER)).unwrap();
        std::fs::create_dir(index.join(COMPLETION_MARKER)).unwrap();
        assert_eq!(index_completion_warning(index), None);
    }

    #[test]
    fn test_completion_marker_errors_reads_the_writers_body() {
        // Writer and reader pinned by one test: if `completion_marker_contents` ever renames
        // or reformats the key, this fails loudly instead of the readers going quietly silent.
        let body = crawl::completion_marker_contents(
            &crawl::CrawlStats {
                errors: 3,
                ..Default::default()
            },
            1_700_000_000,
        );
        assert_eq!(completion_marker_errors(&body), Some(3));
        assert_eq!(
            completion_marker_errors(&crawl::completion_marker_contents(
                &crawl::CrawlStats::default(),
                1_700_000_000
            )),
            Some(0)
        );

        // Nothing to say: no key, no body, or a format this version does not understand.
        assert_eq!(completion_marker_errors(""), None);
        assert_eq!(completion_marker_errors("xdu=0.4.1\nfiles=10\n"), None);
        assert_eq!(completion_marker_errors("errors=garbage\n"), None);
        assert_eq!(completion_marker_errors("errors=-1\n"), None);
        assert_eq!(completion_marker_errors("errors=\n"), None);
        assert_eq!(completion_marker_errors("no separator here\n"), None);

        // Stray whitespace, a missing trailing newline, and CRLF all still parse.
        assert_eq!(completion_marker_errors("errors=4"), Some(4));
        assert_eq!(completion_marker_errors("errors= 5 \n"), Some(5));
        assert_eq!(completion_marker_errors("files=1\r\nerrors=6\r\n"), Some(6));

        // The first key trimming to `errors` decides the answer.
        assert_eq!(completion_marker_errors("errors=1\nerrors=9\n"), Some(1));
    }

    #[test]
    fn test_index_completion_warning_does_not_block_on_a_fifo_marker() {
        use std::os::unix::ffi::OsStrExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tempfile::TempDir::new().unwrap();
        let marker = dir.path().join(COMPLETION_MARKER);

        // Opening a FIFO read-only blocks until a writer appears, so reading the body without
        // first checking the file type would hang every reader forever. An index directory on
        // shared scratch is routinely group-writable, so this is reachable, not theoretical.
        let c_path = std::ffi::CString::new(marker.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) }, 0);

        let index = dir.path().to_path_buf();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(index_completion_warning(&index));
        });

        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(result) => assert_eq!(
                result, None,
                "a FIFO marker attests presence and yields no body"
            ),
            Err(_) => panic!(
                "index_completion_warning blocked on a FIFO marker — the file-type guard is gone"
            ),
        }
    }

    #[test]
    fn test_index_completion_warning_ignores_an_oversized_marker() {
        let dir = tempfile::TempDir::new().unwrap();
        let index = dir.path();

        // This body does record errors, but a reader must not pull 64 KiB+ into memory to
        // find that out — the size guard wins over the key.
        let mut body = String::from("errors=7\n");
        body.push_str(&"x".repeat(MARKER_READ_LIMIT as usize + 1));
        std::fs::write(index.join(COMPLETION_MARKER), &body).unwrap();

        assert_eq!(index_completion_warning(index), None);
    }
}
