//! Shared types and utilities for xdu tools.

use std::path::PathBuf;
use std::sync::Arc;

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
    ((size + block_size - 1) / block_size) * block_size
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
}
