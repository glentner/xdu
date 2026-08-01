//! The index-build crawl hot path, lifted out of the `xdu` binary so its
//! decision/ordering logic and its Parquet-finalization logic are unit-testable.
//!
//! Only the crawler pulls this in; the reader tools (`xdu-find`/`xdu-view`/`xdu-rm`)
//! never touch it. The pieces here are pure or near-pure: work-queue construction,
//! per-file record building, and the `PartitionBuffer` that accumulates records and
//! finalizes them atomically. The concurrency scaffold (shared rayon pool, driver
//! threads, `thread::scope` error propagation) stays in `src/bin/xdu.rs`.

use std::collections::{HashSet, VecDeque};
use std::fs::{self, File, Metadata};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::{FileRecord, SizeMode};

/// Special partition name for files directly in the top-level directory.
///
/// Reserved: a real top-level subdirectory of this name would collide with the
/// synthetic depth-1 partition holding loose root files (guarded against later).
pub const ROOT_PARTITION: &str = "__root__";

/// The chunk filename that is written first, before atomic finalization.
///
/// Zero-padded sequential ids give a deterministic, gap-free chunk sequence that
/// `finalize` can prune against.
pub fn chunk_partial_name(id: usize) -> String {
    format!("{:06}.parquet.partial", id)
}

/// The finalized chunk filename a reader globs (`*/*.parquet`).
pub fn chunk_final_name(id: usize) -> String {
    format!("{:06}.parquet", id)
}

/// Build a `FileRecord` from a file's path and metadata under the chosen size mode.
///
/// The `MetadataExt` reads (`blocks()`×512 for disk usage, `atime()`) are Unix-only.
/// The caller performs the `stat` syscall and passes the borrowed `Metadata` in.
pub fn record_from_metadata(path: &Path, metadata: &Metadata, size_mode: SizeMode) -> FileRecord {
    let disk_usage = metadata.blocks() * 512;
    let file_len = metadata.len();
    let atime = metadata.atime();
    let file_size = size_mode.calculate(disk_usage, file_len);

    FileRecord {
        path: path.to_string_lossy().to_string(),
        size: file_size as i64,
        atime,
    }
}

/// A top-level entry under the indexed root, classified by the crawler's `read_dir`
/// pass. The directory I/O lives in the bin; `build_work_queue` is pure over these
/// so the ordering/classification decision can be tested without a filesystem.
///
/// `path` carries the real bytes from `read_dir` (so a non-UTF-8 directory name is
/// still walked correctly), while `name` is the lossy string used as the partition
/// key and for `--partition` filtering — mirroring the original inline logic.
pub struct TopEntry {
    pub path: PathBuf,
    pub name: String,
    pub is_dir: bool,
    pub is_file: bool,
    pub is_symlink: bool,
}

/// A unit of work for a driver thread: one partition to crawl.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkItem {
    pub path: PathBuf,
    pub partition: String,
    pub max_depth: Option<usize>,
}

/// Statistics returned from a crawl operation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrawlStats {
    pub files: u64,
    pub bytes: u64,
    pub pruned: usize,
}

impl CrawlStats {
    /// Fold another unit's totals into this one (associative aggregation).
    pub fn merge(&mut self, other: &CrawlStats) {
        self.files += other.files;
        self.bytes += other.bytes;
        self.pruned += other.pruned;
    }
}

/// Turn the classified top-level entries into an ordered work queue.
///
/// Pure decision/ordering: directories become partition `WorkItem`s (subject to the
/// `--partition` filter) sorted ascending for deterministic output; a single depth-1
/// `__root__` item is pushed **first** iff any loose top-level file exists. An empty
/// result is an error — there is nothing to index.
///
/// The `is_file() || is_symlink()` root trigger matches the original behavior exactly;
/// the symlink half is a known quirk corrected separately.
pub fn build_work_queue(
    entries: Vec<TopEntry>,
    top_dir: &Path,
    partition_filter: Option<&HashSet<String>>,
) -> Result<VecDeque<WorkItem>> {
    let mut partition_items: Vec<WorkItem> = Vec::new();
    let mut has_root_files = false;

    for entry in entries {
        if entry.is_dir {
            if let Some(pf) = partition_filter
                && !pf.contains(&entry.name)
            {
                continue;
            }
            partition_items.push(WorkItem {
                path: entry.path,
                partition: entry.name,
                max_depth: None,
            });
        } else if entry.is_file || entry.is_symlink {
            has_root_files = true;
        }
    }

    // Sort for deterministic output order.
    partition_items.sort_by(|a, b| a.partition.cmp(&b.partition));

    // Root files first (depth-limited), then partition subdirectories.
    let mut work_queue: VecDeque<WorkItem> = VecDeque::with_capacity(partition_items.len() + 1);

    if has_root_files {
        work_queue.push_back(WorkItem {
            path: top_dir.to_path_buf(),
            partition: ROOT_PARTITION.to_string(),
            max_depth: Some(1),
        });
    }
    for item in partition_items {
        work_queue.push_back(item);
    }

    if work_queue.is_empty() {
        anyhow::bail!("No partitions found in {}", top_dir.display());
    }

    Ok(work_queue)
}

/// Per-partition buffer that accumulates records and flushes to Parquet.
pub struct PartitionBuffer {
    partition: String,
    outdir: PathBuf,
    records: Vec<FileRecord>,
    buffsize: usize,
    chunk_counter: usize,
    schema: Arc<Schema>,
    /// Track all .partial files written for atomic finalization.
    partial_files: Vec<PathBuf>,
    /// Track statistics for this partition.
    pub file_count: u64,
    pub byte_count: u64,
}

impl PartitionBuffer {
    pub fn new(partition: String, outdir: PathBuf, buffsize: usize, schema: Arc<Schema>) -> Self {
        Self {
            partition,
            outdir,
            records: Vec::with_capacity(buffsize),
            buffsize,
            chunk_counter: 0,
            schema,
            partial_files: Vec::new(),
            file_count: 0,
            byte_count: 0,
        }
    }

    pub fn add(&mut self, record: FileRecord) -> Result<()> {
        self.file_count += 1;
        self.byte_count += record.size as u64;
        self.records.push(record);
        if self.records.len() >= self.buffsize {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        let chunk_id = self.chunk_counter;
        self.chunk_counter += 1;
        let partition_dir = self.outdir.join(&self.partition);
        fs::create_dir_all(&partition_dir).with_context(|| {
            format!(
                "Failed to create partition dir: {}",
                partition_dir.display()
            )
        })?;

        // Write to .partial file first; finalize renames it atomically.
        let partial_path = partition_dir.join(chunk_partial_name(chunk_id));

        let mut path_builder = StringBuilder::new();
        let mut size_builder = Vec::with_capacity(self.records.len());
        let mut atime_builder = Vec::with_capacity(self.records.len());

        for record in &self.records {
            path_builder.append_value(&record.path);
            size_builder.push(record.size);
            atime_builder.push(record.atime);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(path_builder.finish()),
                Arc::new(Int64Array::from(size_builder)),
                Arc::new(Int64Array::from(atime_builder)),
            ],
        )?;

        let file = File::create(&partial_path)
            .with_context(|| format!("Failed to create file: {}", partial_path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        self.partial_files.push(partial_path);
        self.records.clear();
        Ok(())
    }

    /// Atomically finalize all .partial files by renaming them and pruning stale chunks.
    pub fn finalize(&self) -> Result<usize> {
        let partition_dir = self.outdir.join(&self.partition);
        let num_chunks = self.partial_files.len();

        // Rename all .partial files to .parquet (atomic on POSIX).
        for partial_path in &self.partial_files {
            let final_path = partial_path.with_extension(""); // removes .partial, leaves .parquet
            fs::rename(partial_path, &final_path).with_context(|| {
                format!(
                    "Failed to rename {} to {}",
                    partial_path.display(),
                    final_path.display()
                )
            })?;
        }

        // Prune any stale chunks beyond what we just wrote (from a prior larger run).
        let mut pruned = 0;
        for chunk_id in num_chunks.. {
            let stale_path = partition_dir.join(chunk_final_name(chunk_id));
            if stale_path.exists() {
                fs::remove_file(&stale_path).with_context(|| {
                    format!("Failed to remove stale chunk: {}", stale_path.display())
                })?;
                pruned += 1;
            } else {
                break; // No more consecutive chunks
            }
        }

        Ok(pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_schema;
    use std::io::Write;

    fn rec(path: &str, size: i64) -> FileRecord {
        FileRecord {
            path: path.to_string(),
            size,
            atime: 0,
        }
    }

    fn top_dir(name: &str, is_dir: bool) -> TopEntry {
        TopEntry {
            path: Path::new("/data").join(name),
            name: name.to_string(),
            is_dir,
            is_file: !is_dir,
            is_symlink: false,
        }
    }

    // ---- chunk name helpers -------------------------------------------------

    #[test]
    fn test_chunk_name_helpers() {
        assert_eq!(chunk_partial_name(0), "000000.parquet.partial");
        assert_eq!(chunk_partial_name(42), "000042.parquet.partial");
        assert_eq!(chunk_final_name(0), "000000.parquet");
        assert_eq!(chunk_final_name(123456), "123456.parquet");
    }

    // ---- CrawlStats fold ----------------------------------------------------

    #[test]
    fn test_crawl_stats_merge_folds_totals() {
        let mut total = CrawlStats::default();
        assert_eq!(
            total,
            CrawlStats {
                files: 0,
                bytes: 0,
                pruned: 0
            }
        );

        total.merge(&CrawlStats {
            files: 3,
            bytes: 300,
            pruned: 1,
        });
        total.merge(&CrawlStats {
            files: 2,
            bytes: 200,
            pruned: 0,
        });
        total.merge(&CrawlStats {
            files: 5,
            bytes: 500,
            pruned: 4,
        });

        assert_eq!(
            total,
            CrawlStats {
                files: 10,
                bytes: 1000,
                pruned: 5
            }
        );
    }

    // ---- build_work_queue ---------------------------------------------------

    #[test]
    fn test_build_work_queue_root_first_and_sorted() {
        let entries = vec![
            top_dir("bravo", true),
            top_dir("alpha", true),
            top_dir("loose.txt", false), // a loose top-level regular file
        ];
        let base = Path::new("/data");
        let queue = build_work_queue(entries, base, None).unwrap();

        let names: Vec<&str> = queue.iter().map(|w| w.partition.as_str()).collect();
        assert_eq!(names, vec![ROOT_PARTITION, "alpha", "bravo"]);

        // __root__ is depth-1; real partitions are unbounded depth.
        assert_eq!(queue[0].max_depth, Some(1));
        assert_eq!(queue[0].path, base.to_path_buf());
        assert_eq!(queue[1].max_depth, None);
        assert_eq!(queue[1].path, Path::new("/data/alpha"));
        assert_eq!(queue[2].max_depth, None);
    }

    #[test]
    fn test_build_work_queue_no_root_item_without_loose_files() {
        let entries = vec![top_dir("alpha", true), top_dir("bravo", true)];
        let queue = build_work_queue(entries, Path::new("/data"), None).unwrap();
        let names: Vec<&str> = queue.iter().map(|w| w.partition.as_str()).collect();
        assert_eq!(names, vec!["alpha", "bravo"]);
    }

    #[test]
    fn test_build_work_queue_partition_filter_excludes() {
        let entries = vec![
            top_dir("alpha", true),
            top_dir("bravo", true),
            top_dir("charlie", true),
        ];
        let filter: HashSet<String> = ["bravo".to_string()].into_iter().collect();
        let queue = build_work_queue(entries, Path::new("/data"), Some(&filter)).unwrap();
        let names: Vec<&str> = queue.iter().map(|w| w.partition.as_str()).collect();
        assert_eq!(names, vec!["bravo"]);
    }

    #[test]
    fn test_build_work_queue_symlink_triggers_root() {
        // A loose top-level symlink triggers __root__ today (preserved quirk).
        let entries = vec![TopEntry {
            path: Path::new("/data/link").to_path_buf(),
            name: "link".to_string(),
            is_dir: false,
            is_file: false,
            is_symlink: true,
        }];
        let queue = build_work_queue(entries, Path::new("/data"), None).unwrap();
        let names: Vec<&str> = queue.iter().map(|w| w.partition.as_str()).collect();
        assert_eq!(names, vec![ROOT_PARTITION]);
    }

    #[test]
    fn test_build_work_queue_empty_errors() {
        let err = build_work_queue(Vec::new(), Path::new("/data"), None).unwrap_err();
        assert!(err.to_string().contains("No partitions found"));
    }

    // ---- record_from_metadata ----------------------------------------------

    #[test]
    fn test_record_from_metadata_size_modes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f.bin");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&vec![b'x'; 5000]).unwrap();
        }
        let meta = fs::metadata(&path).unwrap();

        let apparent = record_from_metadata(&path, &meta, SizeMode::ApparentSize);
        assert_eq!(apparent.size, 5000);
        assert_eq!(apparent.path, path.to_string_lossy());
        assert_eq!(apparent.atime, meta.atime());

        let disk = record_from_metadata(&path, &meta, SizeMode::DiskUsage);
        assert_eq!(disk.size, (meta.blocks() * 512) as i64);

        let rounded = record_from_metadata(&path, &meta, SizeMode::BlockRounded(4096));
        assert_eq!(rounded.size, 8192); // 5000 rounds up to two 4096 blocks
    }

    // ---- PartitionBuffer::finalize -----------------------------------------

    #[test]
    fn test_finalize_renames_partials_and_prunes_stale_tail() {
        let dir = tempfile::TempDir::new().unwrap();
        let outdir = dir.path().to_path_buf();
        let mut buf = PartitionBuffer::new("part".to_string(), outdir.clone(), 1, get_schema());

        // buffsize 1 => each add flushes a chunk. Two records => 000000, 000001.
        buf.add(rec("/part/a", 10)).unwrap();
        buf.add(rec("/part/b", 20)).unwrap();

        let part_dir = outdir.join("part");
        // Seed a stale contiguous tail from a hypothetical prior larger run.
        for id in [2usize, 3] {
            File::create(part_dir.join(chunk_final_name(id))).unwrap();
        }

        let pruned = buf.finalize().unwrap();
        assert_eq!(pruned, 2);
        assert_eq!(buf.file_count, 2);

        assert!(part_dir.join(chunk_final_name(0)).exists());
        assert!(part_dir.join(chunk_final_name(1)).exists());
        assert!(!part_dir.join(chunk_final_name(2)).exists());
        assert!(!part_dir.join(chunk_final_name(3)).exists());
        // No .partial files survive a successful finalize.
        let leftovers = fs::read_dir(&part_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".partial"))
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn test_finalize_prune_stops_at_first_gap() {
        let dir = tempfile::TempDir::new().unwrap();
        let outdir = dir.path().to_path_buf();
        let mut buf = PartitionBuffer::new("part".to_string(), outdir.clone(), 100, get_schema());

        // One flush => a single chunk 000000.
        buf.add(rec("/part/a", 10)).unwrap();
        buf.flush().unwrap();

        let part_dir = outdir.join("part");
        // Stale 000001 exists, 000002 is a gap, 000003 exists beyond the gap.
        File::create(part_dir.join(chunk_final_name(1))).unwrap();
        File::create(part_dir.join(chunk_final_name(3))).unwrap();

        let pruned = buf.finalize().unwrap();
        // Prune starts at num_chunks (1): removes 000001, then 000002 is missing -> stop.
        assert_eq!(pruned, 1);
        assert!(part_dir.join(chunk_final_name(0)).exists());
        assert!(!part_dir.join(chunk_final_name(1)).exists());
        assert!(part_dir.join(chunk_final_name(3)).exists()); // untouched beyond the gap
    }
}
