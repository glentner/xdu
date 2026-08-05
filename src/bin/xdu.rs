#![allow(clippy::too_many_arguments)]

use std::collections::HashSet;
use std::fs;
use std::io::{IsTerminal, stderr};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use clap::Parser;
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use jwalk::{Parallelism, WalkDir};
use rayon::ThreadPoolBuilder;

use xdu::cli::XduArgs;
use xdu::crawl::{
    CrawlStats, EntryError, PartitionBuffer, TopEntry, build_work_queue, classify_io_error,
    clear_completion_marker, completion_marker_contents, file_size_and_atime, lossy_path,
    write_completion_marker,
};
use xdu::{SizeMode, format_bytes, format_count, format_speed, get_schema, parse_size};

/// Crawl a directory tree using concurrent per-partition walks with a shared thread pool.
///
/// Concurrency contract (must be preserved):
/// - A single shared rayon thread pool (N threads) backs *all* jwalk walkers, so
///   work-stealing balances directory reads across active partitions and one huge
///   partition can't starve the rest.
/// - C driver threads (`std::thread`s, joined by `thread::scope`) each pull partitions
///   from the `Mutex<VecDeque>` work queue and consume their walker serially.
/// - `thread::scope` propagates the first driver `Err` (or panic under unwind) as the
///   run's error. Thread budget: N pool + C drivers + 1 main.
///
/// The pure classification/ordering (`build_work_queue`), per-file record building
/// (`record_from_metadata`), and Parquet finalization (`PartitionBuffer`) live in
/// `xdu::crawl` so they are unit-testable; this function is the orchestrator.
///
/// The run-level completion marker is cleared once pre-flight passes and written by
/// `main` only on the success path, so an index this run abandons carries no
/// attestation, while a run rejected before it crawls leaves the previous marker intact.
fn crawl(
    top_dir: &Path,
    outdir: &Path,
    jobs: usize,
    buffsize: usize,
    size_mode: SizeMode,
    schema: &Arc<Schema>,
    partition_filter: Option<&HashSet<String>>,
    is_tty: bool,
) -> Result<CrawlStats> {
    // Build shared rayon thread pool for jwalk walkers
    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(jobs)
            .build()
            .context("Failed to build thread pool")?,
    );

    // Enumerate top-level entries. The directory I/O stays here; the pure classification
    // and ordering decision (partition vs loose file, --partition filter, __root__-first,
    // sort, empty-check) lives in `build_work_queue` so it can be unit-tested.
    let mut entries: Vec<TopEntry> = Vec::new();
    for entry in fs::read_dir(top_dir)
        .with_context(|| format!("Failed to read directory: {}", top_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        entries.push(TopEntry {
            path,
            name,
            is_dir: ft.is_dir(),
            is_file: ft.is_file(),
            is_symlink: ft.is_symlink(),
        });
    }

    let work_queue = build_work_queue(entries, top_dir, partition_filter)?;
    let num_items = work_queue.len();

    // Pre-flight has passed, so from here the index is being rewritten and the previous
    // run's attestation no longer describes it. Dropping the marker after the last check
    // that can still reject the run, and before any driver writes, means a crash below
    // leaves an index that is visibly unattested rather than one that still claims to be
    // complete — while a run rejected without touching the index leaves an
    // already-complete index still attested.
    clear_completion_marker(outdir)?;

    // Progress display
    let mp = MultiProgress::new();
    if !is_tty {
        mp.set_draw_target(ProgressDrawTarget::hidden());
    }

    let filter_desc = if let Some(pf) = partition_filter {
        let mut names: Vec<_> = pf.iter().cloned().collect();
        names.sort();
        format!(" (partitions: {})", names.join(", "))
    } else {
        String::new()
    };

    if is_tty {
        eprintln!(
            "{:>12} {}{}",
            style("Indexing").green().bold(),
            top_dir.display(),
            filter_desc
        );
    } else {
        eprintln!("Indexing {}{}", top_dir.display(), filter_desc);
    }

    // Global summary bar (positioned last, below per-partition bars)
    let global_style = ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg}")
        .unwrap();
    let global_bar = mp.add(ProgressBar::new_spinner());
    global_bar.set_style(global_style);
    global_bar.enable_steady_tick(Duration::from_millis(200));

    // Shared state for cross-thread aggregation
    let queue = Arc::new(Mutex::new(work_queue));
    let global_files = Arc::new(AtomicU64::new(0));
    let global_bytes = Arc::new(AtomicU64::new(0));
    let global_pruned = Arc::new(AtomicUsize::new(0));
    // Benign vanished-file races vs. hard (permission/IO) errors. A hard error fails
    // the run unless --allow-errors; both are surfaced in the summary (all on stderr).
    let global_vanished = Arc::new(AtomicU64::new(0));
    let global_errors = Arc::new(AtomicU64::new(0));
    // Paths stored with U+FFFD replacements: counted and reported, never fatal.
    let global_lossy = Arc::new(AtomicU64::new(0));

    // Raised by the first driver that fails. The others stop pulling partitions
    // rather than growing an index this run will never mark complete.
    let cancel = Arc::new(AtomicBool::new(false));

    // Global speed tracking (shared across drivers, protected by single Mutex)
    let global_speed_state = Arc::new(Mutex::new((
        Instant::now(), // last sample time
        0_u64,          // last sample file count
        0.0_f64,        // current speed
        0.0_f64,        // peak speed
    )));

    let num_drivers = jobs.min(num_items).max(1);

    std::thread::scope(|s| -> Result<()> {
        let handles: Vec<_> = (0..num_drivers)
            .map(|driver_id| {
                let pool = pool.clone();
                let queue = queue.clone();
                let global_files = global_files.clone();
                let global_bytes = global_bytes.clone();
                let global_pruned = global_pruned.clone();
                let global_vanished = global_vanished.clone();
                let global_errors = global_errors.clone();
                let global_lossy = global_lossy.clone();
                let cancel = cancel.clone();
                let global_speed_state = global_speed_state.clone();
                let schema = schema.clone();
                let mp_ref = &mp;
                let global_bar_ref = &global_bar;
                let outdir = outdir.to_path_buf();

                s.spawn(move || -> Result<()> {
                    let bar_style = ProgressStyle::default_spinner()
                        .template("{spinner:.cyan} {msg}")
                        .unwrap();
                    let bar = mp_ref.insert_before(global_bar_ref, ProgressBar::new_spinner());
                    bar.set_style(bar_style);
                    bar.enable_steady_tick(Duration::from_millis(100));

                    // Emit a diagnostic to stderr, coordinating with the progress bars in
                    // TTY mode (stdout stays clean and pipeable).
                    let report = |msg: &str| {
                        if is_tty {
                            let _ = mp_ref.println(msg);
                        } else {
                            eprintln!("{}", msg);
                        }
                    };

                    // The queue drain is its own closure so a first error can raise the
                    // shared cancel flag before this driver returns.
                    let drain_queue = || -> Result<()> {
                        loop {
                            // Another driver has already failed: this run cannot produce a
                            // complete index, so stop taking on new partitions.
                            if cancel.load(Ordering::Relaxed) {
                                break;
                            }

                            let item = {
                                let mut q = queue.lock().unwrap();
                                q.pop_front()
                            };
                            let item = match item {
                                Some(i) => i,
                                None => break,
                            };

                            bar.set_message(format!("{}: scanning...", item.partition));

                            let walker = WalkDir::new(&item.path)
                                .parallelism(Parallelism::RayonExistingPool {
                                    pool: pool.clone(),
                                    busy_timeout: None,
                                })
                                .max_depth(item.max_depth.unwrap_or(usize::MAX))
                                .skip_hidden(false)
                                .follow_links(false);

                            let mut buffer = PartitionBuffer::new(
                                item.partition.clone(),
                                outdir.clone(),
                                buffsize,
                                schema.clone(),
                            );

                            let mut last_bar_update = Instant::now();
                            let bar_interval = Duration::from_millis(100);

                            // Per-partition speed tracking (1s rolling window)
                            let mut speed_sample_count: u64 = 0;
                            let mut speed_sample_time = Instant::now();
                            let mut current_speed: f64 = 0.0;
                            let mut peak_speed: f64 = 0.0;

                            // Per-partition skip/error tallies (folded into the globals below).
                            let mut part_vanished: u64 = 0;
                            let mut part_errors: u64 = 0;
                            let mut part_lossy: u64 = 0;

                            for entry in walker {
                                let entry = match entry {
                                    Ok(e) => e,
                                    // A jwalk error stands in for a whole unreadable subtree: a
                                    // failed directory read yields one Err in place of all its
                                    // children. Never drop it silently.
                                    Err(err) => {
                                        let kind = err.io_error().map(|e| e.kind());
                                        match classify_io_error(kind) {
                                            EntryError::Vanished => part_vanished += 1,
                                            EntryError::Hard => {
                                                part_errors += 1;
                                                let path =
                                                    err.path().unwrap_or(item.path.as_path());
                                                let detail = err
                                                    .io_error()
                                                    .map(|e| e.to_string())
                                                    .unwrap_or_else(|| err.to_string());
                                                report(&format!(
                                                    "error: {}: {}",
                                                    path.display(),
                                                    detail
                                                ));
                                            }
                                        }
                                        continue;
                                    }
                                };

                                // A directory jwalk could not descend into (e.g. permission
                                // denied) is yielded as an Ok entry with the read failure attached
                                // here — NOT as an iterator Err. This is the load-bearing check
                                // that turns a silently-dropped subtree into a counted, reported,
                                // run-failing error.
                                if let Some(err) = entry.read_children_error.as_ref() {
                                    let kind = err.io_error().map(|e| e.kind());
                                    match classify_io_error(kind) {
                                        EntryError::Vanished => part_vanished += 1,
                                        EntryError::Hard => {
                                            part_errors += 1;
                                            let detail = err
                                                .io_error()
                                                .map(|e| e.to_string())
                                                .unwrap_or_else(|| err.to_string());
                                            let path = err
                                                .path()
                                                .map(|p| p.display().to_string())
                                                .unwrap_or_else(|| {
                                                    entry.path().display().to_string()
                                                });
                                            report(&format!("error: {}: {}", path, detail));
                                        }
                                    }
                                }

                                if !entry.file_type.is_file() {
                                    continue;
                                }

                                // `DirEntry::metadata` is `symlink_metadata` here (the
                                // walker sets follow_links(false)), which closes the
                                // window where a file could be swapped for a symlink
                                // between the directory read and this stat. For a regular
                                // file lstat and stat agree, so sizes and atimes are
                                // unchanged.
                                let metadata = match entry.metadata() {
                                    Ok(m) => m,
                                    // The file raced away (ENOENT) or became unreadable
                                    // between the walk and this stat: benign vs. hard.
                                    Err(err) => {
                                        let kind = err.io_error().map(|e| e.kind());
                                        match classify_io_error(kind) {
                                            EntryError::Vanished => part_vanished += 1,
                                            EntryError::Hard => {
                                                part_errors += 1;
                                                let detail = err
                                                    .io_error()
                                                    .map(|e| e.to_string())
                                                    .unwrap_or_else(|| err.to_string());
                                                report(&format!(
                                                    "error: {}: {}",
                                                    entry.path().display(),
                                                    detail
                                                ));
                                            }
                                        }
                                        continue;
                                    }
                                };

                                let (file_size, atime) = file_size_and_atime(&metadata, size_mode);
                                let path = entry.path();
                                let (path_str, lossy) = lossy_path(&path);

                                // The stored path carries U+FFFD in place of the real
                                // bytes, so it names no file on disk. Say so once per
                                // partition and count the rest — a flood of these would
                                // bury the errors that matter.
                                if lossy {
                                    part_lossy += 1;
                                    if part_lossy == 1 {
                                        report(&format!(
                                            "warning: {}: non-UTF-8 path stored with \
                                         replacement characters; it will not round-trip \
                                         to xdu-rm (further occurrences in this \
                                         partition are counted only)",
                                            path.display()
                                        ));
                                    }
                                }

                                buffer.add(&path_str, file_size, atime)?;

                                // Update global atomics
                                global_files.fetch_add(1, Ordering::Relaxed);
                                global_bytes.fetch_add(file_size as u64, Ordering::Relaxed);

                                // Update progress bars periodically
                                let now = Instant::now();
                                if now.duration_since(last_bar_update) >= bar_interval {
                                    // Per-partition speed: 1-second rolling window
                                    let speed_elapsed =
                                        now.duration_since(speed_sample_time).as_secs_f64();
                                    if speed_elapsed >= 1.0 {
                                        let delta = buffer.file_count - speed_sample_count;
                                        current_speed = delta as f64 / speed_elapsed;
                                        if current_speed > peak_speed {
                                            peak_speed = current_speed;
                                        }
                                        speed_sample_count = buffer.file_count;
                                        speed_sample_time = now;
                                    }

                                    // Global speed: 1-second rolling window
                                    let total_files = global_files.load(Ordering::Relaxed);
                                    let global_speed_str = {
                                        let mut gs = global_speed_state.lock().unwrap();
                                        let g_elapsed = now.duration_since(gs.0).as_secs_f64();
                                        if g_elapsed >= 1.0 {
                                            let g_delta = total_files.saturating_sub(gs.1);
                                            gs.2 = g_delta as f64 / g_elapsed;
                                            if gs.2 > gs.3 {
                                                gs.3 = gs.2;
                                            }
                                            gs.0 = now;
                                            gs.1 = total_files;
                                        }
                                        if gs.2 > 0.0 {
                                            format!(
                                                " | {} (peak: {})",
                                                format_speed(gs.2),
                                                format_speed(gs.3)
                                            )
                                        } else {
                                            String::new()
                                        }
                                    };

                                    let speed_str = if current_speed > 0.0 {
                                        format!(
                                            " | {} (peak: {})",
                                            format_speed(current_speed),
                                            format_speed(peak_speed)
                                        )
                                    } else {
                                        String::new()
                                    };

                                    bar.set_message(format!(
                                        "{}: {} files, {}{} [T{}]",
                                        item.partition,
                                        format_count(buffer.file_count),
                                        format_bytes(buffer.byte_count),
                                        speed_str,
                                        driver_id,
                                    ));
                                    global_bar_ref.set_message(format!(
                                        "{} files, {}{}",
                                        format_count(total_files),
                                        format_bytes(global_bytes.load(Ordering::Relaxed)),
                                        global_speed_str,
                                    ));
                                    last_bar_update = now;
                                }
                            }

                            buffer.flush()?;
                            let pruned = buffer.finalize()?;
                            global_pruned.fetch_add(pruned, Ordering::Relaxed);
                            global_vanished.fetch_add(part_vanished, Ordering::Relaxed);
                            global_errors.fetch_add(part_errors, Ordering::Relaxed);
                            global_lossy.fetch_add(part_lossy, Ordering::Relaxed);

                            let mut status_info = if pruned > 0 {
                                format!(", pruned {} stale", pruned)
                            } else {
                                String::new()
                            };
                            if part_vanished > 0 {
                                status_info.push_str(&format!(", {} vanished", part_vanished));
                            }
                            if part_errors > 0 {
                                status_info.push_str(&format!(", {} errors", part_errors));
                            }
                            if part_lossy > 0 {
                                status_info.push_str(&format!(", {} non-UTF-8", part_lossy));
                            }

                            if is_tty {
                                mp_ref.println(format!(
                                    "{:>12} {} ({} files, {}{})",
                                    style("Finished").green().bold(),
                                    item.partition,
                                    format_count(buffer.file_count),
                                    format_bytes(buffer.byte_count),
                                    status_info,
                                ))?;
                            } else {
                                eprintln!(
                                    "Finished {} ({} files, {}{})",
                                    item.partition,
                                    format_count(buffer.file_count),
                                    format_bytes(buffer.byte_count),
                                    status_info,
                                );
                            }
                        }
                        Ok(())
                    };

                    let result = drain_queue();
                    if result.is_err() {
                        cancel.store(true, Ordering::Relaxed);
                    }

                    bar.finish_and_clear();
                    result
                })
            })
            .collect();

        // Wait for all drivers and propagate errors
        let mut first_error: Option<anyhow::Error> = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                // Reachable only under unwind (tests/debug); release sets panic="abort".
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(anyhow::anyhow!("Driver thread panicked"));
                    }
                }
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }
        Ok(())
    })?;

    global_bar.finish_and_clear();

    Ok(CrawlStats {
        files: global_files.load(Ordering::Relaxed),
        bytes: global_bytes.load(Ordering::Relaxed),
        pruned: global_pruned.load(Ordering::Relaxed),
        vanished: global_vanished.load(Ordering::Relaxed),
        errors: global_errors.load(Ordering::Relaxed),
        lossy_paths: global_lossy.load(Ordering::Relaxed),
    })
}

fn main() -> Result<()> {
    let args = XduArgs::parse();
    let start_time = Instant::now();
    let is_tty = stderr().is_terminal();

    // Determine size calculation mode
    let size_mode = if let Some(ref bs) = args.block_size {
        let block_size = parse_size(bs).map_err(|e| anyhow::anyhow!(e))? as u64;
        SizeMode::BlockRounded(block_size)
    } else if args.apparent_size {
        SizeMode::ApparentSize
    } else {
        SizeMode::DiskUsage
    };

    let top_dir = args
        .dir
        .canonicalize()
        .with_context(|| format!("Failed to resolve directory: {}", args.dir.display()))?;

    fs::create_dir_all(&args.outdir).with_context(|| {
        format!(
            "Failed to create output directory: {}",
            args.outdir.display()
        )
    })?;

    let outdir = args.outdir.canonicalize()?;

    let schema = get_schema();

    // Build partition filter if specified
    let partition_filter: Option<HashSet<String>> = args.partition.map(|p| p.into_iter().collect());

    // Validate partition filter if specified
    if let Some(ref pf) = partition_filter {
        for partition_name in pf {
            let partition_path = top_dir.join(partition_name);
            if !partition_path.is_dir() {
                anyhow::bail!(
                    "Partition '{}' not found in {}",
                    partition_name,
                    top_dir.display()
                );
            }
        }
    }

    let stats = crawl(
        &top_dir,
        &outdir,
        args.jobs,
        args.buffsize,
        size_mode,
        &schema,
        partition_filter.as_ref(),
        is_tty,
    )?;

    let elapsed = start_time.elapsed();
    let mut summary_info = if stats.pruned > 0 {
        format!(", pruned {} stale", stats.pruned)
    } else {
        String::new()
    };
    if stats.vanished > 0 {
        summary_info.push_str(&format!(", {} vanished", stats.vanished));
    }
    if stats.errors > 0 {
        summary_info.push_str(&format!(", {} errors", stats.errors));
    }
    if stats.lossy_paths > 0 {
        summary_info.push_str(&format!(", {} non-UTF-8", stats.lossy_paths));
    }

    if is_tty {
        eprintln!(
            "{:>12} {} files ({}) in {:.2}s{}",
            style("Completed").green().bold(),
            format_count(stats.files),
            format_bytes(stats.bytes),
            elapsed.as_secs_f64(),
            summary_info
        );
    } else {
        eprintln!(
            "Completed {} files ({}) in {:.2}s{}",
            format_count(stats.files),
            format_bytes(stats.bytes),
            elapsed.as_secs_f64(),
            summary_info
        );
    }

    // Fail loud: an unreadable region was skipped, so the index is incomplete. The
    // reachable files were still written and the offending paths already reported;
    // --allow-errors opts into indexing what is reachable and exiting 0 instead.
    if stats.errors > 0 && !args.allow_errors {
        anyhow::bail!(
            "encountered {} unreadable path(s); the index is incomplete — \
             re-run with --allow-errors to index reachable files and exit 0",
            stats.errors
        );
    }

    // Attest to the run only now: every partition was walked and finalized, and any
    // errors along the way were explicitly tolerated. Every failure path above returns
    // before this point, leaving the index unmarked. The recorded counts keep an
    // --allow-errors run honest about what it skipped.
    let completed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_completion_marker(&outdir, &completion_marker_contents(&stats, completed_at))?;

    Ok(())
}
