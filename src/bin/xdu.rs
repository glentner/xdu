#![allow(clippy::too_many_arguments)]

use std::collections::HashSet;
use std::fs;
use std::io::{IsTerminal, stderr};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use clap::Parser;
use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use jwalk::{Parallelism, WalkDir};
use rayon::ThreadPoolBuilder;

use xdu::cli::XduArgs;
use xdu::crawl::{CrawlStats, PartitionBuffer, TopEntry, build_work_queue, record_from_metadata};
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

                    loop {
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

                        for entry in walker {
                            let entry = match entry {
                                Ok(e) => e,
                                Err(_) => continue,
                            };

                            if !entry.file_type.is_file() {
                                continue;
                            }

                            let path = entry.path();
                            let metadata = match fs::metadata(&path) {
                                Ok(m) => m,
                                Err(_) => continue,
                            };

                            let record = record_from_metadata(&path, &metadata, size_mode);
                            let file_size = record.size as u64;

                            buffer.add(record)?;

                            // Update global atomics
                            global_files.fetch_add(1, Ordering::Relaxed);
                            global_bytes.fetch_add(file_size, Ordering::Relaxed);

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

                        let prune_info = if pruned > 0 {
                            format!(", pruned {} stale", pruned)
                        } else {
                            String::new()
                        };

                        if is_tty {
                            mp_ref.println(format!(
                                "{:>12} {} ({} files, {}{})",
                                style("Finished").green().bold(),
                                item.partition,
                                format_count(buffer.file_count),
                                format_bytes(buffer.byte_count),
                                prune_info,
                            ))?;
                        } else {
                            eprintln!(
                                "Finished {} ({} files, {}{})",
                                item.partition,
                                format_count(buffer.file_count),
                                format_bytes(buffer.byte_count),
                                prune_info,
                            );
                        }
                    }

                    bar.finish_and_clear();
                    Ok(())
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
    let prune_info = if stats.pruned > 0 {
        format!(", pruned {} stale", stats.pruned)
    } else {
        String::new()
    };

    if is_tty {
        eprintln!(
            "{:>12} {} files ({}) in {:.2}s{}",
            style("Completed").green().bold(),
            format_count(stats.files),
            format_bytes(stats.bytes),
            elapsed.as_secs_f64(),
            prune_info
        );
    } else {
        eprintln!(
            "Completed {} files ({}) in {:.2}s{}",
            format_count(stats.files),
            format_bytes(stats.bytes),
            elapsed.as_secs_f64(),
            prune_info
        );
    }

    Ok(())
}
