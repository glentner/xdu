# Fast-path Crawling for Large Partitions

## The Problem

At extreme scale (1k-10k partitions), most partitions finish quickly but a handful of "whale" partitions with 30-50M files become bottlenecks. With one-thread-per-partition parallelism, 99% of partitions complete in 2-3 hours, but the remaining 1-5 whales take 10+ hours with a single thread each.

This "long tail" problem is common in HPC environments where a few power users accumulate massive home directories while most users have modest footprints.

## The Solution

xdu provides a **two-phase fast crawl** that eliminates the long tail:

1. **Phase 1**: Crawl large "whale" partitions with full thread parallelism *within* each partition
2. **Phase 2**: Crawl remaining partitions with standard one-thread-per-partition parallelism

This approach ensures whale partitions get all available threads instead of being bottlenecked to a single thread.

## Usage

### Identify Large Partitions

Use `xdu-find --top N` to find the N largest partitions by file count from an existing index:

```bash
xdu-find -i /var/lib/xdu/home --top 5
```

Output (one partition per line):
```
alice
bob
charlie
dave
eve
```

### Two-Phase Fast Crawl

Use `--fast --partition` to crawl specified partitions first, then automatically crawl everything else:

```bash
xdu /home -o /var/lib/xdu/home --fast --partition alice,bob,charlie -j 32
```

This will:
1. Crawl `alice` with all 32 threads parallelizing across subdirectories
2. Crawl `bob` with all 32 threads
3. Crawl `charlie` with all 32 threads
4. Crawl all remaining partitions with standard one-partition-per-thread parallelism

### Scripted Workflow

Combine both features for a fully automated fast crawl:

```bash
#!/bin/bash
# fast-index.sh - Two-phase index with automatic whale detection

INDEX="/var/lib/xdu/home"
SOURCE="/home"
THREADS=32
TOP_N=5

# Get top N partitions by file count from previous index
WHALES=$(xdu-find -i "$INDEX" --top "$TOP_N" | paste -sd, -)

if [ -n "$WHALES" ]; then
    echo "Fast-crawling whale partitions: $WHALES"
    xdu "$SOURCE" -o "$INDEX" --fast --partition "$WHALES" -j "$THREADS"
else
    echo "No existing index, running standard crawl"
    xdu "$SOURCE" -o "$INDEX" -j "$THREADS"
fi
```

## Options Reference

### xdu

| Option | Description |
|--------|-------------|
| `--partition NAMES` | Crawl one or more partitions (comma-separated). All threads parallelize within each partition. |
| `--fast` | After crawling specified partitions, do a full crawl excluding them. Requires `--partition`. |

### xdu-find

| Option | Description |
|--------|-------------|
| `--top N` | Show top N partitions by file count. Outputs one partition name per line. |

## How It Works

### Standard Full Crawl

In a standard full crawl (`xdu /home -o /index -j 32`), each top-level subdirectory (partition) is assigned to one thread. With 32 threads and 1000 partitions, 32 partitions are processed concurrently, but each partition is crawled by a single thread.

```
Thread 1:  alice ████████████████████████████████████████ (30M files, 10 hours)
Thread 2:  bob   ██ (100K files, done)
Thread 3:  carol ██ (done)
...
Thread 32: zach  ██ (done)
           [waiting for alice...]
```

### Fast Crawl with --partition

With `--partition alice`, all 32 threads parallelize *within* alice by crawling its subdirectories concurrently:

```
Thread 1:  alice/projects    ████████ (done)
Thread 2:  alice/data        ████████ (done)
Thread 3:  alice/experiments ████████ (done)
...
Thread 32: alice/archive     ████████ (done)
           [alice finished in ~30 minutes instead of 10 hours]
```

### Two-Phase Fast Crawl with --fast

Combining `--fast --partition alice,bob,charlie`:

**Phase 1**: Each whale partition gets full parallelism sequentially:
```
[alice: 32 threads → 30 min]
[bob: 32 threads → 20 min]
[charlie: 32 threads → 25 min]
```

**Phase 2**: Remaining partitions crawl with standard parallelism:
```
[997 small partitions: 32 concurrent → 1 hour]
```

**Total**: ~2.25 hours instead of 10+ hours.

## Performance Impact

Typical improvement for a filesystem with whale partitions:

| Scenario | Standard Crawl | Fast Crawl | Speedup |
|----------|---------------|------------|--------|
| 1000 partitions, 5 whales (30M files each) | 12+ hours | 3-4 hours | ~3-4x |
| 5000 partitions, 10 whales (50M files each) | 20+ hours | 5-6 hours | ~3-4x |

The speedup depends on:
- How much larger the whales are compared to average partitions
- How many subdirectories exist within whale partitions (more = better parallelism)
- I/O bandwidth and thread count
