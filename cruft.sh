#!/bin/bash
# cruft.sh - Generate per-user reports of old files (>60 days) from xdu index
#
# Usage: ./cruft.sh /path/to/index

set -euo pipefail

INDEX_PATH="${1:?Usage: $0 <index_path>}"

# Validate index path
if [[ ! -d "$INDEX_PATH" ]]; then
    echo "Error: Index path does not exist: $INDEX_PATH" >&2
    exit 1
fi

# Resolve to absolute path
INDEX_PATH="$(cd "$INDEX_PATH" && pwd)"

# Calculate cutoff timestamp (60 days ago)
CUTOFF=$(( $(date +%s) - 60 * 86400 ))

# Create output directory
CRUFT_DIR="$INDEX_PATH/.cruft"
mkdir -p "$CRUFT_DIR"

# Format bytes into human-readable form
format_bytes() {
    local bytes=$1
    if (( bytes >= 1099511627776 )); then
        printf "%.2f TiB" "$(echo "scale=2; $bytes / 1099511627776" | bc)"
    elif (( bytes >= 1073741824 )); then
        printf "%.2f GiB" "$(echo "scale=2; $bytes / 1073741824" | bc)"
    elif (( bytes >= 1048576 )); then
        printf "%.2f MiB" "$(echo "scale=2; $bytes / 1048576" | bc)"
    elif (( bytes >= 1024 )); then
        printf "%.2f KiB" "$(echo "scale=2; $bytes / 1024" | bc)"
    else
        printf "%d B" "$bytes"
    fi
}

# Process each partition directory
for part_dir in "$INDEX_PATH"/*/; do
    # Skip if not a directory or if it's .cruft
    [[ -d "$part_dir" ]] || continue
    part=$(basename "$part_dir")
    [[ "$part" == ".cruft" ]] && continue
    
    # Check if partition has parquet files
    if ! ls "$part_dir"/*.parquet >/dev/null 2>&1; then
        continue
    fi
    
    glob_pattern="$part_dir*.parquet"
    
    # Query for count and total size of old files
    result=$(duckdb -csv -noheader <<EOF
SELECT 
    COUNT(*) as file_count,
    COALESCE(SUM(size), 0) as total_size
FROM read_parquet('$glob_pattern')
WHERE atime < $CUTOFF;
EOF
    )
    
    file_count=$(echo "$result" | cut -d',' -f1)
    total_size=$(echo "$result" | cut -d',' -f2)
    
    # Skip if no old files
    if [[ "$file_count" -eq 0 ]]; then
        continue
    fi
    
    # Format the size
    size_formatted=$(format_bytes "$total_size")
    
    # Write the report
    report_file="$CRUFT_DIR/${part}.txt"
    cat > "$report_file" <<EOF
Storage Audit Report
====================
Partition: $part
Generated: $(date '+%Y-%m-%d %H:%M:%S')

Files Not Accessed in Over 60 Days
----------------------------------
File count:  $file_count
Total size:  $size_formatted ($total_size bytes)

Please review these files and consider archiving or removing
data that is no longer needed to free up storage space.
EOF
    
    echo "Generated: $report_file ($file_count files, $size_formatted)"
done

echo "Done. Reports written to: $CRUFT_DIR/"
