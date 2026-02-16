#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]

use std::io::stdout;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use duckdb::Connection;
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};

use xdu::{format_bytes, parse_size, QueryFilters, SortMode};

/// Format file count with K/M/B suffixes
fn format_file_count(count: i64) -> String {
    if count >= 1_000_000_000 {
        format!("{:.1}B files", count as f64 / 1_000_000_000.0)
    } else if count >= 1_000_000 {
        format!("{:.1}M files", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K files", count as f64 / 1_000.0)
    } else if count == 1 {
        "1 file".to_string()
    } else {
        format!("{} files", count)
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "xdu-view",
    about = "Interactive TUI for exploring a file metadata index",
    after_help = "\
Examples:
  xdu-view -i /index/scratch
  xdu-view -i /index/scratch -u alice
  xdu-view -i /index/scratch --older-than 90 --sort size"
)]
struct Args {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR", env = "XDU_INDEX")]
    index: PathBuf,

    /// Initial partition to view (optional, shows partition list if omitted)
    #[arg(short = 'u', long, value_name = "NAME")]
    partition: Option<String>,

    /// Regular expression pattern to match paths
    #[arg(short, long, value_name = "REGEX")]
    pattern: Option<String>,

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

    /// Sort order: name, size-asc, size-desc, count-asc, count-desc
    #[arg(short, long, default_value = "name")]
    sort: String,
}

/// Represents a directory entry in the view
#[derive(Clone, Debug)]
struct DirEntry {
    name: String,
    #[allow(dead_code)]
    path: String,
    is_dir: bool,
    total_size: i64,
    file_count: i64,
    latest_atime: i64,
}

/// Input mode for interactive filter entry
#[derive(Clone, Debug, PartialEq)]
enum InputMode {
    /// Normal navigation mode
    Normal,
    /// Entering a pattern filter
    Pattern,
    /// Entering older-than days
    OlderThan,
    /// Entering newer-than days
    NewerThan,
    /// Entering min-size
    MinSize,
    /// Entering max-size
    MaxSize,
    /// Selecting sort mode
    SortSelect,
}

impl InputMode {
    fn prompt(&self) -> &'static str {
        match self {
            InputMode::Normal => "",
            InputMode::Pattern => "Pattern (regex): ",
            InputMode::OlderThan => "Older than (days): ",
            InputMode::NewerThan => "Newer than (days): ",
            InputMode::MinSize => "Min size (e.g., 1M): ",
            InputMode::MaxSize => "Max size (e.g., 1G): ",
            InputMode::SortSelect => "",
        }
    }
}

/// View mode for the TUI
#[derive(Clone, Copy, Debug, PartialEq)]
enum ViewMode {
    /// Traditional single-list view (default)
    List,
    /// Miller columns (tree) view — horizontal cascade of directory hierarchy
    Tree,
}

/// A single column in the Miller columns (tree) view.
struct Column {
    /// Display title for the column header
    title: String,
    /// Partition name (None = partition list column)
    partition: Option<String>,
    /// Root path for the partition (empty for partition list)
    partition_root: String,
    /// Directory path this column represents
    path: String,
    /// Entries in this column
    entries: Vec<DirEntry>,
    /// Selection state
    list_state: ListState,
}

/// Application state
struct App {
    conn: Connection,
    index_path: PathBuf,
    
    /// Current absolute path prefix (the root path for the current partition)
    partition_root: String,
    
    /// Current directory path (absolute, empty = partition list)
    current_path: String,
    
    /// Current partition being viewed (None = viewing partition list)
    current_partition: Option<String>,
    
    /// Entries in the current view
    entries: Vec<DirEntry>,
    
    /// List selection state
    list_state: ListState,
    
    /// Whether we're currently loading
    loading: bool,
    
    /// Status message
    status: String,

    /// Query filters
    filters: QueryFilters,

    /// Sort mode
    sort_mode: SortMode,

    /// Current input mode
    input_mode: InputMode,

    /// Current input buffer
    input_buffer: String,

    /// Pending sort mode (for sort selection)
    pending_sort: SortMode,

    /// Current view mode (list or tree)
    view_mode: ViewMode,

    /// Columns for Miller columns (tree) view
    columns: Vec<Column>,

    /// Active column index in tree view
    active_column: usize,
}

impl App {
    fn new(
        conn: Connection,
        index_path: PathBuf,
        initial_partition: Option<String>,
        filters: QueryFilters,
        sort_mode: SortMode,
    ) -> Result<Self> {
        let mut app = App {
            conn,
            index_path,
            partition_root: String::new(),
            current_path: String::new(),
            current_partition: None,
            entries: Vec::new(),
            list_state: ListState::default(),
            loading: false,
            status: String::new(),
            filters,
            sort_mode,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            pending_sort: sort_mode,
            view_mode: ViewMode::List,
            columns: Vec::new(),
            active_column: 0,
        };
        
        if let Some(partition) = initial_partition {
            app.current_partition = Some(partition);
            app.load_directory()?;
        } else {
            app.load_partitions()?;
        }
        
        if !app.entries.is_empty() {
            app.list_state.select(Some(0));
        }
        
        Ok(app)
    }
    
    /// Load the list of partitions
    fn load_partitions(&mut self) -> Result<()> {
        self.loading = true;
        let start = Instant::now();
        
        // Use DuckDB's hive partitioning to get partition names directly from directory structure
        let glob = format!("{}/*/*.parquet", self.index_path.display());
        
        // Build filter clause
        let filter_clause = self.filters.to_where_clause();
        let having_clause = if filter_clause.is_empty() {
            "HAVING partition IS NOT NULL AND partition != ''".to_string()
        } else {
            format!("HAVING partition IS NOT NULL AND partition != '' AND SUM(CASE WHEN {} THEN 1 ELSE 0 END) > 0", filter_clause)
        };
        
        // Query using filename to extract partition from the parquet file path
        // The partition name is the directory containing the parquet file
        // When filters are active, only count/sum matching files
        let sql = if self.filters.is_active() {
            format!(
                r#"
                SELECT 
                    regexp_extract(filename, '.*/([^/]+)/[^/]+\.parquet$', 1) as partition,
                    SUM(CASE WHEN {filter} THEN size ELSE 0 END) as total_size,
                    MAX(CASE WHEN {filter} THEN atime ELSE 0 END) as latest_atime,
                    SUM(CASE WHEN {filter} THEN 1 ELSE 0 END) as file_count
                FROM read_parquet('{glob}', filename=true)
                GROUP BY partition
                {having}
                ORDER BY {order}
                "#,
                filter = filter_clause,
                glob = glob,
                having = having_clause,
                order = self.sort_mode.to_partition_order_by()
            )
        } else {
            format!(
                r#"
                SELECT 
                    regexp_extract(filename, '.*/([^/]+)/[^/]+\.parquet$', 1) as partition,
                    SUM(size) as total_size,
                    MAX(atime) as latest_atime,
                    COUNT(*) as file_count
                FROM read_parquet('{glob}', filename=true)
                GROUP BY partition
                HAVING partition IS NOT NULL AND partition != ''
                ORDER BY {order}
                "#,
                glob = glob,
                order = self.sort_mode.to_partition_order_by()
            )
        };
        
        self.entries.clear();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let total_size: i64 = row.get(1)?;
            let latest_atime: i64 = row.get(2)?;
            let file_count: i64 = row.get(3)?;
            
            self.entries.push(DirEntry {
                name: name.clone(),
                path: name,
                is_dir: true,
                total_size,
                file_count,
                latest_atime,
            });
        }
        
        self.status = format!("{} partitions loaded in {:.2}s", self.entries.len(), start.elapsed().as_secs_f64());
        self.loading = false;
        Ok(())
    }
    
    /// Discover the common root path for a partition
    fn discover_partition_root(&self, partition: &str) -> Result<String> {
        let glob = format!("{}/{}/*.parquet", self.index_path.display(), partition);
        
        // Get the shortest path to find the common root
        let sql = format!(
            "SELECT path FROM read_parquet('{}') ORDER BY length(path) LIMIT 1",
            glob
        );
        
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        if let Some(row) = rows.next()? {
            let sample_path: String = row.get(0)?;
            // Find the directory containing this file
            if let Some(pos) = sample_path.rfind('/') {
                return Ok(sample_path[..pos].to_string());
            }
        }
        
        Ok(String::new())
    }
    
    /// Load directory contents for the current path
    fn load_directory(&mut self) -> Result<()> {
        self.loading = true;
        let start = Instant::now();
        
        let partition = self.current_partition.as_ref().unwrap();
        let glob = format!("{}/{}/*.parquet", self.index_path.display(), partition);
        
        // If we don't have a partition root yet, discover it
        if self.partition_root.is_empty() {
            self.partition_root = self.discover_partition_root(partition)?;
            self.current_path = self.partition_root.clone();
        }
        
        // Build the path prefix we're looking at (with trailing slash for LIKE)
        let prefix = format!("{}/", self.current_path);
        
        // Query to get entries at this level
        // We extract the next path component after the current path prefix
        let prefix_len = prefix.len();
        
        // Build filter conditions
        let filter_clause = self.filters.to_where_clause();
        let file_filter = if filter_clause.is_empty() {
            format!("path LIKE '{}%'", prefix)
        } else {
            format!("path LIKE '{}%' AND {}", prefix, filter_clause)
        };
        
        let order_by = self.sort_mode.to_order_by(self.sort_mode == SortMode::Name);
        
        let sql = format!(
            r#"
            WITH files AS (
                SELECT 
                    path,
                    size,
                    atime
                FROM read_parquet('{glob}')
                WHERE {file_filter}
            ),
            components AS (
                SELECT 
                    path,
                    size,
                    atime,
                    CASE 
                        WHEN position('/' IN substr(path, {prefix_len} + 1)) > 0 
                        THEN substr(path, {prefix_len} + 1, position('/' IN substr(path, {prefix_len} + 1)) - 1)
                        ELSE substr(path, {prefix_len} + 1)
                    END as component,
                    CASE 
                        WHEN position('/' IN substr(path, {prefix_len} + 1)) > 0 THEN true
                        ELSE false
                    END as is_dir
                FROM files
            )
            SELECT 
                component,
                bool_or(is_dir) as is_dir,
                SUM(size) as total_size,
                COUNT(*) as file_count,
                MAX(atime) as latest_atime
            FROM components
            WHERE component != '' AND component IS NOT NULL
            GROUP BY component
            ORDER BY {order_by}
            "#,
            glob = glob,
            file_filter = file_filter,
            prefix_len = prefix_len,
            order_by = order_by
        );
        
        self.entries.clear();
        
        // Add parent entry (always show ".." to go back)
        self.entries.push(DirEntry {
            name: "..".to_string(),
            path: "..".to_string(),
            is_dir: true,
            total_size: 0,
            file_count: 0,
            latest_atime: 0,
        });
        
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        while let Some(row) = rows.next()? {
            let component: String = row.get(0)?;
            let is_dir: bool = row.get(1)?;
            let total_size: i64 = row.get(2)?;
            let file_count: i64 = row.get(3)?;
            let latest_atime: i64 = row.get(4)?;
            
            self.entries.push(DirEntry {
                name: component.clone(),
                path: format!("{}/{}", self.current_path, component),
                is_dir,
                total_size,
                file_count,
                latest_atime,
            });
        }
        
        let elapsed = start.elapsed().as_secs_f64();
        let filter_info = if self.filters.is_active() { " (filtered)" } else { "" };
        self.status = format!("{} entries in {:.2}s{}", self.entries.len(), elapsed, filter_info);
        self.loading = false;
        Ok(())
    }

    /// Start sort selection mode
    fn start_sort_select(&mut self) {
        self.pending_sort = self.sort_mode;
        self.input_mode = InputMode::SortSelect;
    }

    /// Cycle pending sort to next mode
    fn sort_select_next(&mut self) {
        self.pending_sort = self.pending_sort.next();
    }

    /// Cycle pending sort to previous mode
    fn sort_select_prev(&mut self) {
        self.pending_sort = self.pending_sort.prev();
    }

    /// Confirm sort selection and reload
    fn confirm_sort(&mut self) -> Result<()> {
        self.sort_mode = self.pending_sort;
        self.input_mode = InputMode::Normal;
        self.reload()
    }

    /// Cancel sort selection
    fn cancel_sort(&mut self) {
        self.pending_sort = self.sort_mode;
        self.input_mode = InputMode::Normal;
    }

    /// Reload the current view
    fn reload(&mut self) -> Result<()> {
        match self.view_mode {
            ViewMode::List => {
                if self.current_partition.is_none() {
                    self.load_partitions()?;
                } else {
                    self.load_directory()?;
                }
                // Preserve selection if possible
                if let Some(idx) = self.list_state.selected() {
                    if idx >= self.entries.len() && !self.entries.is_empty() {
                        self.list_state.select(Some(self.entries.len() - 1));
                    }
                }
            }
            ViewMode::Tree => {
                self.init_tree()?;
            }
        }
        Ok(())
    }

    /// Start input mode for a filter
    fn start_input(&mut self, mode: InputMode) {
        self.input_mode = mode;
        self.input_buffer.clear();
    }

    /// Cancel input mode
    fn cancel_input(&mut self) {
        self.input_mode = InputMode::Normal;
        self.input_buffer.clear();
    }

    /// Confirm input and apply filter
    fn confirm_input(&mut self) -> Result<()> {
        let value = self.input_buffer.trim().to_string();
        
        match self.input_mode {
            InputMode::Pattern => {
                if value.is_empty() {
                    self.filters.pattern = None;
                } else {
                    self.filters.pattern = Some(value);
                }
            }
            InputMode::OlderThan => {
                if value.is_empty() {
                    self.filters.older_than = None;
                } else if let Ok(days) = value.parse::<u64>() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    self.filters.older_than = Some(now - (days as i64 * 86400));
                } else {
                    self.status = format!("Invalid number: {}", value);
                    self.input_mode = InputMode::Normal;
                    self.input_buffer.clear();
                    return Ok(());
                }
            }
            InputMode::NewerThan => {
                if value.is_empty() {
                    self.filters.newer_than = None;
                } else if let Ok(days) = value.parse::<u64>() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    self.filters.newer_than = Some(now - (days as i64 * 86400));
                } else {
                    self.status = format!("Invalid number: {}", value);
                    self.input_mode = InputMode::Normal;
                    self.input_buffer.clear();
                    return Ok(());
                }
            }
            InputMode::MinSize => {
                if value.is_empty() {
                    self.filters.min_size = None;
                } else {
                    match parse_size(&value) {
                        Ok(size) => self.filters.min_size = Some(size),
                        Err(e) => {
                            self.status = e;
                            self.input_mode = InputMode::Normal;
                            self.input_buffer.clear();
                            return Ok(());
                        }
                    }
                }
            }
            InputMode::MaxSize => {
                if value.is_empty() {
                    self.filters.max_size = None;
                } else {
                    match parse_size(&value) {
                        Ok(size) => self.filters.max_size = Some(size),
                        Err(e) => {
                            self.status = e;
                            self.input_mode = InputMode::Normal;
                            self.input_buffer.clear();
                            return Ok(());
                        }
                    }
                }
            }
            InputMode::Normal | InputMode::SortSelect => {}
        }
        
        self.input_mode = InputMode::Normal;
        self.input_buffer.clear();
        self.reload()
    }

    /// Clear all filters
    fn clear_filters(&mut self) -> Result<()> {
        self.filters.clear();
        self.reload()
    }
    
    fn select_next(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) => (i + 1).min(self.entries.len() - 1),
            None => 0,
        };
        self.list_state.select(Some(i));
    }
    
    fn select_prev(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) => i.saturating_sub(1),
            None => 0,
        };
        self.list_state.select(Some(i));
    }
    
    fn enter_selected(&mut self) -> Result<()> {
        let Some(idx) = self.list_state.selected() else {
            return Ok(());
        };
        
        let entry = &self.entries[idx];
        
        if !entry.is_dir {
            return Ok(());
        }
        
        if entry.name == ".." {
            return self.go_up();
        }
        
        // If we're at the partition list, enter the partition
        if self.current_partition.is_none() {
            self.current_partition = Some(entry.name.clone());
            self.partition_root.clear();
            self.current_path.clear();
            self.load_directory()?;
        } else {
            // Enter subdirectory - use the full path from the entry
            self.current_path = format!("{}/{}", self.current_path, entry.name);
            self.load_directory()?;
        }
        
        self.list_state.select(Some(0));
        Ok(())
    }
    
    fn go_up(&mut self) -> Result<()> {
        if self.current_partition.is_none() {
            // Already at root
            return Ok(());
        }
        
        if self.current_path == self.partition_root || self.current_path.is_empty() {
            // Go back to partition list
            self.current_partition = None;
            self.partition_root.clear();
            self.current_path.clear();
            self.load_partitions()?;
        } else {
            // Go up one directory
            if let Some(pos) = self.current_path.rfind('/') {
                self.current_path = self.current_path[..pos].to_string();
            } else {
                self.current_path.clear();
            }
            self.load_directory()?;
        }
        
        self.list_state.select(Some(0));
        Ok(())
    }
    
    // ---- Tree (Miller columns) mode ----

    /// Create a Column for the partition list.
    fn make_partition_column(&self) -> Result<Column> {
        let glob = format!("{}/*/*.parquet", self.index_path.display());
        let filter_clause = self.filters.to_where_clause();
        let having_clause = if filter_clause.is_empty() {
            "HAVING partition IS NOT NULL AND partition != ''".to_string()
        } else {
            format!(
                "HAVING partition IS NOT NULL AND partition != '' AND SUM(CASE WHEN {} THEN 1 ELSE 0 END) > 0",
                filter_clause
            )
        };

        let sql = if self.filters.is_active() {
            format!(
                r#"
                SELECT 
                    regexp_extract(filename, '.*/([^/]+)/[^/]+\.parquet$', 1) as partition,
                    SUM(CASE WHEN {filter} THEN size ELSE 0 END) as total_size,
                    MAX(CASE WHEN {filter} THEN atime ELSE 0 END) as latest_atime,
                    SUM(CASE WHEN {filter} THEN 1 ELSE 0 END) as file_count
                FROM read_parquet('{glob}', filename=true)
                GROUP BY partition
                {having}
                ORDER BY {order}
                "#,
                filter = filter_clause,
                glob = glob,
                having = having_clause,
                order = self.sort_mode.to_partition_order_by()
            )
        } else {
            format!(
                r#"
                SELECT 
                    regexp_extract(filename, '.*/([^/]+)/[^/]+\.parquet$', 1) as partition,
                    SUM(size) as total_size,
                    MAX(atime) as latest_atime,
                    COUNT(*) as file_count
                FROM read_parquet('{glob}', filename=true)
                GROUP BY partition
                HAVING partition IS NOT NULL AND partition != ''
                ORDER BY {order}
                "#,
                glob = glob,
                order = self.sort_mode.to_partition_order_by()
            )
        };

        let mut entries = Vec::new();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let total_size: i64 = row.get(1)?;
            let latest_atime: i64 = row.get(2)?;
            let file_count: i64 = row.get(3)?;
            entries.push(DirEntry {
                name: name.clone(),
                path: name,
                is_dir: true,
                total_size,
                file_count,
                latest_atime,
            });
        }

        let mut list_state = ListState::default();
        if !entries.is_empty() {
            list_state.select(Some(0));
        }

        Ok(Column {
            title: "Partitions".to_string(),
            partition: None,
            partition_root: String::new(),
            path: String::new(),
            entries,
            list_state,
        })
    }

    /// Create a Column for a partition's root directory.
    fn make_partition_root_column(&self, partition: &str) -> Result<Column> {
        let partition_root = self.discover_partition_root(partition)?;
        self.make_directory_column_tree(partition, &partition_root, &partition_root)
    }

    /// Create a Column for a directory within a partition.
    fn make_directory_column_tree(
        &self,
        partition: &str,
        partition_root: &str,
        path: &str,
    ) -> Result<Column> {
        let glob = format!("{}/{}/*.parquet", self.index_path.display(), partition);
        let prefix = format!("{}/", path);
        let prefix_len = prefix.len();

        let filter_clause = self.filters.to_where_clause();
        let file_filter = if filter_clause.is_empty() {
            format!("path LIKE '{}%'", prefix)
        } else {
            format!("path LIKE '{}%' AND {}", prefix, filter_clause)
        };

        let order_by = self.sort_mode.to_order_by(self.sort_mode == SortMode::Name);

        let sql = format!(
            r#"
            WITH files AS (
                SELECT path, size, atime
                FROM read_parquet('{glob}')
                WHERE {file_filter}
            ),
            components AS (
                SELECT 
                    path, size, atime,
                    CASE 
                        WHEN position('/' IN substr(path, {prefix_len} + 1)) > 0 
                        THEN substr(path, {prefix_len} + 1, position('/' IN substr(path, {prefix_len} + 1)) - 1)
                        ELSE substr(path, {prefix_len} + 1)
                    END as component,
                    CASE 
                        WHEN position('/' IN substr(path, {prefix_len} + 1)) > 0 THEN true
                        ELSE false
                    END as is_dir
                FROM files
            )
            SELECT 
                component,
                bool_or(is_dir) as is_dir,
                SUM(size) as total_size,
                COUNT(*) as file_count,
                MAX(atime) as latest_atime
            FROM components
            WHERE component != '' AND component IS NOT NULL
            GROUP BY component
            ORDER BY {order_by}
            "#,
            glob = glob,
            file_filter = file_filter,
            prefix_len = prefix_len,
            order_by = order_by
        );

        let mut entries = Vec::new();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let component: String = row.get(0)?;
            let is_dir: bool = row.get(1)?;
            let total_size: i64 = row.get(2)?;
            let file_count: i64 = row.get(3)?;
            let latest_atime: i64 = row.get(4)?;
            entries.push(DirEntry {
                name: component.clone(),
                path: format!("{}/{}", path, component),
                is_dir,
                total_size,
                file_count,
                latest_atime,
            });
        }

        let title = path.rsplit('/').next().unwrap_or(partition).to_string();

        let mut list_state = ListState::default();
        if !entries.is_empty() {
            list_state.select(Some(0));
        }

        Ok(Column {
            title,
            partition: Some(partition.to_string()),
            partition_root: partition_root.to_string(),
            path: path.to_string(),
            entries,
            list_state,
        })
    }

    /// Initialize tree mode columns from scratch.
    fn init_tree(&mut self) -> Result<()> {
        self.columns.clear();
        self.active_column = 0;

        let partition_col = self.make_partition_column()?;
        self.columns.push(partition_col);

        // Auto-expand preview for the selected partition
        self.tree_update_preview()?;
        Ok(())
    }

    /// Update the preview column (active_column + 1) based on the active column's selection.
    /// Truncates any columns beyond the preview.
    fn tree_update_preview(&mut self) -> Result<()> {
        // Remove everything after the active column
        self.columns.truncate(self.active_column + 1);

        // Extract info from active column's selection (avoid borrow conflict)
        let preview_info = {
            let col = &self.columns[self.active_column];
            if let Some(idx) = col.list_state.selected() {
                if idx < col.entries.len() {
                    let entry = &col.entries[idx];
                    if entry.is_dir {
                        Some((
                            col.partition.is_none(),
                            entry.name.clone(),
                            col.partition.clone(),
                            col.partition_root.clone(),
                            col.path.clone(),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some((is_partition_list, name, partition, partition_root, path)) = preview_info {
            let new_col = if is_partition_list {
                self.make_partition_root_column(&name)?
            } else {
                let part = partition.as_ref().unwrap();
                let new_path = format!("{}/{}", path, name);
                self.make_directory_column_tree(part, &partition_root, &new_path)?
            };
            self.columns.push(new_col);
        }

        Ok(())
    }

    /// Move selection down in the active tree column and update preview.
    fn tree_select_next(&mut self) -> Result<()> {
        if self.columns.is_empty() {
            return Ok(());
        }
        let col = &mut self.columns[self.active_column];
        if col.entries.is_empty() {
            return Ok(());
        }
        let i = match col.list_state.selected() {
            Some(i) => (i + 1).min(col.entries.len() - 1),
            None => 0,
        };
        col.list_state.select(Some(i));
        self.tree_update_preview()
    }

    /// Move selection up in the active tree column and update preview.
    fn tree_select_prev(&mut self) -> Result<()> {
        if self.columns.is_empty() {
            return Ok(());
        }
        let col = &mut self.columns[self.active_column];
        if col.entries.is_empty() {
            return Ok(());
        }
        let i = match col.list_state.selected() {
            Some(i) => i.saturating_sub(1),
            None => 0,
        };
        col.list_state.select(Some(i));
        self.tree_update_preview()
    }

    /// Move focus right into the preview column.
    fn tree_right(&mut self) -> Result<()> {
        if self.active_column + 1 < self.columns.len() {
            self.active_column += 1;
            self.tree_update_preview()?;
        }
        Ok(())
    }

    /// Move focus left to the parent column.
    fn tree_left(&mut self) -> Result<()> {
        if self.active_column > 0 {
            self.active_column -= 1;
            // Keep active column + its preview, truncate the rest
            self.columns.truncate(self.active_column + 2);
        }
        Ok(())
    }

    /// Toggle between list and tree view modes.
    fn toggle_view_mode(&mut self) -> Result<()> {
        match self.view_mode {
            ViewMode::List => {
                self.view_mode = ViewMode::Tree;
                self.init_tree()?;
            }
            ViewMode::Tree => {
                self.view_mode = ViewMode::List;
                // Reset list mode to partition view
                self.current_partition = None;
                self.partition_root.clear();
                self.current_path.clear();
                self.load_partitions()?;
                if !self.entries.is_empty() {
                    self.list_state.select(Some(0));
                }
            }
        }
        Ok(())
    }

    fn format_atime(&self, atime: i64) -> String {
        if atime == 0 {
            return String::new();
        }
        
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        let time = UNIX_EPOCH + Duration::from_secs(atime as u64);
        let now = SystemTime::now();
        
        if let Ok(duration) = now.duration_since(time) {
            let days = duration.as_secs() / 86400;
            if days == 0 {
                "today".to_string()
            } else if days == 1 {
                "1 day ago".to_string()
            } else if days < 30 {
                format!("{} days ago", days)
            } else if days < 365 {
                let months = days / 30;
                if months == 1 {
                    "1 month ago".to_string()
                } else {
                    format!("{} months ago", months)
                }
            } else {
                let years = days / 365;
                if years == 1 {
                    "1 year ago".to_string()
                } else {
                    format!("{} years ago", years)
                }
            }
        } else {
            "future".to_string()
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    // Resolve index path
    let index_path = args.index.canonicalize()
        .with_context(|| format!("Index directory not found: {}", args.index.display()))?;
    
    // Parse sort mode
    let sort_mode: SortMode = args.sort.parse()
        .map_err(|e: String| anyhow::anyhow!(e))?;
    
    // Build filters from CLI args
    let filters = QueryFilters::new()
        .with_pattern(args.pattern)
        .with_older_than(args.older_than)
        .with_newer_than(args.newer_than)
        .with_min_size(args.min_size.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?
        .with_max_size(args.max_size.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?;
    
    // Connect to DuckDB
    let conn = Connection::open_in_memory()?;
    
    // Initialize app
    let mut app = App::new(conn, index_path, args.partition, filters, sort_mode)?;
    
    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    
    // Main loop
    let result = run_app(&mut terminal, &mut app);
    
    // Restore terminal
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    
    result
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;
        
        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                
                // Handle sort selection mode
                if app.input_mode == InputMode::SortSelect {
                    match key.code {
                        KeyCode::Esc => app.cancel_sort(),
                        KeyCode::Enter | KeyCode::Char(' ') => {
                            if let Err(e) = app.confirm_sort() {
                                app.status = format!("Error: {}", e);
                            }
                        }
                        KeyCode::Up | KeyCode::Char('k') | KeyCode::Left => {
                            app.sort_select_prev();
                        }
                        KeyCode::Down | KeyCode::Char('j') | KeyCode::Right | KeyCode::Char('s') => {
                            app.sort_select_next();
                        }
                        _ => {}
                    }
                    continue;
                }

                // Handle text input mode
                if app.input_mode != InputMode::Normal {
                    match key.code {
                        KeyCode::Esc => app.cancel_input(),
                        KeyCode::Enter => {
                            if let Err(e) = app.confirm_input() {
                                app.status = format!("Error: {}", e);
                            }
                        }
                        KeyCode::Backspace => {
                            app.input_buffer.pop();
                        }
                        KeyCode::Char(c) => {
                            app.input_buffer.push(c);
                        }
                        _ => {}
                    }
                    continue;
                }
                
                // Normal mode — shared keys first, then mode-specific navigation
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    // Toggle view mode
                    KeyCode::Char('t') => {
                        if let Err(e) = app.toggle_view_mode() {
                            app.status = format!("Error: {}", e);
                        }
                    }
                    // Sort mode selection (both modes)
                    KeyCode::Char('s') => app.start_sort_select(),
                    // Filter inputs (both modes)
                    KeyCode::Char('/') => app.start_input(InputMode::Pattern),
                    KeyCode::Char('o') => app.start_input(InputMode::OlderThan),
                    KeyCode::Char('n') => app.start_input(InputMode::NewerThan),
                    KeyCode::Char('>') => app.start_input(InputMode::MinSize),
                    KeyCode::Char('<') => app.start_input(InputMode::MaxSize),
                    // Clear filters (both modes)
                    KeyCode::Char('c') => {
                        if let Err(e) = app.clear_filters() {
                            app.status = format!("Error: {}", e);
                        }
                    }
                    // Mode-specific navigation
                    _ => match app.view_mode {
                        ViewMode::List => match key.code {
                            KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                            KeyCode::Up | KeyCode::Char('k') => app.select_prev(),
                            KeyCode::Enter | KeyCode::Right | KeyCode::Char(' ') => {
                                if let Err(e) = app.enter_selected() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            KeyCode::Left | KeyCode::Backspace => {
                                if let Err(e) = app.go_up() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            _ => {}
                        },
                        ViewMode::Tree => match key.code {
                            KeyCode::Down | KeyCode::Char('j') => {
                                if let Err(e) = app.tree_select_next() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                if let Err(e) = app.tree_select_prev() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            KeyCode::Right | KeyCode::Enter | KeyCode::Char(' ') => {
                                if let Err(e) = app.tree_right() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            KeyCode::Left | KeyCode::Backspace => {
                                if let Err(e) = app.tree_left() {
                                    app.status = format!("Error: {}", e);
                                }
                            }
                            _ => {}
                        },
                    }
                }
            }
        }
    }
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),     // Content
            Constraint::Length(1),  // Status bar
        ])
        .split(f.area());

    // Render content based on view mode
    match app.view_mode {
        ViewMode::List => render_list_content(f, app, chunks[0]),
        ViewMode::Tree => render_tree_content(f, app, chunks[0]),
    }

    // Shared status bar
    render_status_bar(f, app, chunks[1]);
}

/// Render the traditional single-list view.
fn render_list_content(f: &mut Frame, app: &App, area: Rect) {
    // Build filter display string
    let filter_display = app.filters.format_display();
    let filter_suffix = if filter_display.is_empty() {
        String::new()
    } else {
        format!(" {}", filter_display)
    };

    // Title with current path
    let title = if let Some(ref partition) = app.current_partition {
        let display_path = if app.current_path.len() > app.partition_root.len() {
            &app.current_path[app.partition_root.len()..]
        } else {
            ""
        };
        if app.loading {
            format!(" {}{} (loading...){} ", partition, display_path, filter_suffix)
        } else {
            format!(" {}{}{} ", partition, display_path, filter_suffix)
        }
    } else {
        if app.loading {
            format!(" Partitions (loading...){} ", filter_suffix)
        } else {
            format!(" Partitions{} ", filter_suffix)
        }
    };

    // Pre-compute formatted strings and find max widths dynamically
    let formatted: Vec<(String, String, String, String)> = app.entries.iter().map(|entry| {
        let prefix = if entry.is_dir && entry.name != ".." {
            "▸ "
        } else if entry.name == ".." {
            "◂ "
        } else {
            "  "
        };

        let name = format!("{}{}", prefix, entry.name);

        let size_str = if entry.name == ".." {
            String::new()
        } else {
            format_bytes(entry.total_size as u64)
        };

        let count_str = if entry.name == ".." {
            String::new()
        } else {
            format_file_count(entry.file_count)
        };

        let atime_str = if entry.name == ".." {
            String::new()
        } else {
            app.format_atime(entry.latest_atime)
        };

        (name, size_str, count_str, atime_str)
    }).collect();

    // Calculate dynamic column widths based on content (with minimum widths)
    let size_width = formatted.iter().map(|(_, s, _, _)| s.len()).max().unwrap_or(0).max(10);
    let count_width = formatted.iter().map(|(_, _, c, _)| c.len()).max().unwrap_or(0).max(8);
    let atime_width = formatted.iter().map(|(_, _, _, a)| a.len()).max().unwrap_or(0).max(12);

    // Calculate available width for names
    let area_width = area.width.saturating_sub(2) as usize; // Account for borders
    let fixed_cols = size_width + count_width + atime_width + 8; // 8 = spacing between columns + highlight symbol
    let name_width = area_width.saturating_sub(fixed_cols);

    // Entry list
    let items: Vec<ListItem> = formatted.iter().map(|(name, size_str, count_str, atime_str)| {
        let name_display = if name.len() > name_width {
            format!("{}…", &name[..name_width.saturating_sub(1)])
        } else {
            name.clone()
        };

        let line = format!(
            "{:<name_width$}  {:>size_width$}  {:>count_width$}  {:>atime_width$}",
            name_display,
            size_str,
            count_str,
            atime_str,
            name_width = name_width,
            size_width = size_width,
            count_width = count_width,
            atime_width = atime_width
        );

        ListItem::new(line)
    }).collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_symbol("▶ ")
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut app.list_state.clone());
}

/// Minimum column width for Miller columns view.
const MIN_COLUMN_WIDTH: u16 = 20;
/// Maximum number of visible columns.
const MAX_VISIBLE_COLUMNS: usize = 5;

/// Render the Miller columns (tree) view.
fn render_tree_content(f: &mut Frame, app: &App, area: Rect) {
    if app.columns.is_empty() {
        return;
    }

    // Calculate how many columns fit in the available width
    let available_width = area.width;
    let num_visible = app.columns.len()
        .min(MAX_VISIBLE_COLUMNS)
        .min((available_width / MIN_COLUMN_WIDTH) as usize)
        .max(1);

    // Ensure the active column is always visible — show columns ending at or after active
    let start_idx = if app.active_column + 2 > num_visible {
        // Show active column and (ideally) its preview
        (app.active_column + 2).saturating_sub(num_visible).min(app.columns.len().saturating_sub(num_visible))
    } else {
        0
    };
    let end_idx = (start_idx + num_visible).min(app.columns.len());
    let visible_columns = &app.columns[start_idx..end_idx];
    let actual_visible = visible_columns.len();

    // Create horizontal layout with equal widths
    let constraints: Vec<Constraint> = (0..actual_visible)
        .map(|_| Constraint::Ratio(1, actual_visible as u32))
        .collect();

    let col_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(area);

    for (i, col) in visible_columns.iter().enumerate() {
        let global_idx = start_idx + i;
        let is_active = global_idx == app.active_column;
        let is_preview = global_idx == app.active_column + 1;

        let border_style = if is_active {
            Style::default().fg(Color::Blue)
        } else if is_preview {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let col_inner_width = col_chunks[i].width.saturating_sub(2) as usize; // borders
        let highlight_width = 2; // "▶ "
        let content_width = col_inner_width.saturating_sub(highlight_width);

        // Format entries: name left, size right
        let items: Vec<ListItem> = col.entries.iter().map(|entry| {
            let prefix = if entry.is_dir { "▸" } else { " " };
            let size_str = format_bytes(entry.total_size as u64);
            let size_display_len = size_str.len();

            // Space for: prefix(1) + space(1) + name + space(1) + size
            let name_max = content_width.saturating_sub(size_display_len + 3);

            let name = &entry.name;
            let name_display = if name.len() > name_max && name_max > 1 {
                format!("{}…", &name[..name_max.saturating_sub(1)])
            } else if name_max == 0 {
                String::new()
            } else {
                name.clone()
            };

            let padding = content_width.saturating_sub(2 + name_display.len() + size_display_len);
            let line = format!("{} {}{:>pad$}{}", prefix, name_display, "", size_str, pad = padding);

            ListItem::new(line)
        }).collect();

        let title_style = if is_active {
            Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(Span::styled(format!(" {} ", col.title), title_style))
            )
            .highlight_symbol("▶ ")
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

        f.render_stateful_widget(list, col_chunks[i], &mut col.list_state.clone());
    }
}

/// Render the shared status bar.
fn render_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let status_text = if app.input_mode == InputMode::SortSelect {
        // Build sort selector display with short names
        let options: Vec<String> = SortMode::ALL
            .iter()
            .map(|m| {
                if *m == app.pending_sort {
                    format!("▶ {} ◀", m)
                } else {
                    format!("  {}  ", m)
                }
            })
            .collect();
        format!(" Sort: {}  (s/→:next  ←:prev  Enter:apply  Esc:cancel)", options.join(""))
    } else if app.input_mode != InputMode::Normal {
        format!(" {}{}", app.input_mode.prompt(), app.input_buffer)
    } else {
        // Build mode-specific status
        let mode_indicator = match app.view_mode {
            ViewMode::List => "list",
            ViewMode::Tree => "tree",
        };

        // In tree mode, show breadcrumb and selected entry info
        let context_info = if app.view_mode == ViewMode::Tree && !app.columns.is_empty() {
            // Build breadcrumb from column titles
            let breadcrumb: Vec<&str> = app.columns.iter()
                .take(app.active_column + 1)
                .map(|c| c.title.as_str())
                .collect();
            let path_str = breadcrumb.join(" > ");

            // Selected entry details
            let col = &app.columns[app.active_column];
            if let Some(idx) = col.list_state.selected() {
                if idx < col.entries.len() {
                    let entry = &col.entries[idx];
                    let atime_str = app.format_atime(entry.latest_atime);
                    format!(
                        "{} │ {} │ {} │ {}",
                        path_str,
                        format_bytes(entry.total_size as u64),
                        format_file_count(entry.file_count),
                        atime_str
                    )
                } else {
                    path_str
                }
            } else {
                path_str
            }
        } else {
            app.status.clone()
        };

        let filter_display = app.filters.format_display();
        let filter_str = if filter_display.is_empty() {
            String::new()
        } else {
            format!(" {}", filter_display)
        };

        format!(
            " {}{} │ sort:{} mode:{} │ q:quit jk↑↓:nav ←→:cd /:pattern s:sort t:mode c:clear",
            context_info,
            filter_str,
            app.sort_mode,
            mode_indicator
        )
    };

    let status_style = if app.input_mode == InputMode::SortSelect {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else if app.input_mode != InputMode::Normal {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::DIM)
    };

    let status = Paragraph::new(status_text).style(status_style);
    f.render_widget(status, area);
}
