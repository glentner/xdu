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

use xdu::format_bytes;

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
#[command(name = "xdu-view", about = "Interactive TUI for exploring a file metadata index")]
struct Args {
    /// Path to the Parquet index directory
    #[arg(short, long, value_name = "DIR")]
    index: PathBuf,

    /// Initial partition to view (optional, shows partition list if omitted)
    #[arg(short, long, value_name = "NAME")]
    partition: Option<String>,
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
}

impl App {
    fn new(conn: Connection, index_path: PathBuf, initial_partition: Option<String>) -> Result<Self> {
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
        
        // Query using filename to extract partition from the parquet file path
        // The partition name is the directory containing the parquet file
        let sql = format!(
            r#"
            SELECT 
                regexp_extract(filename, '.*/([^/]+)/[^/]+\.parquet$', 1) as partition,
                SUM(size) as total_size,
                MAX(atime) as latest_atime,
                COUNT(*) as file_count
            FROM read_parquet('{}', filename=true)
            GROUP BY partition
            HAVING partition IS NOT NULL AND partition != ''
            ORDER BY partition
            "#,
            glob
        );
        
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
        let sql = format!(
            r#"
            WITH files AS (
                SELECT 
                    path,
                    size,
                    atime
                FROM read_parquet('{}')
                WHERE path LIKE '{}%'
            ),
            components AS (
                SELECT 
                    path,
                    size,
                    atime,
                    CASE 
                        WHEN position('/' IN substr(path, {} + 1)) > 0 
                        THEN substr(path, {} + 1, position('/' IN substr(path, {} + 1)) - 1)
                        ELSE substr(path, {} + 1)
                    END as component,
                    CASE 
                        WHEN position('/' IN substr(path, {} + 1)) > 0 THEN true
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
            ORDER BY bool_or(is_dir) DESC, component
            "#,
            glob,
            prefix,
            prefix_len,
            prefix_len,
            prefix_len,
            prefix_len,
            prefix_len
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
        self.status = format!("{} entries in {:.2}s", self.entries.len(), elapsed);
        self.loading = false;
        Ok(())
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
    
    // Connect to DuckDB
    let conn = Connection::open_in_memory()?;
    
    // Initialize app
    let mut app = App::new(conn, index_path, args.partition)?;
    
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
                
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
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
                }
            }
        }
    }
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Min(1),     // List
            Constraint::Length(1),  // Status bar
        ])
        .split(f.area());
    
    // Header with current path
    let title = if let Some(ref partition) = app.current_partition {
        // Show partition name and the relative path within it
        let display_path = if app.current_path.len() > app.partition_root.len() {
            &app.current_path[app.partition_root.len()..]
        } else {
            ""
        };
        format!(" {}{} ", partition, display_path)
    } else {
        " Partitions ".to_string()
    };
    
    let header_block = Block::default()
        .borders(Borders::ALL)
        .title(title);
    
    let loading_indicator = if app.loading { " Loading..." } else { "" };
    let header = Paragraph::new(loading_indicator)
        .block(header_block);
    f.render_widget(header, chunks[0]);
    
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
    let area_width = chunks[1].width.saturating_sub(2) as usize; // Account for borders
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
        .block(Block::default().borders(Borders::ALL))
        .highlight_symbol("▶ ")
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    
    f.render_stateful_widget(list, chunks[1], &mut app.list_state.clone());
    
    // Status bar
    let status = Paragraph::new(format!(" {} │ q:quit  ↑↓/jk:navigate  ←→/space/enter:open/back", app.status))
        .style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(status, chunks[2]);
}
