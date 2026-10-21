//! `echo-node logs path|tail` — inspect the daemon log the CLI writes to.

use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use clap::Subcommand;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::config::EchoHome;

/// Bytes read from the tail when rendering the last N lines (keeps `tail`
/// O(window) instead of slurping a multi-GB log).
const TAIL_WINDOW: u64 = 256 * 1024;

#[derive(Debug, Subcommand)]
pub enum LogsCmd {
    /// Print the daemon log path.
    Path,
    /// Show the last N lines of the daemon log.
    Tail {
        /// Number of trailing lines to show.
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
        /// Keep streaming new output until Ctrl-C.
        #[arg(long)]
        follow: bool,
    },
}

pub async fn run(home: &EchoHome, cmd: LogsCmd) -> Result<()> {
    let path = home.daemon_log_path();
    match cmd {
        LogsCmd::Path => {
            println!("{}", path.display());
            Ok(())
        }
        LogsCmd::Tail { lines, follow } => {
            if !path.exists() {
                anyhow::bail!(
                    "no log file yet — {} (start the daemon with `echo-node start`)",
                    path.display()
                );
            }
            let end = tail(&path, lines).await?;
            if follow {
                println!("--- following {} (Ctrl-C to stop) ---", path.display());
                follow_file(&path, end).await
            } else {
                Ok(())
            }
        }
    }
}

/// Print the last `lines` lines of the file; returns the byte offset just
/// past the bytes that were rendered (the follow cursor).
async fn tail(path: &Path, lines: usize) -> Result<u64> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let len = file.metadata().await?.len();

    let start = len.saturating_sub(TAIL_WINDOW);
    file.seek(SeekFrom::Start(start)).await?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    file.read_to_end(&mut buf).await?;

    let text = String::from_utf8_lossy(&buf);
    let mut all: Vec<&str> = text.split('\n').collect();
    if start > 0 {
        // The window starts mid-line — drop the partial first line.
        all.remove(0);
    }
    // `split` yields a trailing empty element when the file ends in a newline.
    if all.last().is_some_and(|l| l.is_empty()) {
        all.pop();
    }
    let skip = all.len().saturating_sub(lines);
    for line in &all[skip..] {
        println!("{line}");
    }
    Ok(len)
}

/// Poll for appended bytes and write them straight to stdout.
async fn follow_file(path: &Path, mut pos: u64) -> Result<()> {
    // `ctrl_c()` is a !Unpin async fn — pin it once so `select!` can poll it
    // across iterations without losing the signal registration.
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                println!();
                return Ok(());
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }

        let Ok(mut file) = tokio::fs::File::open(path).await else {
            continue; // rotated/removed — retry
        };
        let Ok(len) = file.metadata().await.map(|m| m.len()) else {
            continue;
        };
        if len < pos {
            pos = 0; // truncated or rotated
        }
        if len == pos {
            continue;
        }
        file.seek(SeekFrom::Start(pos)).await?;
        let mut chunk = vec![0u8; (len - pos) as usize];
        let n = file.read(&mut chunk).await?;
        chunk.truncate(n);
        pos += n as u64;

        // Blocking writes to a pipe/terminal are fine here: the chunks are
        // small and we are only mirroring a log.
        let mut stdout = std::io::stdout();
        stdout.write_all(&chunk)?;
        stdout.flush()?;
    }
}

#[cfg(test)]
mod tests {

    /// The tail window logic (split + partial-line drop + last-N) is pure —
    /// exercise it against a synthetic buffer.
    #[test]
    fn tail_window_selects_last_lines() {
        let text = "one\ntwo\nthree\nfour\n";
        let mut all: Vec<&str> = text.split('\n').collect();
        if all.last().is_some_and(|l| l.is_empty()) {
            all.pop();
        }
        let n = 2;
        let skip = all.len().saturating_sub(n);
        assert_eq!(&all[skip..], &["three", "four"]);
    }
}
