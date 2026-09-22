use anyhow::{Context, Result};
use sqlx::{PgPool, SqlitePool};
use tracing::info;

/// A single database migration.
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    pub sql: &'static str,
}

/// Return ordered PostgreSQL migrations (embedded at compile time).
pub fn postgres_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: 1,
            name: "initial_schema",
            sql: include_str!("../migrations/postgres/001_initial_schema.sql"),
        },
        Migration {
            version: 2,
            name: "backfill_nodes_columns",
            sql: include_str!("../migrations/postgres/002_backfill_nodes_columns.sql"),
        },
        Migration {
            version: 3,
            name: "add_noise_key",
            sql: include_str!("../migrations/postgres/003_add_noise_key.sql"),
        },
        Migration {
            version: 4,
            name: "add_settlements",
            sql: include_str!("../migrations/postgres/004_add_settlements.sql"),
        },
    ]
}

/// Return ordered SQLite migrations (embedded at compile time).
pub fn sqlite_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: 1,
            name: "initial_schema",
            sql: include_str!("../migrations/sqlite/001_initial_schema.sql"),
        },
        Migration {
            version: 2,
            name: "backfill_nodes_columns",
            sql: include_str!("../migrations/sqlite/002_backfill_nodes_columns.sql"),
        },
        Migration {
            version: 3,
            name: "add_noise_key",
            sql: include_str!("../migrations/sqlite/003_add_noise_key.sql"),
        },
        Migration {
            version: 4,
            name: "add_settlements",
            sql: include_str!("../migrations/sqlite/004_add_settlements.sql"),
        },
    ]
}

/// Split a multi-statement SQL string into individual statements.
///
/// Handles:
/// - Standard semicolon-delimited statements
/// - Single-line (`--`) and multi-line (`/* ... */`) comments
/// - PostgreSQL dollar-quoted strings (`$$ ... $$`)
/// - Quoted identifiers and strings containing semicolons
///
/// Returns only non-empty statements (whitespace-only strings are skipped).
fn split_sql(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            // Single-line comment: skip until newline
            '-' if chars.peek() == Some(&'-') => {
                chars.next(); // consume second '-'
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
            }
            // Block comment: skip until */
            '/' if chars.peek() == Some(&'*') => {
                chars.next(); // consume '*'
                let mut prev = '*';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            // Dollar-quoted string: skip until closing dollar tag
            '$' => {
                current.push(ch);
                // Read the opening tag
                let mut tag = String::from("$");
                for c in chars.by_ref() {
                    tag.push(c);
                    if c == '$' {
                        break;
                    }
                }
                // Skip until closing tag
                let mut buf = String::new();
                for c in chars.by_ref() {
                    buf.push(c);
                    if buf.ends_with(&tag) {
                        current.push_str(&buf);
                        break;
                    }
                }
            }
            // Single-quoted string
            '\'' => {
                current.push(ch);
                loop {
                    match chars.next() {
                        Some('\'') => {
                            current.push('\'');
                            // Check for escaped quote ('')
                            if chars.peek() == Some(&'\'') {
                                current.push('\'');
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        Some(c) => current.push(c),
                        None => break,
                    }
                }
            }
            // Double-quoted identifier
            '"' => {
                current.push(ch);
                while let Some(c) = chars.next() {
                    current.push(c);
                    if c == '"' {
                        // Check for escaped quote ("")
                        if chars.peek() == Some(&'"') {
                            current.push('"');
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
            }
            // Semicolon: statement boundary
            ';' => {
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                current.clear();
            }
            // Regular character
            _ => current.push(ch),
        }
    }

    // Flush any remaining statement
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }

    statements
}

/// Run PostgreSQL migrations against a pool.
///
/// Creates a `schema_migrations` tracking table if it doesn't exist,
/// then applies each pending migration in order.
pub async fn run_postgres(pool: &PgPool, migrations: &[Migration]) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ DEFAULT now()
        )",
    )
    .execute(pool)
    .await
    .context("failed to create schema_migrations table")?;

    for migration in migrations {
        let applied: Option<(i32,)> = sqlx::query_as(
            "SELECT version FROM schema_migrations WHERE version = $1",
        )
        .bind(migration.version)
        .fetch_optional(pool)
        .await
        .context("failed to check migration status")?;

        if applied.is_some() {
            continue;
        }

        info!(
            version = migration.version,
            name = migration.name,
            "applying postgres migration"
        );

        // Apply each migration atomically so a mid-migration failure cannot
        // leave the schema half-migrated. Postgres migration SQL uses
        // ADD COLUMN IF NOT EXISTS / idempotent ALTERs, so any error here is
        // a genuine failure: the migration is left unrecorded and retried on
        // the next boot.
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin postgres migration transaction")?;
        for stmt in split_sql(migration.sql) {
            sqlx::query(&stmt)
                .execute(&mut *tx)
                .await
                .with_context(|| {
                    format!(
                        "failed to execute migration {} (v{}): {}",
                        migration.name, migration.version, stmt
                    )
                })?;
        }
        tx.commit()
            .await
            .context("failed to commit postgres migration")?;

        sqlx::query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)")
            .bind(migration.version)
            .bind(migration.name)
            .execute(pool)
            .await
            .context("failed to record migration")?;
    }

    Ok(())
}

/// Run SQLite migrations against a pool.
///
/// Creates a `schema_migrations` tracking table if it doesn't exist,
/// then applies each pending migration in order.
pub async fn run_sqlite(pool: &SqlitePool, migrations: &[Migration]) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT DEFAULT (datetime('now'))
        )",
    )
    .execute(pool)
    .await
    .context("failed to create schema_migrations table")?;

    for migration in migrations {
        let applied: Option<(i32,)> = sqlx::query_as(
            "SELECT version FROM schema_migrations WHERE version = ?",
        )
        .bind(migration.version)
        .fetch_optional(pool)
        .await
        .context("failed to check migration status")?;

        if applied.is_some() {
            continue;
        }

        info!(
            version = migration.version,
            name = migration.name,
            "applying sqlite migration"
        );

        // Apply each migration inside a transaction (SQLite DDL is
        // transactional), so a genuine failure rolls back and the migration
        // stays unrecorded for the next boot to retry.
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin sqlite migration transaction")?;
        for stmt in split_sql(migration.sql) {
            if let Err(e) = sqlx::query(&stmt).execute(&mut *tx).await {
                // SQLite has no ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
                // Migrations 002/003 backfill columns for databases created
                // by older schema versions, so "duplicate column name" errors
                // are expected when the column already exists. Tolerate ONLY
                // that exact failure class; anything else is a real error and
                // aborts the migration.
                if is_duplicate_column(&e) {
                    tracing::debug!(
                        migration = migration.name,
                        version = migration.version,
                        stmt = %stmt,
                        "column already exists (idempotent backfill)"
                    );
                } else {
                    tx.rollback().await.ok();
                    return Err(e).with_context(|| {
                        format!(
                            "failed to execute migration {} (v{}): {}",
                            migration.name, migration.version, stmt
                        )
                    });
                }
            }
        }
        tx.commit()
            .await
            .context("failed to commit sqlite migration")?;

        sqlx::query("INSERT INTO schema_migrations (version, name) VALUES (?, ?)")
            .bind(migration.version)
            .bind(migration.name)
            .execute(pool)
            .await
            .context("failed to record migration")?;
    }

    Ok(())
}

/// Returns true when the error is SQLite's "duplicate column name" error.
///
/// SQLite has no `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so legacy
/// column-backfill migrations rely on this failure class for idempotency.
fn is_duplicate_column(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db) => db
            .message()
            .to_lowercase()
            .contains("duplicate column name"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_sql_basic() {
        let sql = "CREATE TABLE foo (id INT); CREATE TABLE bar (id INT);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE foo (id INT)");
        assert_eq!(stmts[1], "CREATE TABLE bar (id INT)");
    }

    #[test]
    fn split_sql_skips_comments() {
        let sql = "-- This is a comment\nCREATE TABLE foo (id INT);\n/* block comment */\nCREATE TABLE bar (id INT);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn split_sql_handles_semicolons_in_strings() {
        let sql = "INSERT INTO foo (val) VALUES ('hello;world'); CREATE TABLE bar (id INT);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("hello;world"));
    }

    #[test]
    fn split_sql_handles_dollar_quotes() {
        let sql = "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN RAISE NOTICE 'semi;colon'; END; $$ LANGUAGE plpgsql; CREATE TABLE bar (id INT);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn split_sql_empty_input() {
        let stmts = split_sql("");
        assert!(stmts.is_empty());
    }

    #[test]
    fn split_sql_whitespace_only() {
        let stmts = split_sql("   \n  \t  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn split_sql_trailing_semicolon() {
        let sql = "CREATE TABLE foo (id INT);";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE foo (id INT)");
    }

    #[test]
    fn split_sql_no_trailing_semicolon() {
        let sql = "CREATE TABLE foo (id INT)";
        let stmts = split_sql(sql);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE foo (id INT)");
    }
}
