use std::collections::HashMap;

use duckdb::Connection;
use lambda_runtime::{service_fn, LambdaEvent};
use serde::Deserialize;
use tokio_postgres::NoTls;

#[derive(Deserialize)]
struct Config {
    postgres_host: String,
    postgres_port: u16,
    postgres_user: String,
    postgres_password: String,
    postgres_db: String,
    minio_endpoint: String,
    minio_access_key: String,
    minio_secret_key: String,
    minio_bucket: String,
}

async fn process_cdc() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config: Config = envy::from_env()?;

    // Connect to PostgreSQL and read checkpoints
    let pg_conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        config.postgres_host, config.postgres_port, config.postgres_user,
        config.postgres_password, config.postgres_db
    );
    let (pg_client, pg_conn) = tokio_postgres::connect(&pg_conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            eprintln!("postgres connection error: {e}");
        }
    });

    let checkpoint_rows = pg_client
        .query("SELECT table_name, snapshot_id FROM checkpoints", &[])
        .await?;

    let checkpoints: HashMap<String, i64> = checkpoint_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let snap: i64 = row.get(1);
            (name, snap)
        })
        .collect();

    if checkpoints.is_empty() {
        println!("No checkpoints found — nothing to track");
        return Ok(());
    }

    println!("Loaded {} checkpoint(s)", checkpoints.len());

    // Set up DuckDB with DuckLake
    let db = Connection::open_in_memory()?;
    db.execute_batch(
        "INSTALL ducklake; INSTALL httpfs; INSTALL postgres;
         LOAD ducklake; LOAD httpfs; LOAD postgres;",
    )?;

    db.execute_batch(&format!(
        "CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        )",
        access_key = config.minio_access_key,
        secret_key = config.minio_secret_key,
        endpoint = config.minio_endpoint,
    ))?;

    db.execute_batch(&format!(
        "ATTACH 'ducklake:postgres:dbname={db} user={user} password={password} host={host} port={port}'
         AS my_lake (DATA_PATH 's3://{bucket}/')",
        db = config.postgres_db,
        user = config.postgres_user,
        password = config.postgres_password,
        host = config.postgres_host,
        port = config.postgres_port,
        bucket = config.minio_bucket,
    ))?;

    // Get latest snapshot ID
    let latest_snapshot: i64 = db
        .prepare("SELECT MAX(snapshot_id) FROM ducklake_snapshots('my_lake')")?
        .query_row([], |row| row.get(0))?;

    println!("Latest DuckLake snapshot: {latest_snapshot}");

    // Check each tracked table for changes
    for (table_name, last_snapshot) in &checkpoints {
        if !has_new_snapshots(latest_snapshot, *last_snapshot) {
            println!("[{table_name}] No new snapshots (at {last_snapshot})");
            continue;
        }

        println!(
            "[{table_name}] New changes: snapshot {last_snapshot} -> {latest_snapshot}"
        );

        let query = format!(
            "SELECT * FROM ducklake_table_changes('my_lake', '{table_name}', {last_snapshot}, {latest_snapshot})"
        );
        let mut stmt = db.prepare(&query)?;
        let column_names: Vec<String> = stmt.column_names();
        let column_count = column_names.len();

        let rows = stmt.query_map([], |row| {
            let values: Vec<String> = (0..column_count)
                .map(|i| {
                    row.get::<_, String>(i)
                        .unwrap_or_else(|_| "NULL".to_string())
                })
                .collect();
            Ok(values)
        })?;

        for row_result in rows {
            let values = row_result?;
            println!("  {}", format_change_row(&column_names, &values));
        }

        // Update checkpoint
        pg_client
            .execute(
                "UPDATE checkpoints SET snapshot_id = $1 WHERE table_name = $2",
                &[&latest_snapshot, table_name],
            )
            .await?;

        println!("[{table_name}] Checkpoint updated to {latest_snapshot}");
    }

    Ok(())
}

fn has_new_snapshots(latest_snapshot: i64, last_checkpoint: i64) -> bool {
    latest_snapshot > last_checkpoint
}

fn format_change_row(column_names: &[String], values: &[String]) -> String {
    column_names
        .iter()
        .zip(values.iter())
        .map(|(col, val)| format!("{col}={val}"))
        .collect::<Vec<_>>()
        .join(" | ")
}

async fn lambda_handler(
    _event: LambdaEvent<serde_json::Value>,
) -> Result<serde_json::Value, lambda_runtime::Error> {
    process_cdc().await?;
    Ok(serde_json::json!({ "status": "ok" }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::var("AWS_LAMBDA_RUNTIME_API").is_ok() {
        lambda_runtime::run(service_fn(lambda_handler)).await?;
    } else {
        dotenvy::dotenv().ok();
        process_cdc().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_new_snapshots_when_latest_is_greater() {
        assert!(has_new_snapshots(5, 3));
    }

    #[test]
    fn test_has_new_snapshots_when_equal() {
        assert!(!has_new_snapshots(3, 3));
    }

    #[test]
    fn test_has_new_snapshots_when_latest_is_less() {
        assert!(!has_new_snapshots(2, 5));
    }

    #[test]
    fn test_format_change_row_single_column() {
        let cols = vec!["id".to_string()];
        let vals = vec!["1".to_string()];
        assert_eq!(format_change_row(&cols, &vals), "id=1");
    }

    #[test]
    fn test_format_change_row_multiple_columns() {
        let cols = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let vals = vec!["1".to_string(), "alice".to_string(), "30".to_string()];
        assert_eq!(format_change_row(&cols, &vals), "id=1 | name=alice | age=30");
    }

    #[test]
    fn test_format_change_row_with_null() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let vals = vec!["1".to_string(), "NULL".to_string()];
        assert_eq!(format_change_row(&cols, &vals), "id=1 | name=NULL");
    }

    #[test]
    fn test_format_change_row_empty() {
        let cols: Vec<String> = vec![];
        let vals: Vec<String> = vec![];
        assert_eq!(format_change_row(&cols, &vals), "");
    }

    #[test]
    fn test_config_from_iter() {
        let vars = vec![
            ("POSTGRES_HOST".to_string(), "localhost".to_string()),
            ("POSTGRES_PORT".to_string(), "5432".to_string()),
            ("POSTGRES_USER".to_string(), "duck".to_string()),
            ("POSTGRES_PASSWORD".to_string(), "pass".to_string()),
            ("POSTGRES_DB".to_string(), "testdb".to_string()),
            ("MINIO_ENDPOINT".to_string(), "localhost:9000".to_string()),
            ("MINIO_ACCESS_KEY".to_string(), "key".to_string()),
            ("MINIO_SECRET_KEY".to_string(), "secret".to_string()),
            ("MINIO_BUCKET".to_string(), "bucket".to_string()),
        ];

        let config: Config = envy::from_iter(vars).unwrap();
        assert_eq!(config.postgres_host, "localhost");
        assert_eq!(config.postgres_port, 5432);
        assert_eq!(config.postgres_user, "duck");
        assert_eq!(config.postgres_password, "pass");
        assert_eq!(config.postgres_db, "testdb");
        assert_eq!(config.minio_endpoint, "localhost:9000");
        assert_eq!(config.minio_access_key, "key");
        assert_eq!(config.minio_secret_key, "secret");
        assert_eq!(config.minio_bucket, "bucket");
    }

    #[test]
    fn test_config_invalid_port() {
        let vars = vec![
            ("POSTGRES_HOST".to_string(), "localhost".to_string()),
            ("POSTGRES_PORT".to_string(), "not_a_number".to_string()),
            ("POSTGRES_USER".to_string(), "duck".to_string()),
            ("POSTGRES_PASSWORD".to_string(), "pass".to_string()),
            ("POSTGRES_DB".to_string(), "testdb".to_string()),
            ("MINIO_ENDPOINT".to_string(), "localhost:9000".to_string()),
            ("MINIO_ACCESS_KEY".to_string(), "key".to_string()),
            ("MINIO_SECRET_KEY".to_string(), "secret".to_string()),
            ("MINIO_BUCKET".to_string(), "bucket".to_string()),
        ];

        let result = envy::from_iter::<_, Config>(vars);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_missing_field() {
        let vars = vec![
            ("POSTGRES_HOST".to_string(), "localhost".to_string()),
            ("POSTGRES_PORT".to_string(), "5432".to_string()),
        ];

        let result = envy::from_iter::<_, Config>(vars);
        assert!(result.is_err());
    }
}
