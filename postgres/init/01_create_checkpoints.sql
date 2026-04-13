CREATE TABLE IF NOT EXISTS checkpoints (
    table_name TEXT PRIMARY KEY,
    snapshot_id BIGINT NOT NULL
);
