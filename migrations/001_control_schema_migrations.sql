CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.schema_migrations (
    version TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
