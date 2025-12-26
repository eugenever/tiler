-- name: create-table-datasource
CREATE TABLE IF NOT EXISTS datasource (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR NOT NULL UNIQUE,
    data_type VARCHAR,
    host VARCHAR,
    port INTEGER,
    store_type VARCHAR,
    mbtiles BOOLEAN,
    name VARCHAR,
    description TEXT,
    attribution TEXT,
    minzoom SMALLINT,
    maxzoom SMALLINT,
    bounds JSONB,
    center JSONB,
    data JSONB NOT NULL
);

-- name: create-table-queue
CREATE TABLE IF NOT EXISTS queue (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL,
    failed_attempts INT NOT NULL,
    status INT NOT NULL,
    job_detail JSONB NOT NULL
);
-- name: create-index_queue_on_scheduled_for
CREATE INDEX IF NOT EXISTS index_queue_on_scheduled_for ON queue (scheduled_for);
-- name: create-index_queue_on_status
CREATE INDEX IF NOT EXISTS index_queue_on_status ON queue (status);
