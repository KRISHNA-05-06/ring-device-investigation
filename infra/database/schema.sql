-- PostgreSQL 17: current four-table AWS schema.
-- Reference DDL for a new empty schema, not an in-place migration.
BEGIN;
CREATE TABLE public.devices (
    household_id TEXT NOT NULL CHECK (btrim(household_id) <> ''),
    device_id TEXT NOT NULL CHECK (btrim(device_id) <> ''),
    device_type TEXT NOT NULL CHECK (device_type IN ('doorbell', 'camera')),
    registered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (household_id, device_id)
);
CREATE TABLE public.ingestion_runs (
    run_id TEXT PRIMARY KEY CHECK (btrim(run_id) <> ''),
    household_id TEXT NOT NULL CHECK (btrim(household_id) <> ''),
    source_bucket TEXT NOT NULL,
    source_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    input_count INTEGER NOT NULL DEFAULT 0 CHECK (input_count >= 0),
    accepted_count INTEGER NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    rejected_count INTEGER NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    error_message TEXT,
    UNIQUE (run_id, household_id),
    CHECK (input_count = accepted_count + duplicate_count + rejected_count),
    CHECK ((status = 'running' AND completed_at IS NULL)
        OR (status IN ('completed', 'failed') AND completed_at IS NOT NULL)),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);
CREATE TABLE public.heartbeat_events (
    household_id TEXT NOT NULL,
    event_id TEXT NOT NULL CHECK (btrim(event_id) <> ''),
    device_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    battery_percent INTEGER NOT NULL CHECK (battery_percent BETWEEN 0 AND 100),
    digest TEXT NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    run_id TEXT NOT NULL,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (household_id, event_id),
    FOREIGN KEY (household_id, device_id) REFERENCES public.devices (household_id, device_id),
    FOREIGN KEY (run_id, household_id) REFERENCES public.ingestion_runs (run_id, household_id)
);
CREATE TABLE public.rejected_records (
    rejection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES public.ingestion_runs (run_id),
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    error_code TEXT NOT NULL CHECK (btrim(error_code) <> ''),
    error_message TEXT NOT NULL,
    raw_record TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, line_number)
);
CREATE INDEX heartbeat_device_time_idx ON public.heartbeat_events
    (household_id, device_id, event_time DESC, event_id DESC);
CREATE INDEX heartbeat_run_idx ON public.heartbeat_events (run_id);
CREATE INDEX ingestion_household_time_idx ON public.ingestion_runs (household_id, started_at DESC);
COMMIT;
