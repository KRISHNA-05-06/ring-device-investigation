PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS devices (
    household_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    PRIMARY KEY (household_id, device_id)
);
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    household_id TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    input_count INTEGER NOT NULL,
    accepted_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    CHECK (input_count = accepted_count + duplicate_count + rejected_count)
);
CREATE TABLE IF NOT EXISTS heartbeat_events (
    household_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    battery_percent INTEGER NOT NULL CHECK (battery_percent BETWEEN 0 AND 100),
    digest TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES ingestion_runs (run_id),
    PRIMARY KEY (household_id, event_id),
    FOREIGN KEY (household_id, device_id) REFERENCES devices (household_id, device_id)
);
CREATE TABLE IF NOT EXISTS rejected_records (
    rejection_id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES ingestion_runs (run_id),
    line_number INTEGER NOT NULL,
    error_code TEXT NOT NULL,
    raw_record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS heartbeat_lookup
ON heartbeat_events (household_id, device_id, event_time DESC);

INSERT OR IGNORE INTO devices VALUES
('household-001', 'doorbell-001', 'doorbell', '2026-09-01T09:00:00.000000Z'),
('household-001', 'camera-001', 'camera', '2026-09-01T09:00:00.000000Z'),
('household-001', 'camera-002', 'camera', '2026-09-01T09:00:00.000000Z'),
('household-002', 'private-camera', 'camera', '2026-09-01T09:00:00.000000Z');
