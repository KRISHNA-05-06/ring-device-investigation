# Ring four-table PostgreSQL reference

Reference version: 1. Source: project aws/schema.sql (PostgreSQL 17).
This is maintained reference DDL for a new empty schema, not a live introspection result or an in-place migration. Reading this resource executes no SQL. Do not run the DDL against the populated project database.

## Tables and meaning

| Table | Row meaning | Identity |
| --- | --- | --- |
| devices | One registered device in a household | household_id + device_id |
| ingestion_runs | One processing attempt for a source object | run_id |
| heartbeat_events | One accepted event; includes ingestion lineage | household_id + event_id |
| rejected_records | One rejected line in a processing attempt, preserving raw input | rejection_id; run_id + line_number unique |

devices connects to heartbeat_events through the composite household/device foreign key. heartbeat_events connects to ingestion_runs through the composite run/household foreign key. rejected_records connects to ingestion_runs through run_id. Rejections have no household_id column: scope rejection queries through the associated ingestion run's household.

ingestion_runs is an attempt summary, not a queue that routes rows. The validator classifies input and writes attempt counts, accepted events, and rejection evidence. Reprocessing can produce additional runs and rejection occurrences. A duplicate is counted without inserting another accepted event.

## Constraints versus application rules

The database enforces nonnegative counts, input = accepted + duplicate + rejected, battery range 0..100, identity uniqueness, and the foreign keys shown below. The validator additionally checks input types, household scope, registration timing, and event-content conflicts. The same household/event ID with identical normalized content is a duplicate; different content is a conflict. A digest is a content fingerprint, not an approval signature.

event_time is occurrence time. received_at is accepted-row ingestion time. started_at is attempt start time. These serve different queries; historical event cutoffs can include late arrivals and replays stored later.

No extra operations, approvals, audit, or health-snapshot table exists. Their saved artifacts are in S3. This resource includes no passwords or raw customer records. SQL household filters are application scoping, not database row-level security.

## Reference DDL


```sql
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

```
