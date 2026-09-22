# AWS database schema

The PostgreSQL schema contains four tables. See [schema.sql](../../infra/database/schema.sql) for constraints and indexes, or [schema.dbml](../../infra/database/schema.dbml) for a diagram definition. Confirm deployed definitions with `\d+ public.table_name` when checking for schema drift.

## Relationships

- `devices (household_id, device_id)` -> many `heartbeat_events (household_id, device_id)`.
- `ingestion_runs (run_id, household_id)` -> many `heartbeat_events (run_id, household_id)`.
- `ingestion_runs (run_id)` -> many `rejected_records (run_id)`.

The last relationship does not include device_id: rejected input may have an unknown device or be malformed. Household scope for a rejected record comes from its ingestion run. There is no separate household, subscription, report, or audit table.

## devices

One row per registered device within a household. Provides the reference list for validation.

| Column | Type | Rules | Meaning |
| --- | --- | --- | --- |
| `household_id` | TEXT | PK part; required | Household scope of the registered device. There is no separate households table. |
| `device_id` | TEXT | PK part; required | Device identifier, unique within one household. |
| `device_type` | TEXT | Required | Allowed values: doorbell or camera. |
| `registered_at` | TIMESTAMPTZ | Required | Registration time. The validator rejects an event earlier than this time. |

## ingestion_runs

One row per processing attempt for one input object. Tracks source evidence, state, counts, and failures.

| Column | Type | Rules | Meaning |
| --- | --- | --- | --- |
| `run_id` | TEXT | PK; required | Identifier for one processing attempt. Current format: Lambda request ID plus object index. |
| `household_id` | TEXT | Required; composite UQ | Household configured for this ingestion attempt. Paired with run_id for the heartbeat foreign key. |
| `source_bucket` | TEXT | Required | S3 bucket containing the original input object. |
| `source_key` | TEXT | Required | Full S3 object key, such as raw/mixed-events.json. A key alone does not preserve an overwritten object. |
| `status` | TEXT | Required; default 'running' | running, completed, or failed. Completed may include rejected records; it means processing finished. |
| `started_at` | TIMESTAMPTZ | Required; default now | Time the attempt started. Used to order recent runs and investigate stale runs. |
| `completed_at` | TIMESTAMPTZ | Nullable | Completion or failure time. Must be absent while running and present after completion or failure. |
| `input_count` | INTEGER | Required; default 0 | Number of nonblank input records accounted for by a completed run. |
| `accepted_count` | INTEGER | Required; default 0 | New valid heartbeat rows inserted by this run. |
| `duplicate_count` | INTEGER | Required; default 0 | Repeated events with the same household, event ID, and normalized content. No new heartbeat row is added. |
| `rejected_count` | INTEGER | Required; default 0 | Records rejected by validation or conflicting event content. |
| `error_message` | TEXT | Nullable | Run-level failure information. The current validator stores the exception class, not a full secret-bearing traceback. |

## heartbeat_events

One row per accepted event ID within a household. Preserves event history for health investigations.

| Column | Type | Rules | Meaning |
| --- | --- | --- | --- |
| `household_id` | TEXT | PK part; FK part; required | Scope for the event. Both the registered device and ingestion run must match this household. |
| `event_id` | TEXT | PK part; required | Event identifier within a household. Reuse with different content is a conflict. |
| `device_id` | TEXT | FK part; required | Registered device that sent this heartbeat. |
| `event_time` | TIMESTAMPTZ | Required | Time the event happened, not the time it arrived. Used to find the latest heartbeat at a chosen cutoff. |
| `battery_percent` | INTEGER | Required; 0 through 100 | Reported battery level. Boolean and non-integer inputs are rejected by the validator. |
| `digest` | TEXT | Required; 64 lowercase hex | SHA-256 fingerprint of normalized event content. Distinguishes an identical repeat from changed content; it is not encryption. |
| `run_id` | TEXT | FK part; required | Run that first inserted this event. Duplicate deliveries do not replace this reference. |
| `line_number` | INTEGER | Required; greater than 0 | Original one-based line number in the source object. |
| `received_at` | TIMESTAMPTZ | Required; default now | Time the database recorded the accepted event. PostgreSQL CURRENT_TIMESTAMP uses the transaction start time. |

## rejected_records

One row per rejected line in a processing attempt. Preserves bad input and the reason for rejection.

| Column | Type | Rules | Meaning |
| --- | --- | --- | --- |
| `rejection_id` | BIGINT | PK; generated identity | Database-generated identifier for one rejected record. |
| `run_id` | TEXT | FK; required; UQ part | Processing attempt that rejected this record. Obtain household scope by joining this run. |
| `line_number` | INTEGER | Required; UQ part; > 0 | Original one-based source line. A run can store one rejection per line. |
| `error_code` | TEXT | Required; nonblank | Machine-readable reason, such as UNKNOWN_DEVICE or EVENT_ID_CONFLICT. |
| `error_message` | TEXT | Required | Readable explanation of the rejection. |
| `raw_record` | TEXT | Required | Original line text, including malformed JSON. TEXT permits evidence that JSON storage would reject. |
| `created_at` | TIMESTAMPTZ | Required; default now | Time the rejection row was recorded. Uses PostgreSQL transaction time by default. |

## Constraints and interpretation

- All run counts are nonnegative. input_count = accepted_count + duplicate_count + rejected_count.
- Completed means the file was processed; it does not mean every record was accepted.
- Failed-run counts currently describe committed outcomes, normally zero after rollback. They are not a reliable count of the source file size.
- Devices have a composite primary key. Heartbeats have a composite primary key. A device or event ID is not globally unique across households.
- The run-household unique pair supports a foreign key that prevents linking a heartbeat to another household's run.
- There are no database row-level security policies yet. Roles restrict operations; the application enforces household scope.
- A repeated rejected record may create a new rejection in a later run. UNIQUE(run_id, line_number) prevents duplication only within the same run.
- Valid late events remain in history. Health queries select by event_time, with event_id as the deterministic equal-time tie breaker.
- Current indexes support device-time lookup, heartbeat-to-run lookup, and recent runs per household.
- Proposals, approvals, correction artifacts, and saved reports are stored in S3. They do not add tables to this schema.

## Verified examples

| Input | Accepted | Duplicate | Rejected |
| --- | ---: | ---: | ---: |
| raw/mixed-events.json | 2 | 0 | 2 |
| raw/edge-cases.json | 0 | 1 | 3 |

Edge-case rejection reasons: EVENT_ID_CONFLICT, UNKNOWN_DEVICE, HOUSEHOLD_MISMATCH. The existing evt-003 heartbeat retained battery_percent = 80.
