-- The batch ID must match the producer command. These queries only read data.
\set batch_id 'duplicate-demo-v1'

SELECT event_id, device_id, event_time, battery_percent, run_id
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND event_id = 'reliability-duplicate-' || :'batch_id';

SELECT COUNT(*) AS stored_event_count
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND event_id = 'reliability-duplicate-' || :'batch_id';

SELECT run_id, source_key, status, started_at, completed_at,
       input_count, accepted_count, duplicate_count, rejected_count, error_message
FROM public.ingestion_runs
WHERE household_id = 'household-001'
  AND source_bucket = 'ring-device-investigation-455646290306-us-east-1'
  AND source_key IN (
      'raw/reliability/duplicate-pair/' || :'batch_id' || '/a.jsonl',
      'raw/reliability/duplicate-pair/' || :'batch_id' || '/b.jsonl'
  )
ORDER BY started_at, run_id;

WITH selected AS (
    SELECT * FROM public.ingestion_runs
    WHERE household_id = 'household-001'
      AND source_bucket = 'ring-device-investigation-455646290306-us-east-1'
      AND source_key IN (
          'raw/reliability/duplicate-pair/' || :'batch_id' || '/a.jsonl',
          'raw/reliability/duplicate-pair/' || :'batch_id' || '/b.jsonl'
      )
)
SELECT COUNT(*) AS observed_attempts,
       COUNT(DISTINCT source_key) AS observed_source_objects,
       COUNT(*) FILTER (WHERE status = 'running') AS running_attempts,
       COUNT(*) FILTER (WHERE status = 'failed') AS failed_attempts,
       COALESCE(SUM(accepted_count) FILTER (WHERE status = 'completed'), 0) AS completed_accepted,
       COALESCE(SUM(duplicate_count) FILTER (WHERE status = 'completed'), 0) AS completed_duplicates,
       COALESCE(SUM(rejected_count) FILTER (WHERE status = 'completed'), 0) AS completed_rejections
FROM selected;

-- Interval overlap is supporting evidence, not proof of simultaneous INSERTs.
WITH selected AS (
    SELECT run_id, source_key, started_at, completed_at
    FROM public.ingestion_runs
    WHERE household_id = 'household-001' AND status = 'completed'
      AND source_bucket = 'ring-device-investigation-455646290306-us-east-1'
      AND source_key IN (
          'raw/reliability/duplicate-pair/' || :'batch_id' || '/a.jsonl',
          'raw/reliability/duplicate-pair/' || :'batch_id' || '/b.jsonl'
      )
)
SELECT COUNT(*) AS overlapping_completed_pairs
FROM selected a
JOIN selected b ON a.run_id < b.run_id AND a.source_key <> b.source_key
              AND a.started_at < b.completed_at AND b.started_at < a.completed_at;
