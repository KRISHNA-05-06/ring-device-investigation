-- Read-only operational triage. This flags candidates, not proven crashed runs.
-- A long-running row requires logs and source evidence before intervention.
SELECT run_id, household_id, source_bucket, source_key, started_at,
       clock_timestamp() - started_at AS observed_age, status
FROM public.ingestion_runs
WHERE household_id = 'household-001'
  AND status = 'running'
  AND started_at < clock_timestamp() - INTERVAL '15 minutes'
ORDER BY started_at;

SELECT run_id, started_at, completed_at, error_message, source_key
FROM public.ingestion_runs
WHERE household_id = 'household-001'
  AND status = 'failed'
ORDER BY started_at DESC
LIMIT 20;
