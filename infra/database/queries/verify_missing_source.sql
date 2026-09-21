-- Read-only: isolates attempts for this deliberately absent input.
SELECT
    r.run_id,
    r.status,
    r.started_at,
    r.completed_at,
    r.error_message,
    r.input_count,
    r.accepted_count,
    r.duplicate_count,
    r.rejected_count,
    (SELECT COUNT(*) FROM public.heartbeat_events e
     WHERE e.run_id = r.run_id AND e.household_id = r.household_id)
        AS stored_accepted_rows,
    (SELECT COUNT(*) FROM public.rejected_records q
     WHERE q.run_id = r.run_id) AS stored_rejection_rows
FROM public.ingestion_runs r
WHERE r.household_id = 'household-001'
  AND r.source_bucket = 'ring-device-investigation-455646290306-us-east-1'
  AND r.source_key = 'raw/reliability/missing-source/expected-absent-6508c2f6-536a-43aa-a7f8-5c358b2ec92e.jsonl'
ORDER BY r.started_at DESC, r.run_id DESC
LIMIT 10;
