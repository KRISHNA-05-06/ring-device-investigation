-- Replace with the SEND session_id for the one-batch invalid-battery scenario.
\set session_id 'PASTE_SEND_SESSION_ID'

SELECT device_id, COUNT(*) AS accepted_events,
       MIN(battery_percent) AS min_battery,
       MAX(battery_percent) AS max_battery
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND starts_with(event_id, 'sim-' || :'session_id' || '-')
GROUP BY device_id
ORDER BY device_id;

-- Search raw text without parsing every rejection as JSON: other inputs may
-- legitimately have malformed JSON. The session ID is unique to this run.
SELECT q.rejection_id, q.error_code, q.error_message, q.raw_record,
       r.status AS run_status, r.input_count, r.accepted_count,
       r.duplicate_count, r.rejected_count, r.source_key
FROM public.rejected_records q
JOIN public.ingestion_runs r ON r.run_id = q.run_id
WHERE r.household_id = 'household-001'
  AND strpos(q.raw_record, 'sim-' || :'session_id' || '-') > 0
ORDER BY q.created_at, q.rejection_id;

SELECT COUNT(*) AS invalid_camera_events_stored
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND device_id = 'camera-002'
  AND starts_with(event_id, 'sim-' || :'session_id' || '-');
