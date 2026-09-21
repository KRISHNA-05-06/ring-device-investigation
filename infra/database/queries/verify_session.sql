-- Paste the session_id from the SEND run, not from preview.
\set session_id 'PASTE_SEND_SESSION_ID'

SELECT device_id, COUNT(*) AS accepted_events,
       MIN(event_time) AS first_event_time,
       MAX(event_time) AS last_event_time,
       MIN(battery_percent) AS min_battery,
       MAX(battery_percent) AS max_battery
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND starts_with(event_id, 'sim-' || :'session_id' || '-')
GROUP BY device_id
ORDER BY device_id;

SELECT COUNT(*) AS stored_event_count
FROM public.heartbeat_events
WHERE household_id = 'household-001'
  AND starts_with(event_id, 'sim-' || :'session_id' || '-');
