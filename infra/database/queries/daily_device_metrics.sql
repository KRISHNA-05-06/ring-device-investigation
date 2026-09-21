-- Run with psql while connected as ring_reader to the ring database.
-- Change this date to intentionally examine a different UTC day.
\set report_day '2026-09-18'

WITH bounds AS (
    SELECT
        :'report_day'::date AS report_day,
        :'report_day'::date::timestamp AT TIME ZONE 'UTC' AS day_start,
        (:'report_day'::date + 1)::timestamp AT TIME ZONE 'UTC' AS day_end
), selected_devices AS (
    SELECT d.household_id, d.device_id, d.device_type, d.registered_at
    FROM public.devices AS d
    CROSS JOIN bounds AS b
    WHERE d.household_id = 'household-001'
      AND d.registered_at < b.day_end
), day_events AS (
    SELECT e.*
    FROM public.heartbeat_events AS e
    JOIN selected_devices AS d
      ON d.household_id = e.household_id
     AND d.device_id = e.device_id
    CROSS JOIN bounds AS b
    WHERE e.event_time >= b.day_start
      AND e.event_time < b.day_end
), daily_totals AS (
    SELECT
        household_id,
        device_id,
        COUNT(*) AS accepted_heartbeat_count,
        MIN(event_time) AS first_event_time,
        MAX(event_time) AS last_event_time,
        MIN(battery_percent) AS min_battery_percent,
        MAX(battery_percent) AS max_battery_percent,
        ROUND(AVG(battery_percent), 2) AS average_battery_percent
    FROM day_events
    GROUP BY household_id, device_id
), latest_in_day AS (
    SELECT DISTINCT ON (household_id, device_id)
        household_id, device_id, event_id, battery_percent, received_at, run_id
    FROM day_events
    ORDER BY household_id, device_id, event_time DESC, event_id DESC
)
SELECT
    b.report_day,
    'UTC' AS report_timezone,
    d.household_id,
    d.device_id,
    d.device_type,
    d.registered_at,
    COALESCE(t.accepted_heartbeat_count, 0) AS accepted_heartbeat_count,
    t.first_event_time,
    t.last_event_time,
    t.min_battery_percent,
    t.max_battery_percent,
    t.average_battery_percent,
    latest.event_id AS latest_event_id,
    latest.battery_percent AS latest_battery_percent,
    latest.received_at AS latest_event_received_at,
    latest.run_id AS latest_event_run_id,
    CURRENT_TIMESTAMP AS queried_at
FROM selected_devices AS d
CROSS JOIN bounds AS b
LEFT JOIN daily_totals AS t
  ON t.household_id = d.household_id
 AND t.device_id = d.device_id
LEFT JOIN latest_in_day AS latest
  ON latest.household_id = d.household_id
 AND latest.device_id = d.device_id
ORDER BY d.device_id;
