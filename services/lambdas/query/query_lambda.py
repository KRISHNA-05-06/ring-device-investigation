import hashlib
import json
import logging
import os
import re
import ssl
from datetime import date, datetime, timedelta, timezone

import pg8000.dbapi


logger = logging.getLogger()
logger.setLevel(logging.INFO)

OPERATIONS = {
    "get_daily_device_metrics",
    "get_device_health",
    "get_device_details",
    "get_event_history",
    "get_quality_results",
    "get_pipeline_status",
    "get_rejection_summary",
    "list_rejected_records",
    "inspect_rejected_record",
    "preview_record_correction",
    "preview_replay",
    "get_replay_ingestion_status",
}


REPLAY_RULES_SHA256 = "5963163f3630f470382e2d82e4edcea44e8c2c566bd2424403dcd3f77889e28f"
MAX_REPLAY_CANDIDATE_BYTES = 16000
REPLAY_BUCKET = "ring-device-investigation-455646290306-us-east-1"
REPLAY_RUN_LIMIT = 50


def parse_request(event):
    if not isinstance(event, dict):
        raise ValueError("Request must be a JSON object")

    operation = event.get("operation")

    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise ValueError("Unsupported operation")

    allowed_fields = {"operation"}

    if operation == "get_daily_device_metrics":
        allowed_fields.update({"report_day", "device_limit"})
    elif operation in {"get_device_health", "get_device_details"}:
        allowed_fields.update({"as_of", "silence_minutes"})
        if operation == "get_device_details":
            allowed_fields.add("device_id")
    elif operation == "get_event_history":
        allowed_fields.update({"device_id", "start_time", "end_time", "limit"})
    elif operation == "get_quality_results":
        allowed_fields.update({"start_time", "end_time", "run_limit"})
    elif operation == "list_rejected_records":
        allowed_fields.update({"limit", "error_code"})
    elif operation == "inspect_rejected_record":
        allowed_fields.add("rejection_id")
    elif operation == "preview_record_correction":
        allowed_fields.update({"rejection_id", "battery_percent", "reason"})
    elif operation == "preview_replay":
        allowed_fields.add("candidate")
    elif operation == "get_replay_ingestion_status":
        allowed_fields.add("replay_id")

    if set(event) - allowed_fields:
        raise ValueError("Request contains unsupported fields")

    if operation == "get_daily_device_metrics":
        daily_window(event)
        if type(event.get("device_limit", 100)) is not int or not 1 <= event.get("device_limit", 100) <= 100:
            raise ValueError("device_limit must be an integer from 1 to 100")

    if operation in {"get_device_details", "get_event_history"}:
        device_id = event.get("device_id")
        if (not isinstance(device_id, str) or not device_id.strip() or device_id != device_id.strip()
            or len(device_id) > 128 or "\x00" in device_id):
            raise ValueError("device_id must be a nonblank ID of at most 128 characters without surrounding whitespace or NUL")
        try:
            device_id.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("device_id contains invalid Unicode") from None

    if operation == "get_event_history":
        history_window(event)
        limit = event.get("limit", 20)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 to 100")

    if operation == "get_quality_results":
        history_window(event)
        if type(event.get("run_limit", 100)) is not int or not 1 <= event.get("run_limit", 100) <= 200:
            raise ValueError("run_limit must be an integer from 1 to 200")

    if operation == "get_replay_ingestion_status":
        replay_id = event.get("replay_id")
        if not isinstance(replay_id, str) or re.fullmatch(r"[0-9a-f]{64}", replay_id) is None:
            raise ValueError("replay_id must be 64 lowercase hexadecimal characters")

    as_of = datetime.now(timezone.utc)
    silence_minutes = 10

    if operation in {"get_device_health", "get_device_details"}:
        if "as_of" in event:
            value = event["as_of"]

            if not isinstance(value, str) or len(value) > 64:
                raise ValueError("as_of must be an ISO timestamp")

            try:
                as_of = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError("as_of must be an ISO timestamp") from None

            if as_of.tzinfo is None or as_of.utcoffset() is None:
                raise ValueError("as_of must include a timezone")

            as_of = as_of.astimezone(timezone.utc)

        silence_minutes = event.get("silence_minutes", 10)

        if type(silence_minutes) is not int or not 1 <= silence_minutes <= 1440:
            raise ValueError("silence_minutes must be an integer from 1 to 1440")

    if operation == "list_rejected_records":
        limit = event.get("limit", 20)

        if type(limit) is not int or not 1 <= limit <= 50:
            raise ValueError("limit must be an integer from 1 to 50")

        if "error_code" in event:
            error_code = event["error_code"]

            if (
                not isinstance(error_code, str)
                or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", error_code) is None
            ):
                raise ValueError(
                    "error_code must be 1 to 64 uppercase letters, digits, "
                    "or underscores, starting with a letter"
                )

    if operation in {"inspect_rejected_record", "preview_record_correction"}:
        rejection_id = event.get("rejection_id")
        if type(rejection_id) is not int or not 1 <= rejection_id <= 9223372036854775807:
            raise ValueError("rejection_id must be a positive 64-bit integer")

    if operation == "preview_record_correction":
        battery_percent = event.get("battery_percent")
        if type(battery_percent) is not int or not 0 <= battery_percent <= 100:
            raise ValueError("battery_percent must be an integer from 0 to 100")
        reason = event.get("reason")
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise ValueError("reason must contain 1 to 500 nonblank characters")

    if operation == "preview_replay":
        candidate = event.get("candidate")
        if not isinstance(candidate, dict):
            raise ValueError("candidate must be a JSON object")
        try:
            encoded = json.dumps(candidate, ensure_ascii=True, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, RecursionError):
            raise ValueError("candidate must contain valid JSON values") from None
        if len(encoded) > MAX_REPLAY_CANDIDATE_BYTES:
            raise ValueError("candidate exceeds the 16000-byte preview limit")

    return operation, as_of, silence_minutes


def daily_window(event):
    value = event.get("report_day")
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        raise ValueError("report_day must be a UTC calendar date in YYYY-MM-DD format")
    try:
        day = date.fromisoformat(value)
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
    except (ValueError, OverflowError):
        raise ValueError("report_day must be a valid date with a representable following day") from None
    return start, end


def history_window(event):
    """Require an explicit UTC-normalized window, start inclusive/end exclusive."""
    times = []
    for name in ("start_time", "end_time"):
        value = event.get(name)
        try:
            if not isinstance(value, str) or len(value) > 64:
                raise ValueError()
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.utcoffset() is None:
                raise ValueError()
            times.append(parsed.astimezone(timezone.utc))
        except (ValueError, OverflowError):
            raise ValueError(f"{name} must be an ISO timestamp with a timezone") from None
    start, end = times
    if not 0 < (end - start).total_seconds() <= 31 * 86400:
        raise ValueError("History window must be greater than zero and at most 31 days")
    return start, end


def connect_database():
    if os.environ["DB_USER"] != "ring_reader":
        raise RuntimeError("Query service must use ring_reader")

    tls = ssl.create_default_context(cafile=os.environ["DB_SSLROOTCERT"])

    return pg8000.dbapi.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        ssl_context=tls,
        timeout=5,
    )


def fetch_rows(cursor, sql, parameters):
    cursor.execute(sql, parameters)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_device_health(cursor, household_id, as_of, silence_minutes):
    rows = fetch_rows(
        cursor,
        """
        SELECT
            d.device_id,
            d.device_type,
            d.registered_at,
            h.event_id,
            h.event_time AS last_heartbeat,
            h.battery_percent,
            h.received_at,
            h.run_id
        FROM public.devices AS d
        LEFT JOIN LATERAL (
            SELECT event_id, event_time, battery_percent, received_at, run_id
            FROM public.heartbeat_events
            WHERE household_id = d.household_id
              AND device_id = d.device_id
              AND event_time <= %s
            ORDER BY event_time DESC, event_id DESC
            LIMIT 1
        ) AS h ON TRUE
        WHERE d.household_id = %s
          AND d.registered_at <= %s
        ORDER BY d.device_id
        LIMIT 101
        """,
        (as_of, household_id, as_of),
    )

    truncated = len(rows) > 100
    devices = rows[:100]

    for device in devices:
        last_heartbeat = device["last_heartbeat"]

        if last_heartbeat is None:
            device["health_status"] = "unknown"
            device["minutes_since_heartbeat"] = None
        else:
            age_seconds = (as_of - last_heartbeat).total_seconds()
            device["minutes_since_heartbeat"] = round(age_seconds / 60, 2)
            device["health_status"] = (
                "suspected_offline"
                if age_seconds > silence_minutes * 60
                else "reporting"
            )

    return {
        "as_of": as_of,
        "silence_minutes": silence_minutes,
        "devices": devices,
        "returned_devices": len(devices),
        "truncated": truncated,
    }


def get_device_details(cursor, household_id, device_id, as_of, silence_minutes):
    rows = fetch_rows(cursor, """
        SELECT d.device_id, d.device_type, d.registered_at,
               h.event_id, h.event_time, h.battery_percent, h.received_at,
               h.run_id, h.line_number, h.digest,
               r.source_bucket, r.source_key, r.status AS run_status
        FROM public.devices AS d
        LEFT JOIN LATERAL (
            SELECT event_id, event_time, battery_percent, received_at,
                   run_id, line_number, digest
            FROM public.heartbeat_events
            WHERE household_id = d.household_id AND device_id = d.device_id
              AND event_time <= %s
            ORDER BY event_time DESC, event_id DESC
            LIMIT 1
        ) AS h ON TRUE
        LEFT JOIN public.ingestion_runs AS r
          ON r.run_id = h.run_id AND r.household_id = d.household_id
        WHERE d.household_id = %s AND d.device_id = %s AND d.registered_at <= %s
        LIMIT 1
        """, (as_of, household_id, device_id, as_of))
    result = {
        "scope": "registered_device_at_event_time_cutoff", "device_id": device_id,
        "as_of": as_of, "silence_minutes": silence_minutes,
        "found": bool(rows), "device": None, "health": None, "latest_heartbeat": None,
        "data_basis": "currently_stored_accepted_events_including_late_arrivals_and_replays",
    }
    if not rows:
        result["message"] = "No device registration was found in the configured household at this cutoff."
        return result
    row = rows[0]
    result["device"] = {name: row[name] for name in ("device_id", "device_type", "registered_at")}
    result["health"] = {"status": "unknown", "minutes_since_heartbeat": None}
    if row["event_id"] is not None:
        age_seconds = (as_of - row["event_time"]).total_seconds()
        result["health"] = {
            "status": "suspected_offline" if age_seconds > silence_minutes * 60 else "reporting",
            "minutes_since_heartbeat": round(age_seconds / 60, 2),
        }
        result["latest_heartbeat"] = {name: row[name] for name in (
            "event_id", "event_time", "battery_percent", "received_at", "run_id",
            "line_number", "digest", "source_bucket", "source_key", "run_status")}
    return result


def get_event_history(cursor, household_id, device_id, start_time, end_time, limit):
    """Read accepted event history; rejected records remain a separate investigation."""
    devices = fetch_rows(cursor, """
        SELECT device_id, device_type, registered_at
        FROM public.devices
        WHERE household_id = %s AND device_id = %s
        LIMIT 1
        """, (household_id, device_id))
    result = {
        "scope": "accepted_device_events_in_time_window",
        "device_id": device_id, "device_found": bool(devices),
        "device": devices[0] if devices else None,
        "start_time": start_time, "end_time": end_time,
        "window_bounds": "start_inclusive_end_exclusive",
        "order": "event_time_desc_event_id_desc",
        "records": [], "returned_records": 0, "limit": limit, "truncated": False,
        "data_basis": "currently_stored_accepted_events_including_late_arrivals_and_replays",
    }
    if not devices:
        return result
    rows = fetch_rows(cursor, """
        SELECT e.event_id, e.event_time, e.battery_percent, e.received_at,
               e.run_id, e.line_number, e.digest,
               r.source_bucket, r.source_key, r.status AS run_status
        FROM public.heartbeat_events AS e
        JOIN public.ingestion_runs AS r
          ON r.run_id = e.run_id AND r.household_id = e.household_id
        WHERE e.household_id = %s AND e.device_id = %s
          AND e.event_time >= %s AND e.event_time < %s
        ORDER BY e.event_time DESC, e.event_id DESC
        LIMIT %s
        """, (household_id, device_id, start_time, end_time, limit + 1))
    result.update(records=rows[:limit], returned_records=min(len(rows), limit), truncated=len(rows) > limit)
    return result


def get_quality_results(cursor, household_id, start_time, end_time, run_limit):
    rows = fetch_rows(cursor, """
        WITH selected AS (
            SELECT run_id, household_id, status, started_at, source_key,
                   input_count, accepted_count, duplicate_count, rejected_count
            FROM public.ingestion_runs
            WHERE household_id = %s AND started_at >= %s AND started_at < %s
            ORDER BY started_at DESC, run_id DESC
            LIMIT %s
        )
        SELECT r.*,
            (SELECT COUNT(*) FROM public.heartbeat_events e
             WHERE e.run_id = r.run_id AND e.household_id = r.household_id) AS stored_accepted_count,
            (SELECT COUNT(*) FROM public.rejected_records q
             WHERE q.run_id = r.run_id) AS stored_rejected_count,
            (SELECT COUNT(*) FROM public.heartbeat_events e
             LEFT JOIN public.devices d ON d.household_id = e.household_id AND d.device_id = e.device_id
             WHERE e.run_id = r.run_id AND e.household_id = r.household_id
               AND (e.battery_percent IS NULL OR e.battery_percent < 0 OR e.battery_percent > 100
                    OR d.device_id IS NULL OR e.event_time < d.registered_at)) AS invalid_accepted_count
        FROM selected r
        ORDER BY r.started_at DESC, r.run_id DESC
        """, (household_id, start_time, end_time, run_limit + 1))
    truncated = len(rows) > run_limit
    selected = rows[:run_limit]
    completed = [r for r in selected if r["status"] == "completed"]
    checks = []
    definitions = [
        ("run_count_balance", "Input equals accepted + duplicate + rejected", "input_count",
         lambda r: r["accepted_count"] + r["duplicate_count"] + r["rejected_count"]),
        ("accepted_row_count", "Stored accepted rows equal the run's accepted count", "accepted_count",
         lambda r: r["stored_accepted_count"]),
        ("rejected_row_count", "Stored rejection rows equal the run's rejected count", "rejected_count",
         lambda r: r["stored_rejected_count"]),
        ("accepted_event_validity", "Accepted rows have battery 0..100, registered device, and event time at/after registration",
         None, lambda r: r["invalid_accepted_count"]),
    ]
    for check_id, description, expected_field, observed in definitions:
        failures = []
        for run in completed:
            expected = run[expected_field] if expected_field else 0
            actual = observed(run)
            if actual != expected:
                failures.append({"run_id": run["run_id"], "source_key": run["source_key"],
                                 "expected": expected, "observed": actual})
        checks.append({"check_id": check_id, "description": description,
                       "status": "fail" if failures else "pass" if completed else "not_evaluated",
                       "examined_runs": len(completed), "affected_runs": len(failures),
                       "examples": failures[:5], "examples_truncated": len(failures) > 5})
    totals = {name: sum(r[name] for r in completed) for name in
              ("input_count", "accepted_count", "duplicate_count", "rejected_count", "stored_accepted_count", "stored_rejected_count")}
    run_states = {status: sum(r["status"] == status for r in selected) for status in ("completed", "running", "failed")}
    overall = ("fail" if any(c["status"] == "fail" for c in checks) else
               "incomplete" if truncated else "not_evaluated" if not completed else "pass")
    return {
        "scope": "consistency_of_selected_completed_ingestion_runs",
        "start_time": start_time, "end_time": end_time,
        "time_basis": "run_started_at", "window_bounds": "start_inclusive_end_exclusive",
        "run_limit": run_limit, "selected_runs": len(selected), "run_states": run_states,
        "truncated": truncated, "overall_status": overall, "checks": checks,
        "completed_run_totals": totals,
        "operational_attention_required": bool(run_states["running"] or run_states["failed"]),
        "persisted": False, "data_modified": False,
        "limitations": [
            "Checks cover selected completed runs only; running and failed runs are excluded from consistency checks.",
            "Pass is not proof of device health, source delivery completeness, absence of ingestion failures, or S3 content integrity.",
            "Duplicate counts are included in arithmetic but are not independently verified against raw input.",
            "Accepted validity checks cover battery range, device registration, and event time, not a full validator rerun.",
            "Results use the current database snapshot; source files and orphan records without selected runs are not scanned.",
            "A truncated selection omits older matching runs; example lists are separately capped at five per check.",
        ],
    }


DAILY_METRICS_SQL = "WITH bounds AS (\n    SELECT %s::text AS report_day, %s::timestamptz AS day_start, %s::timestamptz AS day_end\n), selected_devices AS (\n    SELECT d.household_id, d.device_id, d.device_type, d.registered_at\n    FROM public.devices AS d\n    CROSS JOIN bounds AS b\n    WHERE d.household_id = %s\n      AND d.registered_at < b.day_end\n    ORDER BY d.device_id\n    LIMIT %s\n), day_events AS (\n    SELECT e.*\n    FROM public.heartbeat_events AS e\n    JOIN selected_devices AS d\n      ON d.household_id = e.household_id\n     AND d.device_id = e.device_id\n    CROSS JOIN bounds AS b\n    WHERE e.event_time >= b.day_start\n      AND e.event_time < b.day_end\n), daily_totals AS (\n    SELECT\n        household_id,\n        device_id,\n        COUNT(*) AS accepted_heartbeat_count,\n        MIN(event_time) AS first_event_time,\n        MAX(event_time) AS last_event_time,\n        MIN(battery_percent) AS min_battery_percent,\n        MAX(battery_percent) AS max_battery_percent,\n        ROUND(AVG(battery_percent), 2) AS average_battery_percent\n    FROM day_events\n    GROUP BY household_id, device_id\n), latest_in_day AS (\n    SELECT DISTINCT ON (household_id, device_id)\n        household_id, device_id, event_id, battery_percent, received_at, run_id\n    FROM day_events\n    ORDER BY household_id, device_id, event_time DESC, event_id DESC\n)\nSELECT\n    b.report_day,\n    'UTC' AS report_timezone,\n    d.household_id,\n    d.device_id,\n    d.device_type,\n    d.registered_at,\n    COALESCE(t.accepted_heartbeat_count, 0) AS accepted_heartbeat_count,\n    t.first_event_time,\n    t.last_event_time,\n    t.min_battery_percent,\n    t.max_battery_percent,\n    t.average_battery_percent,\n    latest.event_id AS latest_event_id,\n    latest.battery_percent AS latest_battery_percent,\n    latest.received_at AS latest_event_received_at,\n    latest.run_id AS latest_event_run_id,\n    CURRENT_TIMESTAMP AS queried_at\nFROM selected_devices AS d\nCROSS JOIN bounds AS b\nLEFT JOIN daily_totals AS t\n  ON t.household_id = d.household_id\n AND t.device_id = d.device_id\nLEFT JOIN latest_in_day AS latest\n  ON latest.household_id = d.household_id\n AND latest.device_id = d.device_id\nORDER BY d.device_id;\n"


def get_daily_device_metrics(cursor, household_id, report_day, start, end, device_limit):
    rows = fetch_rows(cursor, DAILY_METRICS_SQL, (report_day, start, end, household_id, device_limit + 1))
    truncated = len(rows) > device_limit
    devices = rows[:device_limit]
    for row in devices:
        # PostgreSQL AVG(integer) returns Decimal; return an ordinary JSON number.
        if row["average_battery_percent"] is not None:
            row["average_battery_percent"] = float(row["average_battery_percent"])
    return {
        "scope": "accepted_daily_events_for_selected_registered_devices",
        "report_day": report_day,
        "report_timezone": "UTC",
        "start_time": start,
        "end_time": end,
        "window_bounds": "start_inclusive_end_exclusive",
        "time_basis": "event_time",
        "device_order": "device_id_asc",
        "devices": devices,
        "returned_devices": len(devices),
        "device_limit": device_limit,
        "truncated": truncated,
        "persisted": False,
        "data_modified": False,
        "data_basis": "currently_stored_accepted_events_including_late_arrivals_and_replays",
        "limitations": [
            "Current registrations before the day's end are included; this is not historical registry reconstruction.",
            "Zero accepted events means no accepted data for this day, not battery zero or confirmed downtime.",
            "Battery average is per accepted event, not time-weighted or a measurement of battery drain.",
            "Rejected input and duplicate attempts are excluded from accepted-event metrics.",
            "Late accepted arrivals and replays can change a subsequent report for the same day.",
            "Tied latest timestamps use event_id descending, not physical observation ordering.",
            "Truncation omits additional devices; all matching daily events for each returned device are aggregated.",
            "Results are calculated on request, not saved or scheduled."
        ],
    }


def get_pipeline_status(cursor, household_id):
    summary = fetch_rows(
        cursor,
        """
        SELECT
            COUNT(*) AS total_runs,
            COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
            COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
            COALESCE(SUM(input_count), 0) AS input_records,
            COALESCE(SUM(accepted_count), 0) AS accepted_records,
            COALESCE(SUM(duplicate_count), 0) AS duplicate_records,
            COALESCE(SUM(rejected_count), 0) AS rejected_records,
            MAX(started_at) AS latest_run_started_at,
            MAX(completed_at) FILTER (
                WHERE status = 'completed'
            ) AS latest_success_at
        FROM public.ingestion_runs
        WHERE household_id = %s
        """,
        (household_id,),
    )[0]

    recent_runs = fetch_rows(
        cursor,
        """
        SELECT
            run_id, status, source_bucket, source_key,
            started_at, completed_at, input_count,
            accepted_count, duplicate_count, rejected_count
        FROM public.ingestion_runs
        WHERE household_id = %s
        ORDER BY started_at DESC, run_id DESC
        LIMIT 20
        """,
        (household_id,),
    )

    return {
        "scope": "all recorded ingestion attempts",
        "summary": summary,
        "recent_runs": recent_runs,
        "recent_runs_limit": 20,
    }


def get_rejection_summary(cursor, household_id):
    rows = fetch_rows(
        cursor,
        """
        SELECT
            q.error_code,
            COUNT(*) AS rejection_count,
            MAX(q.created_at) AS latest_rejection_at
        FROM public.rejected_records AS q
        JOIN public.ingestion_runs AS r ON r.run_id = q.run_id
        WHERE r.household_id = %s
        GROUP BY q.error_code
        ORDER BY rejection_count DESC, q.error_code
        LIMIT 101
        """,
        (household_id,),
    )

    return {
        "scope": "all recorded rejection occurrences",
        "errors": rows[:100],
        "truncated": len(rows) > 100,
    }


def list_rejected_records(cursor, household_id, limit, error_code):
    rows = fetch_rows(
        cursor,
        """
        SELECT
            q.rejection_id,
            q.created_at,
            q.error_code,
            q.error_message,
            q.line_number,
            q.run_id,
            r.source_bucket,
            r.source_key
        FROM public.rejected_records AS q
        JOIN public.ingestion_runs AS r ON r.run_id = q.run_id
        WHERE r.household_id = %s
          AND (%s::text IS NULL OR q.error_code = %s)
        ORDER BY q.created_at DESC, q.rejection_id DESC
        LIMIT %s
        """,
        (household_id, error_code, error_code, limit + 1),
    )

    return {
        "scope": "rejection occurrences in the configured household's runs",
        "error_code_filter": error_code,
        "records": rows[:limit],
        "returned_records": min(len(rows), limit),
        "limit": limit,
        "truncated": len(rows) > limit,
    }


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def reject_json_constant(value):
    raise ValueError("Nonstandard JSON constant")


def inspect_rejected_record(cursor, household_id, rejection_id):
    rows = fetch_rows(
        cursor,
        """
        SELECT
            q.rejection_id,
            q.created_at,
            q.error_code,
            q.error_message,
            q.line_number,
            q.run_id,
            r.source_bucket,
            r.source_key,
            char_length(q.raw_record) AS raw_record_length,
            CASE WHEN char_length(q.raw_record) <= 16000
                 THEN q.raw_record ELSE NULL END AS raw_record
        FROM public.rejected_records AS q
        JOIN public.ingestion_runs AS r ON r.run_id = q.run_id
        WHERE r.household_id = %s AND q.rejection_id = %s
        """,
        (household_id, rejection_id),
    )

    if not rows:
        return {"found": False, "rejection_id": rejection_id}

    record = rows[0]
    raw_record = record.pop("raw_record")
    record["raw_record"] = None
    record["parsed_record"] = None
    record["raw_content_status"] = "withheld"

    if record["raw_record_length"] > 16000:
        record["withheld_reason"] = "Record exceeds the 16000-character inspection limit"
    else:
        try:
            parsed = json.loads(
                raw_record,
                object_pairs_hook=unique_json_object,
                parse_constant=reject_json_constant,
            )
        except (ValueError, RecursionError):
            record["withheld_reason"] = "Cannot verify household from unambiguous valid JSON"
        else:
            if not isinstance(parsed, dict) or parsed.get("household_id") != household_id:
                record["withheld_reason"] = "Record household does not match the configured household"
            else:
                record["raw_record"] = raw_record
                record["parsed_record"] = parsed
                record["raw_content_status"] = "included"
                record["withheld_reason"] = None

    return {"found": True, "record": record}


def preview_record_correction(cursor, household_id, rejection_id, battery_percent, reason):
    inspection = inspect_rejected_record(cursor, household_id, rejection_id)

    if not inspection["found"]:
        return {
            "preview_status": "unavailable",
            "rejection_id": rejection_id,
            "message": "No accessible rejection was found",
        }

    record = inspection["record"]
    if record["raw_content_status"] != "included":
        return {
            "preview_status": "unavailable",
            "rejection_id": rejection_id,
            "message": record["withheld_reason"],
        }

    original = record["parsed_record"]
    if record["error_code"] != "INVALID_BATTERY" or "battery_percent" not in original:
        return {
            "preview_status": "unsupported",
            "rejection_id": rejection_id,
            "message": "Only INVALID_BATTERY records with a battery field are supported",
        }

    if type(original["battery_percent"]) is int and original["battery_percent"] == battery_percent:
        return {
            "preview_status": "no_change",
            "rejection_id": rejection_id,
            "message": "The proposed value equals the stored value",
        }

    proposed = dict(original)
    proposed["battery_percent"] = battery_percent
    source_digest = hashlib.sha256(record["raw_record"].encode("utf-8")).hexdigest()
    preview = {
        "preview_version": 1,
        "household_id": household_id,
        "rejection_id": rejection_id,
        "source": {
            "run_id": record["run_id"],
            "bucket": record["source_bucket"],
            "key": record["source_key"],
            "line_number": record["line_number"],
            "raw_record_sha256": source_digest,
        },
        "original_record": original,
        "proposed_record": proposed,
        "changes": [{
            "field": "battery_percent",
            "before": original["battery_percent"],
            "after": battery_percent,
        }],
        "reason": reason.strip(),
    }
    canonical = json.dumps(
        preview, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )

    return {
        "preview_status": "ready",
        "preview": preview,
        "preview_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "persisted": False,
        "approval_status": "not_requested",
        "replay_validation": "not_performed",
        "message": "Preview only. No proposal saved, approval granted, or event replayed.",
    }


class ReplayRecordError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code

def reject_replay_json_constant(value):
    raise ReplayRecordError(
        "INVALID_JSON",
        f"{value} is not a valid JSON value",
    )

def unique_replay_json_fields(pairs):
    result = {}

    for key, value in pairs:
        if key in result:
            raise ReplayRecordError(
                "INVALID_JSON",
                f"Repeated JSON field: {key}",
            )
        result[key] = value

    return result

def validate_replay_event(line, household_id):
    try:
        record = json.loads(
            line,
            parse_constant=reject_replay_json_constant,
            object_pairs_hook=unique_replay_json_fields,
        )
    except json.JSONDecodeError as error:
        raise ReplayRecordError("INVALID_JSON", error.msg) from error

    if not isinstance(record, dict):
        raise ReplayRecordError(
            "INVALID_STRUCTURE",
            "Event must be a JSON object",
        )

    required = {
        "event_id",
        "household_id",
        "device_id",
        "event_time",
        "battery_percent",
    }

    missing = required - record.keys()
    if missing:
        raise ReplayRecordError(
            "MISSING_FIELDS",
            f"Missing fields: {', '.join(sorted(missing))}",
        )

    unexpected = record.keys() - required
    if unexpected:
        raise ReplayRecordError(
            "UNEXPECTED_FIELDS",
            f"Unexpected fields: {', '.join(sorted(unexpected))}",
        )

    for field in ("event_id", "household_id", "device_id"):
        value = record[field]

        if (
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or len(value) > 128
            or "\x00" in value
        ):
            raise ReplayRecordError(
                "INVALID_ID",
                f"{field} must be a non-empty ID of at most 128 characters",
            )

        try:
            value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ReplayRecordError(
                "INVALID_ID",
                f"{field} contains invalid Unicode",
            ) from error

    if record["household_id"] != household_id:
        raise ReplayRecordError(
            "HOUSEHOLD_MISMATCH",
            "Event is outside this pipeline's household scope",
        )

    timestamp = record["event_time"]

    if not isinstance(timestamp, str) or "T" not in timestamp:
        raise ReplayRecordError(
            "INVALID_TIMESTAMP",
            "event_time must be an ISO timestamp with a timezone",
        )

    try:
        event_time = datetime.fromisoformat(timestamp)

        if event_time.tzinfo is None:
            raise ValueError("Timezone is missing")

        event_time = event_time.astimezone(timezone.utc)
    except (ValueError, OverflowError) as error:
        raise ReplayRecordError(
            "INVALID_TIMESTAMP",
            "event_time must be a valid timestamp with a timezone",
        ) from error

    battery = record["battery_percent"]

    if type(battery) is not int or not 0 <= battery <= 100:
        raise ReplayRecordError(
            "INVALID_BATTERY",
            "battery_percent must be an integer from 0 to 100",
        )

    normalized = dict(record)
    normalized["event_time"] = event_time.isoformat()

    canonical = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return record, event_time, digest


def predict_event_outcome(cursor, household_id, record, event_time, candidate_digest):
    cursor.execute(
        """
        SELECT registered_at
        FROM public.devices
        WHERE household_id = %s AND device_id = %s
        """,
        (household_id, record["device_id"]),
    )
    device = cursor.fetchone()
    if device is None:
        raise ReplayRecordError("UNKNOWN_DEVICE", "Device is not registered for this household")
    if event_time < device[0]:
        raise ReplayRecordError("BEFORE_REGISTRATION", "Event time is earlier than device registration")

    cursor.execute(
        """
        SELECT digest
        FROM public.heartbeat_events
        WHERE household_id = %s AND event_id = %s
        """,
        (household_id, record["event_id"]),
    )
    existing = cursor.fetchone()
    if existing is None:
        return "accepted"
    if existing[0] == candidate_digest:
        return "duplicate"
    raise ReplayRecordError("EVENT_ID_CONFLICT", "This event ID already exists with different content")


def preview_replay(cursor, household_id, candidate):
    candidate_digest = None
    event_time = None
    error_code = None
    error_message = None
    try:
        raw = json.dumps(candidate, ensure_ascii=True, allow_nan=False)
        record, event_time, candidate_digest = validate_replay_event(raw, household_id)
        outcome = predict_event_outcome(
            cursor, household_id, record, event_time, candidate_digest
        )
    except ReplayRecordError as error:
        outcome = "rejected"
        error_code = error.code
        error_message = str(error)

    return {
        "scope": "one_candidate_against_current_database_snapshot",
        "predicted_outcome": outcome,
        "error_code": error_code,
        "error_message": error_message,
        "candidate_digest": candidate_digest,
        "normalized_event_time": event_time,
        "validator_rules_sha256": REPLAY_RULES_SHA256,
        "evaluated_at": datetime.now(timezone.utc),
        "data_modified": False,
        "replay_performed": False,
        "authorization_checked": False,
        "requires_revalidation": True,
        "message": "Prediction only. Replay must check authorization and validate again; database state and infrastructure availability can change.",
    }


def replay_run_outcome(run):
    """Describe one ingestion attempt, not the overall replay operation."""
    if run["status"] == "running":
        return "processing"
    if run["status"] == "failed":
        return "failed"
    if run["status"] != "completed":
        return "unexpected_run_shape"
    counts = tuple(run[name] for name in (
        "input_count", "accepted_count", "duplicate_count", "rejected_count"
    ))
    if any(type(value) is not int for value in counts):
        return "unexpected_run_shape"
    return {
        (1, 1, 0, 0): "accepted",
        (1, 0, 1, 0): "duplicate",
        (1, 0, 0, 1): "rejected",
    }.get(counts, "unexpected_run_shape")


def get_replay_ingestion_status(cursor, household_id, replay_id):
    """Read attempts for the deterministic one-record replay source.

    This does not check S3, authorize replay, or prove that a submission exists.
    Duplicate S3 delivery may produce multiple runs; preserve that evidence.
    """
    key = f"raw/replays/{household_id}/{replay_id}.jsonl"
    rows = fetch_rows(
        cursor,
        """
        SELECT run_id, status, started_at, completed_at,
               input_count, accepted_count, duplicate_count, rejected_count,
               COUNT(*) OVER () AS matching_run_count
        FROM public.ingestion_runs
        WHERE household_id = %s AND source_bucket = %s AND source_key = %s
        ORDER BY started_at DESC, run_id DESC
        LIMIT %s
        """,
        (household_id, REPLAY_BUCKET, key, REPLAY_RUN_LIMIT),
    )
    total = rows[0]["matching_run_count"] if rows else 0
    runs = []
    for row in rows:
        run = {name: value for name, value in row.items() if name != "matching_run_count"}
        run["record_outcome"] = replay_run_outcome(run)
        runs.append(run)
    return {
        "scope": "ingestion_attempts_for_one_replay_source",
        "replay_id": replay_id,
        "source_bucket": REPLAY_BUCKET,
        "source_key": key,
        "observation": "runs_observed" if total else "no_runs_observed",
        "matching_run_count": total,
        "runs": runs,
        "returned_runs": len(runs),
        "limit": REPLAY_RUN_LIMIT,
        "truncated": total > len(runs),
        "submission_checked": False,
        "data_modified": False,
        "message": (
            "Database evidence only. No runs does not prove that replay was not submitted. "
            "Each outcome describes one attempt; running can include an interrupted invocation. "
            "This lookup does not verify source content, approval, or the current event row."
        ),
    }


def serialize_value(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Unsupported response type: {type(value).__name__}")


def lambda_handler(event, context):
    try:
        operation, as_of, silence_minutes = parse_request(event)
    except ValueError as error:
        return {
            "status": "error",
            "error_code": "INVALID_REQUEST",
            "message": str(error),
        }

    connection = None

    try:
        household_id = os.environ["HOUSEHOLD_ID"]
        if not household_id.strip():
            raise RuntimeError("HOUSEHOLD_ID is not configured")

        connection = connect_database()
        cursor = connection.cursor()
        cursor.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        )
        cursor.execute("SET LOCAL statement_timeout = '5000ms'")
        cursor.execute("SELECT current_user")
        database_user = cursor.fetchone()[0]

        if database_user != "ring_reader":
            raise RuntimeError("Unexpected database identity")

        if operation == "get_daily_device_metrics":
            start, end = daily_window(event)
            result = get_daily_device_metrics(cursor, household_id, event["report_day"], start, end, event.get("device_limit", 100))
        elif operation == "get_device_health":
            result = get_device_health(
                cursor, household_id, as_of, silence_minutes
            )
        elif operation == "get_device_details":
            result = get_device_details(cursor, household_id, event["device_id"], as_of, silence_minutes)
        elif operation == "get_event_history":
            start_time, end_time = history_window(event)
            result = get_event_history(cursor, household_id, event["device_id"], start_time, end_time, event.get("limit", 20))
        elif operation == "get_quality_results":
            start_time, end_time = history_window(event)
            result = get_quality_results(cursor, household_id, start_time, end_time, event.get("run_limit", 100))
        elif operation == "get_pipeline_status":
            result = get_pipeline_status(cursor, household_id)
        elif operation == "get_rejection_summary":
            result = get_rejection_summary(cursor, household_id)
        elif operation == "list_rejected_records":
            result = list_rejected_records(
                cursor,
                household_id,
                event.get("limit", 20),
                event.get("error_code"),
            )
        elif operation == "inspect_rejected_record":
            result = inspect_rejected_record(
                cursor, household_id, event["rejection_id"]
            )
        elif operation == "preview_record_correction":
            result = preview_record_correction(
                cursor,
                household_id,
                event["rejection_id"],
                event["battery_percent"],
                event["reason"],
            )
        elif operation == "preview_replay":
            result = preview_replay(cursor, household_id, event["candidate"])
        elif operation == "get_replay_ingestion_status":
            result = get_replay_ingestion_status(cursor, household_id, event["replay_id"])

        connection.rollback()

        response = {
            "status": "ok",
            "operation": operation,
            "household_id": household_id,
            "database_user": database_user,
            "generated_at": datetime.now(timezone.utc),
            "result": result,
        }

        logger.info(json.dumps({
            "operation": operation,
            "household_id": household_id,
            "status": "ok",
            "request_id": context.aws_request_id,
        }))

        return json.loads(json.dumps(response, default=serialize_value))

    except Exception as error:
        logger.error(json.dumps({
            "operation": operation,
            "status": "failed",
            "error_type": type(error).__name__,
            "request_id": context.aws_request_id,
        }))
        raise RuntimeError(
            "Query failed; check configuration and CloudWatch logs"
        ) from None

    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                logger.warning("Database connection close failed")
