import hashlib
import json
import logging
import os
import ssl
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import pg8000.dbapi
from botocore.config import Config


logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = "ring-device-investigation-455646290306-us-east-1"
HOUSEHOLD_ID = os.environ["HOUSEHOLD_ID"]

MAX_FILE_BYTES = 1_048_576
MAX_RECORDS = 500

s3 = boto3.client(
    "s3",
    config=Config(
        connect_timeout=3,
        read_timeout=5,
        retries={"mode": "standard", "total_max_attempts": 2},
    ),
)

TLS_CONTEXT = ssl.create_default_context(
    cafile=os.environ["DB_SSLROOTCERT"]
)


class RecordError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def connect_database():
    return pg8000.dbapi.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        ssl_context=TLS_CONTEXT,
        timeout=5,
    )


def check_database():
    connection = connect_database()

    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT current_database(), current_user, COUNT(*)
            FROM public.devices
            WHERE household_id = %s
            """,
            (HOUSEHOLD_ID,),
        )
        database, username, device_count = cursor.fetchone()

        return {
            "status": "connected",
            "database": database,
            "database_user": username,
            "household_id": HOUSEHOLD_ID,
            "registered_devices": device_count,
        }
    finally:
        connection.close()


def reject_json_constant(value):
    raise RecordError(
        "INVALID_JSON",
        f"{value} is not a valid JSON value",
    )


def unique_json_fields(pairs):
    result = {}

    for key, value in pairs:
        if key in result:
            raise RecordError(
                "INVALID_JSON",
                f"Repeated JSON field: {key}",
            )
        result[key] = value

    return result


def validate_event(line):
    try:
        record = json.loads(
            line,
            parse_constant=reject_json_constant,
            object_pairs_hook=unique_json_fields,
        )
    except json.JSONDecodeError as error:
        raise RecordError("INVALID_JSON", error.msg) from error

    if not isinstance(record, dict):
        raise RecordError(
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
        raise RecordError(
            "MISSING_FIELDS",
            f"Missing fields: {', '.join(sorted(missing))}",
        )

    unexpected = record.keys() - required
    if unexpected:
        raise RecordError(
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
            raise RecordError(
                "INVALID_ID",
                f"{field} must be a non-empty ID of at most 128 characters",
            )

        try:
            value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise RecordError(
                "INVALID_ID",
                f"{field} contains invalid Unicode",
            ) from error

    if record["household_id"] != HOUSEHOLD_ID:
        raise RecordError(
            "HOUSEHOLD_MISMATCH",
            "Event is outside this pipeline's household scope",
        )

    timestamp = record["event_time"]

    if not isinstance(timestamp, str) or "T" not in timestamp:
        raise RecordError(
            "INVALID_TIMESTAMP",
            "event_time must be an ISO timestamp with a timezone",
        )

    try:
        event_time = datetime.fromisoformat(timestamp)

        if event_time.tzinfo is None:
            raise ValueError("Timezone is missing")

        event_time = event_time.astimezone(timezone.utc)
    except (ValueError, OverflowError) as error:
        raise RecordError(
            "INVALID_TIMESTAMP",
            "event_time must be a valid timestamp with a timezone",
        ) from error

    battery = record["battery_percent"]

    if type(battery) is not int or not 0 <= battery <= 100:
        raise RecordError(
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


def read_source(event):
    request = {
        "Bucket": BUCKET,
        "Key": event["key"],
    }

    if event.get("etag"):
        request["IfMatch"] = '"' + event["etag"].strip('"') + '"'

    response = s3.get_object(**request)
    body = response["Body"]

    try:
        content = body.read(MAX_FILE_BYTES + 1)
    finally:
        body.close()

    if len(content) > MAX_FILE_BYTES:
        raise ValueError("File exceeds the 1 MiB limit")

    text = content.decode("utf-8-sig")

    if "\x00" in text:
        raise ValueError("File contains an unsupported null character")

    lines = [
        (number, line)
        for number, line in enumerate(text.splitlines(), start=1)
        if line.strip()
    ]

    if len(lines) > MAX_RECORDS:
        raise ValueError("File exceeds the 500-record limit")

    return lines


def store_event(cursor, record, event_time, digest, run_id, line_number):
    cursor.execute(
        """
        SELECT registered_at
        FROM public.devices
        WHERE household_id = %s AND device_id = %s
        """,
        (HOUSEHOLD_ID, record["device_id"]),
    )
    device = cursor.fetchone()

    if device is None:
        raise RecordError(
            "UNKNOWN_DEVICE",
            "Device is not registered for this household",
        )

    if event_time < device[0]:
        raise RecordError(
            "BEFORE_REGISTRATION",
            "Event time is earlier than device registration",
        )

    cursor.execute(
        """
        INSERT INTO public.heartbeat_events (
            household_id,
            event_id,
            device_id,
            event_time,
            battery_percent,
            digest,
            run_id,
            line_number
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (household_id, event_id) DO NOTHING
        RETURNING event_id
        """,
        (
            HOUSEHOLD_ID,
            record["event_id"],
            record["device_id"],
            event_time,
            record["battery_percent"],
            digest,
            run_id,
            line_number,
        ),
    )

    if cursor.fetchone() is not None:
        return "accepted"

    cursor.execute(
        """
        SELECT digest
        FROM public.heartbeat_events
        WHERE household_id = %s AND event_id = %s
        """,
        (HOUSEHOLD_ID, record["event_id"]),
    )
    existing = cursor.fetchone()

    if existing is None:
        raise RuntimeError("Conflicting event disappeared during processing")

    if existing[0] == digest:
        return "duplicate"

    raise RecordError(
        "EVENT_ID_CONFLICT",
        "This event ID already exists with different content",
    )


def write_export(prefix, run_id, records):
    if not records:
        return None

    key = f"{prefix}/{run_id}.jsonl"
    content = "\n".join(json.dumps(record) for record in records) + "\n"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    return key


def process_file(event, context, file_index=0):
    key = event.get("key")

    if (
        not isinstance(key, str)
        or not key.startswith("raw/")
        or key.endswith("/")
    ):
        raise ValueError("key must identify a file under raw/")

    run_id = f"{context.aws_request_id}-{file_index}"
    connection = connect_database()
    run_created = False

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO public.ingestion_runs (
                run_id, household_id, source_bucket, source_key
            )
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, HOUSEHOLD_ID, BUCKET, key),
        )
        connection.commit()
        run_created = True

        lines = read_source(event)
        accepted = []
        rejected = []
        duplicate_count = 0

        for line_number, line in lines:
            if context.get_remaining_time_in_millis() < 15_000:
                raise RuntimeError("Insufficient execution time to finish safely")

            try:
                record, event_time, digest = validate_event(line)

                outcome = store_event(
                    cursor,
                    record,
                    event_time,
                    digest,
                    run_id,
                    line_number,
                )

                if outcome == "duplicate":
                    duplicate_count += 1
                else:
                    accepted.append({
                        "source_bucket": BUCKET,
                        "source_key": key,
                        "line_number": line_number,
                        "run_id": run_id,
                        "event": record,
                    })

            except RecordError as error:
                cursor.execute(
                    """
                    INSERT INTO public.rejected_records (
                        run_id,
                        line_number,
                        error_code,
                        error_message,
                        raw_record
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        line_number,
                        error.code,
                        str(error),
                        line,
                    ),
                )

                rejected.append({
                    "source_bucket": BUCKET,
                    "source_key": key,
                    "line_number": line_number,
                    "run_id": run_id,
                    "error_code": error.code,
                    "reason": str(error),
                    "raw_record": line,
                })

        validated_key = write_export(
            "curated/validated",
            run_id,
            accepted,
        )
        rejected_key = write_export(
            "rejected",
            run_id,
            rejected,
        )

        cursor.execute(
            """
            UPDATE public.ingestion_runs
            SET status = 'completed',
                completed_at = clock_timestamp(),
                input_count = %s,
                accepted_count = %s,
                duplicate_count = %s,
                rejected_count = %s
            WHERE run_id = %s
            """,
            (
                len(lines),
                len(accepted),
                duplicate_count,
                len(rejected),
                run_id,
            ),
        )

        connection.commit()

        result = {
            "run_id": run_id,
            "status": "completed",
            "source_key": key,
            "total_records": len(lines),
            "accepted_records": len(accepted),
            "duplicate_records": duplicate_count,
            "rejected_records": len(rejected),
            "validated_key": validated_key,
            "rejected_key": rejected_key,
        }

        logger.info(json.dumps(result))
        return result

    except Exception as error:
        logger.exception("Processing failed for run %s", run_id)

        try:
            connection.rollback()

            if run_created:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    UPDATE public.ingestion_runs
                    SET status = 'failed',
                        completed_at = clock_timestamp(),
                        error_message = %s
                    WHERE run_id = %s AND status = 'running'
                    """,
                    (type(error).__name__, run_id),
                )
                connection.commit()
        except Exception:
            logger.exception("Could not record failure for run %s", run_id)

        raise

    finally:
        connection.close()


def lambda_handler(event, context):
    if event.get("action") == "check_database":
        return check_database()

    if event.get("Event") == "s3:TestEvent":
        return {"status": "notification configuration checked"}

    if "Records" not in event:
        return process_file(event, context)

    results = []

    for file_index, notification in enumerate(event["Records"]):
        if notification.get("eventSource") != "aws:s3":
            raise ValueError("Unsupported event source")

        if not notification.get("eventName", "").startswith("ObjectCreated:"):
            continue

        bucket = notification["s3"]["bucket"]["name"]
        source = notification["s3"]["object"]
        key = unquote_plus(source["key"])

        if bucket != BUCKET:
            raise ValueError("Unexpected source bucket")

        if not key.startswith("raw/") or key.endswith("/"):
            continue

        result = process_file(
            {
                "key": key,
                "etag": source.get("eTag"),
            },
            context,
            file_index,
        )
        results.append(result)

    return {
        "files_processed": len(results),
        "results": results,
    }