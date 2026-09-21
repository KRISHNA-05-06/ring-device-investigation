import argparse
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
DEFAULT_TIME = '2026-09-01T10:20:00Z'
HOUSEHOLD = os.getenv('HOUSEHOLD_ID', 'household-001')
DATABASE = Path(os.getenv('DEVICE_DB', str(ROOT / 'demo.sqlite')))


def timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError('Timezone required')
    return parsed.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON key')
        result[key] = value
    return result


def validate(raw: str, household: str) -> dict:
    try:
        event = json.loads(raw, object_pairs_hook=unique_keys)
    except (ValueError, RecursionError):
        raise ValueError('INVALID_JSON') from None
    fields = {'event_id', 'household_id', 'device_id', 'event_time', 'battery_percent'}
    if not isinstance(event, dict) or set(event) != fields:
        raise ValueError('INVALID_FIELDS')
    for key in fields - {'battery_percent'}:
        value = event[key]
        if not isinstance(value, str) or not value.strip() or len(value) > 200:
            raise ValueError('INVALID_FIELDS')
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('INVALID_FIELDS') from None
        if any(ord(char) < 32 for char in value):
            raise ValueError('INVALID_FIELDS')
    if event['household_id'] != household:
        raise ValueError('HOUSEHOLD_NOT_ALLOWED')
    battery = event['battery_percent']
    if type(battery) is not int or not 0 <= battery <= 100:
        raise ValueError('INVALID_BATTERY')
    try:
        timestamp(event['event_time'])
    except (ValueError, OverflowError):
        raise ValueError('INVALID_TIMESTAMP') from None
    return event


def ingest(db=DATABASE, source=ROOT / 'events.jsonl', household=HOUSEHOLD) -> dict:
    counts = dict(input=0, accepted=0, duplicate=0, rejected=0)
    run_id = str(uuid4())
    with closing(sqlite3.connect(db)) as conn:
        conn.executescript((ROOT / 'schema.sql').read_text())
        with conn:
            conn.execute('INSERT INTO ingestion_runs VALUES (?, ?, ?, 0, 0, 0, 0)',
                         (run_id, household, timestamp(datetime.now(timezone.utc).isoformat())))
            with Path(source).open(encoding='utf-8', newline='') as records:
                for line_number, raw in enumerate(records, 1):
                    counts['input'] += 1
                    try:
                        event = validate(raw, household)
                        digest = hashlib.sha256(json.dumps(event, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                        existing = conn.execute('SELECT digest FROM heartbeat_events WHERE household_id=? AND event_id=?',
                                                (household, event['event_id'])).fetchone()
                        if existing:
                            if existing[0] != digest:
                                raise ValueError('EVENT_ID_CONFLICT')
                            counts['duplicate'] += 1
                            continue
                        device = conn.execute('SELECT registered_at FROM devices WHERE household_id=? AND device_id=?',
                                              (household, event['device_id'])).fetchone()
                        if not device:
                            raise ValueError('UNKNOWN_DEVICE')
                        event_time = timestamp(event['event_time'])
                        if event_time < device[0]:
                            raise ValueError('BEFORE_REGISTRATION')
                        conn.execute('INSERT INTO heartbeat_events VALUES (?, ?, ?, ?, ?, ?, ?)',
                                     (household, event['event_id'], event['device_id'], event_time,
                                      event['battery_percent'], digest, run_id))
                        counts['accepted'] += 1
                    except ValueError as exc:
                        conn.execute('INSERT INTO rejected_records (run_id, line_number, error_code, raw_record) VALUES (?, ?, ?, ?)',
                                     (run_id, line_number, str(exc), raw))
                        counts['rejected'] += 1
            conn.execute('''UPDATE ingestion_runs SET completed_at=?, input_count=?, accepted_count=?,
                            duplicate_count=?, rejected_count=? WHERE run_id=?''',
                         (timestamp(datetime.now(timezone.utc).isoformat()), counts['input'], counts['accepted'],
                          counts['duplicate'], counts['rejected'], run_id))
    return dict(run_id=run_id, **counts)


def read_database(db=DATABASE):
    conn = sqlite3.connect(Path(db).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    return closing(conn)


def device_health(as_of=DEFAULT_TIME, silence_minutes=10, db=DATABASE, household=HOUSEHOLD):
    if type(silence_minutes) is not int or not 1 <= silence_minutes <= 1440:
        raise ValueError('silence_minutes must be between 1 and 1440')
    cutoff = timestamp(as_of)
    with read_database(db) as conn:
        rows = conn.execute('''
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY household_id, device_id ORDER BY event_time DESC, event_id DESC
                ) AS position
                FROM heartbeat_events WHERE household_id=? AND event_time<=?
            )
            SELECT d.device_id, d.device_type, e.event_id, e.event_time AS last_heartbeat,
                   e.battery_percent
            FROM devices d LEFT JOIN ranked e
              ON d.household_id=e.household_id AND d.device_id=e.device_id AND e.position=1
            WHERE d.household_id=? AND d.registered_at<=?
            ORDER BY d.device_id LIMIT 51
        ''', (household, cutoff, household, cutoff)).fetchall()
    devices = []
    for row in rows[:50]:
        item = dict(row)
        age = None if item['last_heartbeat'] is None else (
            datetime.fromisoformat(cutoff) - datetime.fromisoformat(item['last_heartbeat'])
        ).total_seconds() / 60
        item['silence_minutes'] = age
        item['status'] = 'unknown' if age is None else 'suspected_offline' if age > silence_minutes else 'reporting'
        devices.append(item)
    return dict(household_id=household, as_of=cutoff, synthetic=True, completeness='not_verified',
                threshold_minutes=silence_minutes, devices=devices, truncated=len(rows) > 50)


def pipeline_status(as_of=DEFAULT_TIME, db=DATABASE, household=HOUSEHOLD):
    cutoff = timestamp(as_of)
    with read_database(db) as conn:
        run = conn.execute('SELECT * FROM ingestion_runs WHERE household_id=? ORDER BY completed_at DESC LIMIT 1',
                           (household,)).fetchone()
        latest = conn.execute('SELECT max(event_time) FROM heartbeat_events WHERE household_id=? AND event_time<=?',
                              (household, cutoff)).fetchone()[0]
    lag = None if latest is None else (datetime.fromisoformat(cutoff) - datetime.fromisoformat(latest)).total_seconds() / 60
    return dict(household_id=household, latest_run=None if run is None else dict(run), as_of=cutoff,
                newest_event_at=latest, event_lag_minutes=lag, synthetic=True,
                completeness='not_verified', note='Event lag is not proof that all source records arrived.')


def rejection_summary(db=DATABASE, household=HOUSEHOLD):
    with read_database(db) as conn:
        rows = conn.execute('''SELECT q.error_code, count(*) AS rejected_attempts
            FROM rejected_records q JOIN ingestion_runs r ON r.run_id=q.run_id
            WHERE r.household_id=? GROUP BY q.error_code ORDER BY q.error_code LIMIT 20''', (household,)).fetchall()
    return dict(household_id=household, errors=[dict(row) for row in rows], raw_records_exposed=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=Path, default=DATABASE)
    parser.add_argument('--report', action='store_true')
    args = parser.parse_args()
    result = device_health(db=args.db) if args.report else ingest(db=args.db)
    print(json.dumps(result, indent=2))
