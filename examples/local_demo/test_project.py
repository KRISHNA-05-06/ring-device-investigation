import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import ROOT, device_health, ingest, read_database, rejection_summary, validate


class ProjectTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / 'test.sqlite'
        self.result = ingest(db=self.db)

    def test_expected_results(self):
        self.assertEqual([self.result[key] for key in ('input', 'accepted', 'duplicate', 'rejected')], [8, 3, 1, 4])
        codes = {row['error_code'] for row in rejection_summary(db=self.db)['errors']}
        self.assertEqual(codes, {'EVENT_ID_CONFLICT', 'UNKNOWN_DEVICE', 'INVALID_JSON', 'HOUSEHOLD_NOT_ALLOWED'})

    def test_retry_preserves_events(self):
        result = ingest(db=self.db)
        self.assertEqual([result[key] for key in ('accepted', 'duplicate', 'rejected')], [0, 4, 4])
        with read_database(self.db) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM heartbeat_events').fetchone()[0], 3)

    def test_late_event_does_not_replace_latest(self):
        devices = device_health(db=self.db)['devices']
        doorbell = next(d for d in devices if d['device_id'] == 'doorbell-001')
        self.assertEqual((doorbell['event_id'], doorbell['battery_percent']), ('evt-001', 85))

    def test_historical_cutoff(self):
        devices = device_health('2026-09-01T10:10:00Z', db=self.db)['devices']
        self.assertEqual(next(d for d in devices if d['device_id'] == 'doorbell-001')['event_id'], 'evt-003')

    def test_exact_threshold_is_reporting(self):
        devices = device_health('2026-09-01T10:00:00Z', db=self.db)['devices']
        self.assertEqual(next(d for d in devices if d['device_id'] == 'camera-001')['status'], 'reporting')

    def test_no_heartbeat_is_unknown(self):
        devices = device_health(db=self.db)['devices']
        self.assertEqual(next(d for d in devices if d['device_id'] == 'camera-002')['status'], 'unknown')

    def test_household_filter(self):
        devices = device_health(db=self.db)['devices']
        self.assertNotIn('private-camera', [d['device_id'] for d in devices])
        other = device_health(db=self.db, household='household-002')['devices']
        self.assertEqual([d['device_id'] for d in other], ['private-camera'])

    def test_read_only_connection(self):
        with read_database(self.db) as conn, self.assertRaises(sqlite3.OperationalError):
            conn.execute('DELETE FROM devices')

    def test_invalid_values(self):
        event = json.loads((ROOT / 'events.jsonl').read_text().splitlines()[0])
        for value in (True, -1, 101, '85'):
            event['battery_percent'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate(json.dumps(event), 'household-001')
        with self.assertRaises(ValueError):
            validate('{"event_id":"a","event_id":"b"}', 'household-001')
        with self.assertRaises(ValueError):
            device_health('2026-09-01T10:20:00', db=self.db)

    def test_before_registration(self):
        event = json.loads((ROOT / 'events.jsonl').read_text().splitlines()[0])
        event.update(event_id='early', event_time='2026-08-01T10:00:00Z')
        source = Path(self.temp.name) / 'early.jsonl'
        source.write_text(json.dumps(event))
        result = ingest(db=self.db, source=source)
        self.assertEqual(result['rejected'], 1)

    def test_source_failure_rolls_back(self):
        with read_database(self.db) as conn:
            before = conn.execute('SELECT count(*) FROM ingestion_runs').fetchone()[0]
        with self.assertRaises(FileNotFoundError):
            ingest(db=self.db, source=Path(self.temp.name) / 'missing.jsonl')
        with read_database(self.db) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM ingestion_runs').fetchone()[0], before)


if __name__ == '__main__':
    unittest.main()
