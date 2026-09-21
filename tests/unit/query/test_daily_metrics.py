import json
import os
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from test_replay_ingestion_status import query

REQUEST = {"operation": "get_daily_device_metrics", "report_day": "2026-09-18"}


class FixtureCursor:
    """Execute SQL against local fixtures with explicit PostgreSQL dialect adaptations.

    This tests selection/aggregation; deployed pg8000/PostgreSQL is verified separately.
    """
    def __init__(self, db):
        self.cursor = db.cursor()

    def execute(self, sql, parameters):
        sql = sql.replace("%s::text", "%s").replace("%s::timestamptz", "%s")
        old = """SELECT DISTINCT ON (household_id, device_id)
        household_id, device_id, event_id, battery_percent, received_at, run_id
    FROM day_events
    ORDER BY household_id, device_id, event_time DESC, event_id DESC"""
        new = """SELECT household_id, device_id, event_id, battery_percent, received_at, run_id
    FROM (SELECT *, ROW_NUMBER() OVER (
        PARTITION BY household_id, device_id ORDER BY event_time DESC, event_id DESC
    ) AS position FROM day_events) WHERE position = 1"""
        assert old in sql
        sql = sql.replace(old, new).replace("%s", "?")
        values = tuple(v.isoformat() if isinstance(v, datetime) else v for v in parameters)
        self.cursor.execute(sql, values)

    @property
    def description(self):
        return self.cursor.description

    def fetchall(self):
        return self.cursor.fetchall()


class DailyMetricsTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH DATABASE ':memory:' AS public")
        self.db.executescript("""
            CREATE TABLE public.devices (household_id TEXT, device_id TEXT, device_type TEXT, registered_at TEXT);
            CREATE TABLE public.heartbeat_events (household_id TEXT, device_id TEXT, event_id TEXT,
                event_time TEXT, battery_percent INTEGER, received_at TEXT, run_id TEXT);
        """)
        for household, device, registered in (
            ("household-001", "camera-001", "2026-09-01T09:00:00+00:00"),
            ("household-001", "camera-002", "2026-09-01T09:00:00+00:00"),
            ("household-002", "camera-001", "2026-09-01T09:00:00+00:00"),
            ("household-001", "future", "2026-09-19T00:00:00+00:00"),
        ):
            self.db.execute("INSERT INTO public.devices VALUES (?,?,?,?)", (household, device, "camera", registered))

    def tearDown(self):
        self.db.close()

    def seed(self, event_id, time, battery, household="household-001", device="camera-001"):
        self.db.execute("INSERT INTO public.heartbeat_events VALUES (?,?,?,?,?,?,?)",
                        (household, device, event_id, time, battery, "2026-09-20T00:00:00+00:00", "run-" + event_id))

    def observe(self, limit=100):
        start, end = query.daily_window(REQUEST)
        return query.get_daily_device_metrics(FixtureCursor(self.db), "household-001", REQUEST["report_day"], start, end, limit)

    def test_empty_day_retains_devices_null_readings_excludes_future_registration(self):
        result = self.observe()
        self.assertEqual([d["device_id"] for d in result["devices"]], ["camera-001", "camera-002"])
        for row in result["devices"]:
            self.assertEqual(row["accepted_heartbeat_count"], 0)
            self.assertIsNone(row["average_battery_percent"])
            self.assertIsNone(row["latest_event_id"])

    def test_aggregation_tie_break_late_arrival_and_household_scope(self):
        self.seed("a", "2026-09-18T12:00:00+00:00", 0)
        self.seed("b", "2026-09-18T12:00:00+00:00", 100)
        self.seed("other", "2026-09-18T12:00:00+00:00", 25, household="household-002")
        row = self.observe()["devices"][0]
        self.assertEqual(row["accepted_heartbeat_count"], 2)
        self.assertEqual(row["min_battery_percent"], 0)
        self.assertEqual(row["max_battery_percent"], 100)
        self.assertEqual(row["average_battery_percent"], 50.0)
        self.assertEqual(row["latest_event_id"], "b")
        self.assertEqual(row["latest_event_run_id"], "run-b")
        self.assertEqual(row["latest_event_received_at"], "2026-09-20T00:00:00+00:00")

    def test_half_open_boundaries(self):
        self.seed("before", "2026-09-17T23:59:59.999999+00:00", 1)
        self.seed("start", "2026-09-18T00:00:00+00:00", 10)
        self.seed("last", "2026-09-18T23:59:59.999999+00:00", 20)
        self.seed("end", "2026-09-19T00:00:00+00:00", 100)
        row = self.observe()["devices"][0]
        self.assertEqual(row["accepted_heartbeat_count"], 2)
        self.assertEqual(row["latest_event_id"], "last")
        self.assertEqual(row["average_battery_percent"], 15.0)

    def test_limit_reports_omitted_devices_not_omitted_event_counts(self):
        self.seed("a", "2026-09-18T10:00:00+00:00", 20)
        self.seed("b", "2026-09-18T12:00:00+00:00", 40)
        result = self.observe(1)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["returned_devices"], 1)
        self.assertEqual(result["devices"][0]["accepted_heartbeat_count"], 2)

    def test_invalid_requests_fail_before_database_connection(self):
        for update in ({"report_day": "2026-02-30"}, {"report_day": "2026-9-18"},
                       {"report_day": None}, {"report_day": "9999-12-31"},
                       {"report_day": "2026-09-18T00:00:00Z"},
                       {"device_limit": True}, {"device_limit": 0}, {"device_limit": 101},
                       {"household_id": "household-002"}, {"sql": "SELECT 1"}):
            with self.subTest(update=update), patch.object(query, "connect_database") as connect:
                response = query.lambda_handler({**REQUEST, **update}, SimpleNamespace(aws_request_id="test"))
                self.assertEqual(response["error_code"], "INVALID_REQUEST")
                connect.assert_not_called()

    def test_leap_day_utc_window(self):
        start, end = query.daily_window({"report_day": "2024-02-29"})
        self.assertEqual(end.isoformat(), "2024-03-01T00:00:00+00:00")
        self.assertEqual((end - start).total_seconds(), 86400)

    def test_decimal_null_serialization_and_handler_read_only_identity(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = ("ring_reader",)
        rows = [{"average_battery_percent": Decimal("74.33")}, {"average_battery_percent": None}]
        with patch.dict(os.environ, {"HOUSEHOLD_ID": "household-001"}), \
             patch.object(query, "connect_database", return_value=connection), \
             patch.object(query, "fetch_rows", return_value=rows) as fetch:
            response = query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["result"]["devices"][0]["average_battery_percent"], 74.33)
        self.assertIsNone(response["result"]["devices"][1]["average_battery_percent"])
        self.assertFalse(response["result"]["persisted"])
        json.dumps(response)
        self.assertEqual(fetch.call_args.args[2][-2:], ("household-001", 101))
        cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
