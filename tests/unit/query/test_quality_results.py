import copy
import io
import json
import os
import sqlite3
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from test_device_details import query, mcp, wire

START = datetime(2026, 9, 18, tzinfo=timezone.utc)
END = datetime(2026, 9, 20, tzinfo=timezone.utc)
REQUEST = dict(operation="get_quality_results", start_time=START.isoformat(), end_time=END.isoformat(), run_limit=100)


class SQLiteCursor:
    """Execute the query's portable SQL in an isolated fixture, not an AWS substitute."""
    def __init__(self, connection):
        self.cursor = connection.cursor()
    def execute(self, sql, params):
        self.cursor.execute(sql.replace("%s", "?"), tuple(v.isoformat() if isinstance(v, datetime) else v for v in params))
    @property
    def description(self):
        return self.cursor.description
    def fetchall(self):
        return self.cursor.fetchall()


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH DATABASE ':memory:' AS public")
        self.db.executescript("""
            CREATE TABLE public.devices (household_id TEXT, device_id TEXT, registered_at TEXT);
            CREATE TABLE public.ingestion_runs (
                run_id TEXT, household_id TEXT, status TEXT, started_at TEXT, source_key TEXT,
                input_count INTEGER, accepted_count INTEGER, duplicate_count INTEGER, rejected_count INTEGER);
            CREATE TABLE public.heartbeat_events (
                run_id TEXT, household_id TEXT, device_id TEXT, battery_percent INTEGER, event_time TEXT);
            CREATE TABLE public.rejected_records (run_id TEXT);
        """)
        self.db.execute("INSERT INTO public.devices VALUES (?, ?, ?)", ("household-001", "camera-001", "2026-09-01T00:00:00+00:00"))

    def tearDown(self):
        self.db.close()

    def seed(self, run="run-1", counts=(4, 2, 0, 2), status="completed", household="household-001", started=None):
        self.db.execute("INSERT INTO public.ingestion_runs VALUES (?,?,?,?,?,?,?,?,?)",
                        (run, household, status, started or START.isoformat(), "raw/example.jsonl", *counts))
        for _ in range(counts[1]):
            self.db.execute("INSERT INTO public.heartbeat_events VALUES (?,?,?,?,?)",
                            (run, household, "camera-001", 75, START.isoformat()))
        for _ in range(counts[3]):
            self.db.execute("INSERT INTO public.rejected_records VALUES (?)", (run,))

    def observe(self, limit=100):
        result = query.get_quality_results(SQLiteCursor(self.db), "household-001", START, END, limit)
        mcp.validate_quality_results(wire(result), {**REQUEST, "run_limit": limit})
        return result

    def test_rejections_are_not_consistency_failure(self):
        self.seed()
        result = self.observe()
        self.assertEqual(result["overall_status"], "pass")
        self.assertEqual(result["completed_run_totals"]["rejected_count"], 2)
        self.assertEqual(result["completed_run_totals"]["stored_rejected_count"], 2)
        self.assertTrue(all(c["status"] == "pass" for c in result["checks"]))

    def test_missing_accepted_row_produces_run_evidence(self):
        self.seed()
        self.db.execute("DELETE FROM public.heartbeat_events")
        result = self.observe()
        check = next(c for c in result["checks"] if c["check_id"] == "accepted_row_count")
        self.assertEqual(check["examples"], [dict(run_id="run-1", source_key="raw/example.jsonl", expected=2, observed=0)])
        self.assertEqual(result["overall_status"], "fail")

    def test_missing_rejection_row_and_bad_arithmetic(self):
        self.seed()
        self.db.execute("DELETE FROM public.rejected_records")
        self.db.execute("UPDATE public.ingestion_runs SET input_count=5")
        failed = {c["check_id"] for c in self.observe()["checks"] if c["status"] == "fail"}
        self.assertEqual(failed, {"run_count_balance", "rejected_row_count"})

    def test_accepted_validity_detects_battery_and_registration_issues(self):
        self.seed(counts=(1, 1, 0, 0))
        for assignment in ("battery_percent=150", "battery_percent=75, device_id='missing'",
                           "device_id='camera-001', event_time='2026-08-01T00:00:00+00:00'"):
            self.db.execute("UPDATE public.heartbeat_events SET " + assignment)
            self.assertEqual(self.observe()["checks"][3]["affected_runs"], 1)

    def test_duplicate_run_does_not_require_another_accepted_row(self):
        self.seed(counts=(1, 1, 0, 0))
        self.seed(run="retry", counts=(1, 0, 1, 0))
        result = self.observe()
        self.assertEqual(result["overall_status"], "pass")
        self.assertEqual(result["completed_run_totals"]["stored_accepted_count"], 1)

    def test_failed_and_running_context_is_separate(self):
        self.seed()
        self.seed(run="failed", counts=(0, 0, 0, 0), status="failed")
        self.seed(run="running", counts=(0, 0, 0, 0), status="running")
        result = self.observe()
        self.assertEqual(result["overall_status"], "pass")
        self.assertTrue(result["operational_attention_required"])
        self.assertEqual(result["run_states"], dict(completed=1, running=1, failed=1))
        self.assertEqual(result["checks"][0]["examined_runs"], 1)

    def test_empty_and_only_failed_are_not_evaluated(self):
        self.assertEqual(self.observe()["overall_status"], "not_evaluated")
        self.seed(status="failed", counts=(0, 0, 0, 0))
        self.assertEqual(self.observe()["overall_status"], "not_evaluated")

    def test_household_scope_window_and_late_event_time(self):
        self.seed(run="included")
        self.seed(run="other-house", household="household-002", counts=(99, 0, 0, 0))
        self.seed(run="at-end", started=END.isoformat(), counts=(99, 0, 0, 0))
        self.seed(run="before", started="2026-09-17T23:59:59+00:00", counts=(99, 0, 0, 0))
        self.db.execute("UPDATE public.heartbeat_events SET event_time='2026-09-02T00:00:00+00:00'")
        result = self.observe()
        self.assertEqual(result["selected_runs"], 1)
        self.assertEqual(result["overall_status"], "pass")

    def test_selection_truncation_never_overall_pass(self):
        self.seed(run="a")
        self.seed(run="b")
        result = self.observe(limit=1)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["overall_status"], "incomplete")
        self.db.execute("UPDATE public.ingestion_runs SET accepted_count=3 WHERE run_id='b'")
        self.assertEqual(self.observe(limit=1)["overall_status"], "fail")

    def test_failure_examples_bounded_separately(self):
        for i in range(6):
            self.seed(run=str(i), counts=(5, 2, 0, 2))
        result = self.observe()
        check = result["checks"][0]
        self.assertEqual(check["affected_runs"], 6)
        self.assertEqual(len(check["examples"]), 5)
        self.assertTrue(check["examples_truncated"])
        self.assertFalse(result["truncated"])

    def test_input_errors_do_not_connect_to_database(self):
        invalid = [{**REQUEST, "run_limit": v} for v in (True, 0, 201, "100")]
        invalid += [{**REQUEST, "household_id": "other"}, {**REQUEST, "start_time": "bad"}, {**REQUEST, "limit": 5}]
        with patch.object(query, "connect_database") as connect:
            for event in invalid:
                self.assertEqual(query.lambda_handler(event, None)["error_code"], "INVALID_REQUEST")
            connect.assert_not_called()

    def test_handler_read_only_and_database_error_not_pass(self):
        self.seed()
        expected = self.observe()
        connection = MagicMock()
        connection.cursor.return_value.fetchone.return_value = ("ring_reader",)
        with patch.dict(os.environ, HOUSEHOLD_ID="household-001"), \
             patch.object(query, "connect_database", return_value=connection), \
             patch.object(query, "get_quality_results", return_value=expected):
            response = query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="quality-test"))
        self.assertEqual(response["result"]["overall_status"], "pass")
        connection.cursor.return_value.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()
        with patch.dict(os.environ, HOUSEHOLD_ID="household-001"), patch.object(query, "connect_database", side_effect=RuntimeError()):
            with self.assertRaises(RuntimeError):
                query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="quality-test"))

    def test_mcp_calls_query_with_exact_window(self):
        self.seed()
        body = dict(status="ok", operation="get_quality_results", household_id="household-001",
                    database_user="ring_reader", result=wire(self.observe()))
        client = MagicMock()
        client.invoke.return_value = {"Payload": io.BytesIO(json.dumps(body).encode())}
        with patch.object(mcp.boto3, "Session") as session:
            session.return_value.client.return_value = client
            response = mcp.get_quality_results(START.isoformat(), END.isoformat())
        self.assertEqual(response["result"], body["result"])
        self.assertEqual(json.loads(client.invoke.call_args.kwargs["Payload"]), REQUEST)

    def test_mcp_rejects_contradictory_verdict_or_scope(self):
        self.seed(counts=(5, 2, 0, 2))
        good = wire(self.observe())
        for key, value in (("overall_status", "pass"), ("persisted", True), ("selected_runs", 1000), ("time_basis", "event_time")):
            with self.assertRaises(RuntimeError):
                mcp.validate_quality_results({**good, key: value}, REQUEST)
        bad = copy.deepcopy(good)
        bad["checks"][0]["examples"] = []
        with self.assertRaises(RuntimeError):
            mcp.validate_quality_results(bad, REQUEST)


if __name__ == "__main__":
    unittest.main(verbosity=2)
