import os
import sys
import types
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

try:
    import pg8000.dbapi
except ModuleNotFoundError:
    package = types.ModuleType("pg8000")
    package.dbapi = types.ModuleType("pg8000.dbapi")
    sys.modules["pg8000"] = package
    sys.modules["pg8000.dbapi"] = package.dbapi

import query_lambda as query

REPLAY_ID = "987d6f004086868297234141091fd0db704219732f884ebed709e13397aa47ed"
REQUEST = {"operation": "get_replay_ingestion_status", "replay_id": REPLAY_ID}


def run_row(status="completed", counts=(1, 1, 0, 0), total=1, run_id="run-1"):
    return {
        "run_id": run_id, "status": status,
        "started_at": datetime(2026, 9, 19, 21, tzinfo=timezone.utc),
        "completed_at": None if status == "running" else datetime(2026, 9, 19, 21, 1, tzinfo=timezone.utc),
        **dict(zip(("input_count", "accepted_count", "duplicate_count", "rejected_count"), counts)),
        "matching_run_count": total,
    }


class ReplayStatusTests(unittest.TestCase):
    def observe(self, rows):
        with patch.object(query, "fetch_rows", return_value=rows):
            return query.get_replay_ingestion_status(object(), "household-001", REPLAY_ID)

    def test_no_runs_is_not_a_submission_verdict(self):
        result = self.observe([])
        self.assertEqual(result["observation"], "no_runs_observed")
        self.assertEqual(result["matching_run_count"], 0)
        self.assertEqual(result["runs"], [])
        self.assertFalse(result["submission_checked"])
        self.assertFalse(result["data_modified"])
        self.assertFalse(result["truncated"])

    def test_each_one_record_outcome(self):
        for counts, expected in [((1, 1, 0, 0), "accepted"), ((1, 0, 1, 0), "duplicate"), ((1, 0, 0, 1), "rejected")]:
            with self.subTest(expected=expected):
                self.assertEqual(self.observe([run_row(counts=counts)])["runs"][0]["record_outcome"], expected)

    def test_running_and_failed_are_not_accepted(self):
        for status, expected in [("running", "processing"), ("failed", "failed")]:
            self.assertEqual(self.observe([run_row(status=status, counts=(0, 0, 0, 0))])["runs"][0]["record_outcome"], expected)

    def test_unexpected_counts_do_not_claim_single_event_success(self):
        for counts in [(2, 2, 0, 0), (0, 0, 0, 0), (1, 1, 1, 0), (1, True, 0, 0)]:
            self.assertEqual(self.observe([run_row(counts=counts)])["runs"][0]["record_outcome"], "unexpected_run_shape")

    def test_duplicate_attempt_does_not_hide_original_acceptance(self):
        result = self.observe([
            run_row(counts=(1, 0, 1, 0), total=2, run_id="retry"),
            run_row(total=2, run_id="original"),
        ])
        self.assertEqual([r["record_outcome"] for r in result["runs"]], ["duplicate", "accepted"])
        self.assertEqual(result["matching_run_count"], 2)

    def test_truncation_retains_total_and_does_not_invent_overall_outcome(self):
        result = self.observe([run_row(total=51, run_id=str(i)) for i in range(50)])
        self.assertTrue(result["truncated"])
        self.assertEqual(result["returned_runs"], 50)
        self.assertEqual(result["matching_run_count"], 51)
        self.assertNotIn("overall_outcome", result)

    def test_sql_is_exact_scoped_parameterized_and_bounded(self):
        cursor = MagicMock()
        cursor.description = [("run_id",)]
        cursor.fetchall.return_value = []
        query.get_replay_ingestion_status(cursor, "household-001", REPLAY_ID)
        sql, parameters = cursor.execute.call_args.args
        self.assertEqual(parameters, ("household-001", query.REPLAY_BUCKET, f"raw/replays/household-001/{REPLAY_ID}.jsonl", 50))
        self.assertIn("household_id = %s AND source_bucket = %s AND source_key = %s", sql)
        self.assertIn("COUNT(*) OVER ()", sql)
        self.assertIn("ORDER BY started_at DESC, run_id DESC", sql)
        self.assertIn("LIMIT %s", sql)
        self.assertNotIn(REPLAY_ID, sql)

    def test_invalid_ids_and_extra_fields_rejected_before_database_connection(self):
        requests = [{**REQUEST, "replay_id": value} for value in [None, True, 1, "", "a" * 63, "A" * 64, "../raw/file", "x" * 64]]
        requests += [{"operation": REQUEST["operation"]}]
        requests += [{**REQUEST, key: value} for key, value in [("household_id", "household-002"), ("source_key", "raw/other.json"), ("approved", True), ("limit", 100)]]
        with patch.object(query, "connect_database") as connect:
            for request in requests:
                with self.subTest(request=request):
                    result = query.lambda_handler(request, SimpleNamespace(aws_request_id="test"))
                    self.assertEqual(result["error_code"], "INVALID_REQUEST")
            connect.assert_not_called()

    def test_handler_keeps_read_only_snapshot_serializes_and_closes(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = ("ring_reader",)
        with patch.dict(os.environ, {"HOUSEHOLD_ID": "household-001"}), patch.object(query, "connect_database", return_value=connection), patch.object(query, "fetch_rows", return_value=[run_row()]):
            result = query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["database_user"], "ring_reader")
        self.assertEqual(result["result"]["runs"][0]["started_at"], "2026-09-19T21:00:00+00:00")
        cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()

    def test_database_error_is_not_reported_as_no_runs(self):
        connection = MagicMock()
        connection.cursor.return_value.fetchone.return_value = ("ring_reader",)
        with patch.dict(os.environ, {"HOUSEHOLD_ID": "household-001"}), patch.object(query, "connect_database", return_value=connection), patch.object(query, "fetch_rows", side_effect=RuntimeError("unavailable")):
            with self.assertRaisesRegex(RuntimeError, "Query failed"):
                query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
