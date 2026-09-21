import ast
import hashlib
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
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


SOURCE = (Path(__file__).parent / "validator_snapshot.py").read_text(encoding="utf-8")
NAMES = {"RecordError", "reject_json_constant", "unique_json_fields", "validate_event", "store_event"}
NODES = [node for node in ast.parse(SOURCE).body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in NAMES]
ORIGINAL = {
    "hashlib": hashlib, "json": json, "datetime": datetime,
    "timezone": timezone, "HOUSEHOLD_ID": "household-001",
}
exec(compile(ast.Module(body=NODES, type_ignores=[]), "validator_rules", "exec"), ORIGINAL)
REGISTERED = datetime(2026, 9, 1, 9, tzinfo=timezone.utc)
EVENT = {
    "event_id": "evt-batch-003", "household_id": "household-001",
    "device_id": "camera-002", "event_time": "2026-09-18T12:00:00Z",
    "battery_percent": 75,
}


class SnapshotCursor:
    def __init__(self, registered=REGISTERED, existing=None, allow_insert=False):
        self.registered = registered
        self.existing = existing
        self.allow_insert = allow_insert
        self.statements = []
        self.row = None

    def execute(self, sql, parameters):
        sql = " ".join(sql.split())
        self.statements.append((sql, parameters))
        if sql.startswith("SELECT registered_at"):
            self.row = None if self.registered is None else (self.registered,)
        elif sql.startswith("SELECT digest"):
            self.row = None if self.existing is None else (self.existing,)
        elif sql.startswith("INSERT") and self.allow_insert:
            self.row = ("inserted",) if self.existing is None else None
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.row


def validation_result(function, raw):
    try:
        return ("valid", function(raw))
    except ValueError as error:
        return ("invalid", error.code, str(error))


class ReplayPreviewTests(unittest.TestCase):
    def test_validation_matches_saved_validator(self):
        cases = [json.dumps(EVENT), "not json", "[]", "{}", "null"]
        changes = [
            {"battery_percent": value} for value in (0, 100, -1, 101, True, 75.0, "75", None)
        ] + [
            {"household_id": "household-002"}, {"event_id": " leading"},
            {"event_id": "trailing "}, {"event_id": "x" * 129},
            {"event_id": "\u0000"}, {"event_id": "\ud800"},
            {"event_id": "camera-\u00e9"}, {"extra": "unexpected"},
            {"event_time": "2026-09-18T08:00:00-04:00"},
            {"event_time": "2026-09-18T12:00:00"}, {"event_time": "bad"},
            {"event_time": None},
        ]
        cases += [json.dumps({**EVENT, **change}) for change in changes]
        cases += [json.dumps({key: value for key, value in EVENT.items() if key != field}) for field in EVENT]
        cases += [
            '{"event_id":"one","event_id":"two"}',
            json.dumps(EVENT).replace('75}', 'NaN}'),
        ]
        for raw in cases:
            with self.subTest(raw=raw):
                expected = validation_result(ORIGINAL["validate_event"], raw)
                actual = validation_result(lambda line: query.validate_replay_event(line, "household-001"), raw)
                self.assertEqual(actual, expected)

    def test_normalized_timezone_produces_identical_digest(self):
        a = query.validate_replay_event(json.dumps(EVENT), "household-001")
        b = query.validate_replay_event(json.dumps({**EVENT, "event_time": "2026-09-18T08:00:00-04:00"}), "household-001")
        self.assertEqual(a[1:], b[1:])

    def test_database_predictions_match_store_event_decisions(self):
        record, event_time, fingerprint = ORIGINAL["validate_event"](json.dumps(EVENT))
        states = [
            (REGISTERED, None), (REGISTERED, fingerprint), (REGISTERED, "0" * 64),
            (None, None), (None, fingerprint),
            (datetime(2026, 9, 20, tzinfo=timezone.utc), fingerprint),
        ]
        for registered, existing in states:
            with self.subTest(registered=registered, existing=existing):
                original_cursor = SnapshotCursor(registered, existing, allow_insert=True)
                try:
                    expected = ORIGINAL["store_event"](original_cursor, record, event_time, fingerprint, "run", 1)
                    expected_code = None
                except ORIGINAL["RecordError"] as error:
                    expected, expected_code = "rejected", error.code
                preview_cursor = SnapshotCursor(registered, existing)
                result = query.preview_replay(preview_cursor, "household-001", EVENT)
                self.assertEqual(result["predicted_outcome"], expected)
                self.assertEqual(result["error_code"], expected_code)
                self.assertFalse(result["data_modified"])
                self.assertFalse(result["replay_performed"])
                self.assertFalse(result["authorization_checked"])
                self.assertTrue(result["requires_revalidation"])
                for sql, parameters in preview_cursor.statements:
                    self.assertTrue(sql.startswith("SELECT"))
                    self.assertEqual(parameters[0], "household-001")

    def test_invalid_candidate_does_not_query_database(self):
        for change, expected in [
            ({"battery_percent": 150}, "INVALID_BATTERY"),
            ({"household_id": "household-002"}, "HOUSEHOLD_MISMATCH"),
        ]:
            cursor = SnapshotCursor()
            result = query.preview_replay(cursor, "household-001", {**EVENT, **change})
            self.assertEqual(result["error_code"], expected)
            self.assertEqual(cursor.statements, [])

    def test_request_boundaries(self):
        query.parse_request({"operation": "preview_replay", "candidate": EVENT})
        invalid = [
            {"operation": "preview_replay"},
            {"operation": "preview_replay", "candidate": []},
            {"operation": "preview_replay", "candidate": {"extra": "x" * 16001}},
            {"operation": "preview_replay", "candidate": EVENT, "approved": True},
            {"operation": "preview_replay", "candidate": EVENT, "household_id": "household-002"},
        ]
        with patch.object(query, "connect_database") as connect:
            for event in invalid:
                self.assertEqual(query.lambda_handler(event, None)["error_code"], "INVALID_REQUEST")
            connect.assert_not_called()

    def test_handler_keeps_read_only_transaction_and_rolls_back(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchone.side_effect = [("ring_reader",), (REGISTERED,), None]
        with patch.dict(query.os.environ, {"HOUSEHOLD_ID": "household-001"}):
            with patch.object(query, "connect_database", return_value=connection):
                result = query.lambda_handler(
                    {"operation": "preview_replay", "candidate": EVENT},
                    SimpleNamespace(aws_request_id="preview-test"),
                )
        self.assertEqual(result["result"]["predicted_outcome"], "accepted")
        statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
        self.assertIn("READ ONLY", statements[0])
        self.assertTrue(all(sql.startswith(("SET", "SELECT")) for sql in statements))
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()

    def test_database_error_does_not_become_acceptance(self):
        cursor = MagicMock()
        cursor.execute.side_effect = TimeoutError("Database unavailable")
        with self.assertRaises(TimeoutError):
            query.preview_replay(cursor, "household-001", EVENT)

    def test_rule_fingerprint_matches_saved_source(self):
        names = {"RecordError", "reject_json_constant", "unique_json_fields", "validate_event"}
        segments = [ast.get_source_segment(SOURCE, node) for node in NODES if node.name in names]
        store = next(ast.get_source_segment(SOURCE, node) for node in NODES if node.name == "store_event")
        fingerprint = hashlib.sha256(("\n\n".join(segments) + "\n\n" + store).encode()).hexdigest()
        self.assertEqual(query.REPLAY_RULES_SHA256, fingerprint)


if __name__ == "__main__":
    unittest.main(verbosity=2)
