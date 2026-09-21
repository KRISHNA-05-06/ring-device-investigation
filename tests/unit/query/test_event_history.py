import copy
import io
import json
import os
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from test_device_details import AS_OF, row, wire, mcp, query

START = AS_OF - timedelta(minutes=5)
END = AS_OF
REQUEST = {"operation": "get_event_history", "device_id": "camera-002",
           "start_time": START.isoformat(), "end_time": END.isoformat(), "limit": 20}
DEVICE = {k: row()[k] for k in ("device_id", "device_type", "registered_at")}


def event(event_id="evt-batch-003", time=START):
    result = row()
    for key in DEVICE:
        result.pop(key)
    return {**result, "event_id": event_id, "event_time": time}


def observe(events, limit=20, device=True):
    with patch.object(query, "fetch_rows", side_effect=[[DEVICE], events] if device else [[]]):
        return query.get_event_history(object(), "household-001", "camera-002", START, END, limit)


class EventHistoryTests(unittest.TestCase):
    def test_rejects_invalid_windows_and_inputs_before_database(self):
        cases = [
            {**REQUEST, "start_time": None}, {**REQUEST, "end_time": "2026-09-18"},
            {**REQUEST, "start_time": END.isoformat()},
            {**REQUEST, "start_time": (END + timedelta(seconds=1)).isoformat()},
            {**REQUEST, "start_time": (END - timedelta(days=31, seconds=1)).isoformat()},
            {**REQUEST, "household_id": "household-002"}, {**REQUEST, "as_of": END.isoformat()},
            {**REQUEST, "device_id": ""}, {**REQUEST, "device_id": ["camera-002"]},
        ] + [{**REQUEST, "limit": limit} for limit in (0, 101, True, 1.5, "20")]
        with patch.object(query, "connect_database") as connect:
            for case in cases:
                with self.subTest(case=case):
                    self.assertEqual(query.lambda_handler(case, None)["error_code"], "INVALID_REQUEST")
            connect.assert_not_called()

    def test_normalizes_offsets_and_accepts_max_window(self):
        start, end = query.history_window({"start_time": "2026-09-01T05:00:00+05:00",
                                           "end_time": "2026-10-02T00:00:00Z"})
        self.assertEqual((end - start).days, 31)
        self.assertEqual(start.hour, 0)
        query.parse_request({**REQUEST, "limit": 100})

    def test_empty_history_differs_from_missing_registration(self):
        registered, missing = observe([]), observe([], device=False)
        self.assertTrue(registered["device_found"])
        self.assertFalse(missing["device_found"])
        for result in (registered, missing):
            self.assertEqual(result["records"], [])
            self.assertFalse(result["truncated"])
            mcp.validate_event_history(wire(result), REQUEST)

    def test_limit_plus_one_marks_truncation_without_returning_extra(self):
        result = observe([event("new", START + timedelta(minutes=1)), event("old", START)], limit=1)
        self.assertEqual([r["event_id"] for r in result["records"]], ["new"])
        self.assertTrue(result["truncated"])
        self.assertEqual(result["returned_records"], 1)
        mcp.validate_event_history(wire(result), {**REQUEST, "limit": 1})
        self.assertFalse(observe([event()], limit=1)["truncated"])

    def test_replayed_event_keeps_later_received_time_and_source(self):
        result = observe([event()])
        self.assertGreater(result["records"][0]["received_at"], END)
        self.assertEqual(result["records"][0]["battery_percent"], 75)
        mcp.validate_event_history(wire(result), REQUEST)

    def test_sql_filters_household_device_window_and_ties(self):
        device = "x' OR TRUE --"
        with patch.object(query, "fetch_rows", side_effect=[[DEVICE], []]) as fetch:
            query.get_event_history(object(), "household-001", device, START, END, 20)
        registration, events = fetch.call_args_list
        self.assertEqual(registration.args[2], ("household-001", device))
        sql, params = events.args[1:]
        self.assertEqual(params, ("household-001", device, START, END, 21))
        self.assertNotIn(device, sql)
        for fragment in ("e.household_id = %s AND e.device_id = %s", "r.household_id = e.household_id",
                         "e.event_time >= %s AND e.event_time < %s",
                         "ORDER BY e.event_time DESC, e.event_id DESC", "LIMIT %s"):
            self.assertIn(fragment, sql)

    def test_missing_device_does_not_query_other_events(self):
        with patch.object(query, "fetch_rows", return_value=[]) as fetch:
            result = query.get_event_history(object(), "household-001", "missing", START, END, 20)
        self.assertFalse(result["device_found"])
        self.assertEqual(fetch.call_count, 1)

    def test_handler_uses_read_only_snapshot_and_propagates_failure(self):
        connection = MagicMock()
        connection.cursor.return_value.fetchone.return_value = ("ring_reader",)
        with patch.dict(os.environ, HOUSEHOLD_ID="household-001"), \
             patch.object(query, "connect_database", return_value=connection), \
             patch.object(query, "fetch_rows", side_effect=[[DEVICE], [event()]]):
            result = query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))
        self.assertEqual(result["operation"], "get_event_history")
        self.assertEqual(result["result"]["returned_records"], 1)
        connection.cursor.return_value.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        with patch.dict(os.environ, HOUSEHOLD_ID="household-001"), \
             patch.object(query, "connect_database", side_effect=RuntimeError("unavailable")):
            with self.assertRaises(RuntimeError):
                query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))

    def test_mcp_wrapper_passes_exact_window_and_limit(self):
        body = dict(status="ok", operation="get_event_history", household_id="household-001",
                    database_user="ring_reader", result=wire(observe([event()])))
        client = MagicMock()
        client.invoke.return_value = {"Payload": io.BytesIO(json.dumps(body).encode())}
        with patch.object(mcp.boto3, "Session") as session:
            session.return_value.client.return_value = client
            result = mcp.get_event_history("camera-002", START.isoformat(), END.isoformat())
        self.assertEqual(result["result"], body["result"])
        self.assertEqual(json.loads(client.invoke.call_args.kwargs["Payload"]), REQUEST)
        self.assertEqual(client.invoke.call_args.kwargs["FunctionName"], mcp.QUERY_FUNCTION)

    def test_response_rejects_outside_window_duplicates_and_bad_lineage(self):
        good = wire(observe([event()]))
        for key, value in [("event_time", END.isoformat()), ("event_time", (START-timedelta(seconds=1)).isoformat()),
                           ("source_bucket", "another-household-bucket"), ("run_status", "failed"), ("battery_percent", True)]:
            bad = copy.deepcopy(good)
            bad["records"][0][key] = value
            with self.assertRaises(RuntimeError):
                mcp.validate_event_history(bad, REQUEST)
        duplicate = wire(observe([event(), event()]))
        with self.assertRaises(RuntimeError):
            mcp.validate_event_history(duplicate, REQUEST)
        reversed_order = wire(observe([event("old"), event("new", START+timedelta(minutes=1))]))
        with self.assertRaises(RuntimeError):
            mcp.validate_event_history(reversed_order, REQUEST)

    def test_response_cannot_hide_truncation_or_change_request(self):
        good = wire(observe([event()]))
        for key, value in [("device_id", "other"), ("start_time", (START-timedelta(minutes=1)).isoformat()),
                           ("returned_records", 99), ("truncated", True), ("device_found", False)]:
            bad = {**good, key: value}
            with self.assertRaises(RuntimeError):
                mcp.validate_event_history(bad, REQUEST)


if __name__ == "__main__":
    unittest.main(verbosity=2)
