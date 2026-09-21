import asyncio
import copy
import io
import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from test_replay_ingestion_status import query
import aws_mcp_server as mcp
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

AS_OF = datetime(2026, 9, 18, 12, 5, tzinfo=timezone.utc)
REQUEST = {"operation": "get_device_details", "device_id": "camera-002", "as_of": AS_OF.isoformat()}


def row():
    return dict(device_id="camera-002", device_type="camera",
                registered_at=datetime(2026, 9, 1, 9, tzinfo=timezone.utc),
                event_id="evt-batch-003", event_time=AS_OF - timedelta(minutes=5),
                battery_percent=75, received_at=AS_OF + timedelta(days=1),
                run_id="replay-run", line_number=1, digest="a" * 64,
                source_bucket=mcp.BUCKET, source_key="raw/replays/household-001/example.jsonl",
                run_status="completed")


def observe(rows, cutoff=AS_OF):
    with patch.object(query, "fetch_rows", return_value=rows):
        return query.get_device_details(object(), "household-001", "camera-002", cutoff, 10)


def wire(result):
    return json.loads(json.dumps(result, default=query.serialize_value))


class DetailsTests(unittest.TestCase):
    def test_invalid_requests_fail_before_database(self):
        cases = [{**REQUEST, "device_id": v} for v in (None, True, "", " x", "x ", "x"*129, "x\x00", "\ud800")]
        cases += [{**REQUEST, "silence_minutes": v} for v in (True, 0, 1441, 1.5)]
        cases += [{**REQUEST, "household_id": "household-002"}, {**REQUEST, "as_of": "2026-09-18"}]
        with patch.object(query, "connect_database") as connect:
            for event in cases:
                with self.subTest(event=event):
                    self.assertEqual(query.lambda_handler(event, None)["error_code"], "INVALID_REQUEST")
            connect.assert_not_called()

    def test_missing_is_distinct_from_no_heartbeat(self):
        missing = observe([])
        self.assertFalse(missing["found"])
        self.assertIsNone(missing["health"])
        registered = row()
        registered["event_id"] = None
        result = observe([registered])
        self.assertTrue(result["found"])
        self.assertEqual(result["health"], {"status": "unknown", "minutes_since_heartbeat": None})
        self.assertIsNone(result["latest_heartbeat"])
        for value in (missing, result):
            mcp.validate_device_details(wire(value), REQUEST)

    def test_late_replay_is_included_at_historical_event_cutoff(self):
        result = observe([row()])
        self.assertEqual(result["latest_heartbeat"]["battery_percent"], 75)
        self.assertEqual(result["health"], {"status": "reporting", "minutes_since_heartbeat": 5})
        self.assertGreater(result["latest_heartbeat"]["received_at"], AS_OF)
        mcp.validate_device_details(wire(result), REQUEST)

    def test_exact_threshold_and_just_beyond(self):
        for minutes, seconds, expected in [(10, 0, "reporting"), (10, 1, "suspected_offline")]:
            cutoff = row()["event_time"] + timedelta(minutes=minutes, seconds=seconds)
            result = observe([row()], cutoff)
            self.assertEqual(result["health"]["status"], expected)
            mcp.validate_device_details(wire(result), {**REQUEST, "as_of": cutoff.isoformat()})

    def test_sql_scopes_both_joins_and_parameterizes_device(self):
        cursor = MagicMock()
        cursor.description = [("device_id",)]
        cursor.fetchall.return_value = []
        device = "x' OR TRUE --"
        query.parse_request({**REQUEST, "device_id": device})
        query.get_device_details(cursor, "household-001", device, AS_OF, 10)
        sql, parameters = cursor.execute.call_args.args
        self.assertNotIn(device, sql)
        self.assertEqual(parameters, (AS_OF, "household-001", device, AS_OF))
        for fragment in ("household_id = d.household_id", "device_id = d.device_id",
                         "r.household_id = d.household_id", "d.household_id = %s",
                         "d.registered_at <= %s", "event_time <= %s",
                         "ORDER BY event_time DESC, event_id DESC", "LIMIT 1"):
            self.assertIn(fragment, sql)

    def test_handler_read_only_transaction_and_serialization(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = ("ring_reader",)
        with patch.dict(os.environ, HOUSEHOLD_ID="household-001"), \
             patch.object(query, "connect_database", return_value=connection), \
             patch.object(query, "fetch_rows", return_value=[row()]):
            response = query.lambda_handler(REQUEST, SimpleNamespace(aws_request_id="test"))
        self.assertEqual(response["database_user"], "ring_reader")
        self.assertEqual(response["result"]["as_of"], AS_OF.isoformat())
        cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()

    def test_wrapper_invokes_query_and_checks_response(self):
        result = wire(observe([row()]))
        body = dict(status="ok", operation="get_device_details", household_id="household-001",
                    database_user="ring_reader", result=result)
        client = MagicMock()
        client.invoke.return_value = {"Payload": io.BytesIO(json.dumps(body).encode())}
        with patch.object(mcp.boto3, "Session") as session:
            session.return_value.client.return_value = client
            received = mcp.get_device_details("camera-002", AS_OF.isoformat())
        self.assertEqual(received["result"], result)
        invocation = client.invoke.call_args.kwargs
        self.assertEqual(invocation["FunctionName"], mcp.QUERY_FUNCTION)
        self.assertEqual(json.loads(invocation["Payload"]), {**REQUEST, "silence_minutes": 10})
        client.close.assert_called_once()

    def test_response_cannot_change_request_or_invent_lineage(self):
        good = wire(observe([row()]))
        for field, value in [("device_id", "camera-001"), ("as_of", "2026-09-18T12:06:00Z"),
                             ("silence_minutes", 30), ("found", False)]:
            bad = copy.deepcopy(good)
            bad[field] = value
            with self.assertRaises(RuntimeError):
                mcp.validate_device_details(bad, REQUEST)
        for field, value in [("source_bucket", "another-bucket"), ("run_status", "failed"),
                             ("battery_percent", 150), ("event_time", "2026-09-18T13:00:00Z")]:
            bad = copy.deepcopy(good)
            bad["latest_heartbeat"][field] = value
            with self.assertRaises(RuntimeError):
                mcp.validate_device_details(bad, REQUEST)


class LocalProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_and_guidance_without_aws_access(self):
        parameters = StdioServerParameters(command=sys.executable,
            args=[str(Path(__file__).resolve().parents[3] / "aws_mcp_server.py")])
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=20)) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                self.assertEqual(len(tools), 17)
                quality = next(t for t in tools if t.name == "get_quality_results")
                self.assertEqual(set(quality.inputSchema["required"]), {"start_time", "end_time"})
                self.assertTrue(quality.annotations.readOnlyHint)
                details = next(t for t in tools if t.name == "get_device_details")
                self.assertEqual(details.inputSchema["required"], ["device_id"])
                self.assertTrue(details.annotations.readOnlyHint)
                history = next(t for t in tools if t.name == "get_event_history")
                self.assertEqual(set(history.inputSchema["required"]), {"device_id", "start_time", "end_time"})
                self.assertTrue(history.annotations.readOnlyHint)
                resource = await session.read_resource("ring://definitions")
                self.assertIn("get_device_details", resource.contents[0].text)
                self.assertIn("get_event_history", resource.contents[0].text)
                self.assertIn("get_quality_results", resource.contents[0].text)
                prompt = await session.get_prompt("investigate_devices")
                self.assertIn("get_device_details", prompt.messages[0].content.text)
                self.assertIn("get_event_history", prompt.messages[0].content.text)
                self.assertIn("get_quality_results", prompt.messages[0].content.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
