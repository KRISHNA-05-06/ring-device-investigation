import copy
import io
import json
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import recovery_lambda as recovery
import aws_mcp_server as server

RAW = {"operation": "recompute_device_health", "as_of": "2026-09-18T12:05:00Z",
       "silence_minutes": 10, "idempotency_key": "health-demo-v1"}
REQUEST = recovery.parse_request(RAW)


def health_body():
    device = {"device_id": "camera-002", "device_type": "camera", "registered_at": "2026-09-01T09:00:00+00:00",
              "event_id": "evt-batch-003", "last_heartbeat": "2026-09-18T12:00:00+00:00", "battery_percent": 75,
              "received_at": "2026-09-19T22:59:20+00:00", "run_id": "replay-run",
              "minutes_since_heartbeat": 5, "health_status": "reporting"}
    return {"status": "ok", "operation": "get_device_health", "household_id": recovery.HOUSEHOLD,
            "database_user": "ring_reader", "generated_at": "2026-09-20T01:00:00+00:00",
            "result": {"as_of": REQUEST["as_of"], "silence_minutes": 10, "devices": [device],
                       "returned_devices": 1, "truncated": False}}


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.race = False
        self.uncertain = False
    def get_object(self, **kw):
        if kw["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[kw["Key"]])}
    def put_object(self, **kw):
        self.puts.append(kw)
        if kw["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        assert kw["IfNoneMatch"] == "*"
        self.objects[kw["Key"]] = kw["Body"]
        if self.race:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        if self.uncertain:
            raise TimeoutError("Response lost after storage")
        return {}


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.client = MagicMock()
        self.body = health_body()
        self.client.invoke.side_effect = lambda **kw: {"Payload": io.BytesIO(json.dumps(self.body).encode())}
    def save(self, request=REQUEST):
        return recovery.recompute_device_health(self.s3, self.client, request, "snapshot-invocation")

    def test_save_and_exact_retry_keep_one_report(self):
        first, second = self.save(), self.save()
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertFalse(second["recomputed_this_request"])
        self.assertEqual(first["snapshot"], second["snapshot"])
        self.assertEqual(first["snapshot_sha256"], second["snapshot_sha256"])
        self.assertEqual(len(self.s3.puts), 1)
        self.assertEqual(self.client.invoke.call_count, 1)
        self.assertFalse(first["database_modified"])
        self.assertFalse(first["replayed"])
        server.validate_snapshot_response(first, RAW)
        server.validate_snapshot_response(second, RAW)

    def test_write_is_fixed_prefix_encrypted_and_conditional(self):
        receipt = self.save()
        write = self.s3.puts[0]
        self.assertEqual(write["Bucket"], recovery.BUCKET)
        self.assertEqual(write["Key"], receipt["storage"]["key"])
        self.assertTrue(write["Key"].startswith("reports/device-health/household-001/"))
        self.assertEqual(write["ServerSideEncryption"], "AES256")
        self.assertEqual(write["IfNoneMatch"], "*")
        self.assertEqual(write["ContentType"], "application/json")
        payload = json.loads(self.client.invoke.call_args.kwargs["Payload"])
        self.assertEqual(payload, {"operation": "get_device_health", "as_of": REQUEST["as_of"], "silence_minutes": 10})

    def test_same_key_changed_parameters_conflict_before_query(self):
        first = self.save()
        with self.assertRaises(recovery.RequestError) as error:
            self.save({**REQUEST, "silence_minutes": 20})
        self.assertEqual(error.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(self.client.invoke.call_count, 1)
        self.assertEqual(self.save()["snapshot_sha256"], first["snapshot_sha256"])

    def test_equivalent_timezone_normalizes_and_new_key_creates_new_report(self):
        self.save()
        equivalent = recovery.parse_request({**RAW, "as_of": "2026-09-18T08:05:00-04:00"})
        self.assertFalse(self.save(equivalent)["created"])
        self.assertTrue(self.save({**REQUEST, "idempotency_key": "health-demo-v2"})["created"])
        self.assertEqual(len(self.s3.objects), 2)

    def test_invalid_requests_rejected_before_aws(self):
        invalid = [{**RAW, k: v} for k, v in [("as_of", "2026-09-18"), ("as_of", None), ("silence_minutes", True),
                   ("silence_minutes", 0), ("silence_minutes", 1441), ("idempotency_key", "../raw"),
                   ("idempotency_key", "x"*65), ("household_id", "household-002"), ("approved", True), ("bucket", "other")]]
        with patch.object(recovery.boto3, "client") as client:
            for request in invalid:
                self.assertEqual(recovery.lambda_handler(request, SimpleNamespace(aws_request_id="test"))["error_code"], "INVALID_REQUEST")
            client.assert_not_called()

    def test_partial_health_list_never_saved(self):
        self.body["result"]["truncated"] = True
        with self.assertRaises(recovery.RequestError) as error:
            self.save()
        self.assertEqual(error.exception.code, "HEALTH_RESULT_TRUNCATED")
        self.assertEqual(self.s3.puts, [])

    def test_wrong_household_and_query_errors_never_saved(self):
        for key, value in [("household_id", "household-002"), ("status", "error"), ("database_user", "ring_admin")]:
            self.body = {**health_body(), key: value}
            with self.assertRaises(RuntimeError):
                self.save()
        self.assertEqual(self.s3.puts, [])

    def test_invalid_health_evidence_never_saved(self):
        for key, value in [("battery_percent", 150), ("health_status", "unknown"), ("minutes_since_heartbeat", 99),
                           ("last_heartbeat", "2026-09-18T13:00:00+00:00")]:
            self.body = health_body()
            self.body["result"]["devices"][0][key] = value
            with self.assertRaises(RuntimeError):
                self.save()
        self.assertEqual(self.s3.puts, [])

    def test_unknown_and_silence_threshold(self):
        health = self.body["result"]
        device = health["devices"][0]
        for field in ("event_id", "last_heartbeat", "battery_percent", "received_at", "run_id", "minutes_since_heartbeat"):
            device[field] = None
        device["health_status"] = "unknown"
        self.assertEqual(self.save()["snapshot"]["summary"]["unknown"], 1)
        for cutoff, minutes, status in [("2026-09-18T12:10:00+00:00", 10, "reporting"), ("2026-09-18T12:11:00+00:00", 11, "suspected_offline")]:
            self.body = health_body()
            self.body["result"]["as_of"] = cutoff
            self.body["result"]["devices"][0].update(minutes_since_heartbeat=minutes, health_status=status)
            result = self.save({**REQUEST, "as_of": cutoff, "idempotency_key": "cutoff-"+str(minutes)})
            self.assertEqual(result["snapshot"]["summary"][status], 1)

    def test_concurrent_winner_returned_without_overwrite(self):
        self.s3.race = True
        result = self.save()
        self.assertFalse(result["created"])
        self.assertTrue(result["recomputed_this_request"])
        self.assertEqual(len(self.s3.objects), 1)

    def test_uncertain_write_retry_reads_existing_without_recomputation(self):
        self.s3.uncertain = True
        with self.assertRaises(TimeoutError):
            self.save()
        result = self.save()
        self.assertFalse(result["created"])
        self.assertEqual(self.client.invoke.call_count, 1)

    def test_access_denied_is_not_treated_as_absent(self):
        with patch.object(self.s3, "get_object", side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")):
            with self.assertRaises(ClientError):
                self.save()
        self.client.invoke.assert_not_called()
        self.assertEqual(self.s3.puts, [])

    def test_corrupt_saved_report_is_not_overwritten(self):
        result = self.save()
        key = result["storage"]["key"]
        document = json.loads(self.s3.objects[key])
        document["snapshot"]["summary"]["reporting"] = 50
        self.s3.objects[key] = json.dumps(document).encode()
        with self.assertRaises(RuntimeError):
            self.save()
        self.assertEqual(len(self.s3.puts), 1)

    def test_mcp_checks_receipt_and_calls_recovery(self):
        result = self.save()
        client = MagicMock()
        client.invoke.return_value = {"Payload": io.BytesIO(json.dumps({"status": "ok", "operation": RAW["operation"],
            "household_id": recovery.HOUSEHOLD, "result": result}).encode())}
        with patch.object(server.boto3, "Session") as session:
            session.return_value.client.return_value = client
            received = server.recompute_device_health(RAW["as_of"], RAW["idempotency_key"])
        self.assertEqual(received["result"], result)
        self.assertEqual(client.invoke.call_args.kwargs["FunctionName"], server.RECOVERY_FUNCTION)
        for field, value in [("database_modified", True), ("snapshot_sha256", "bad"), ("storage", {})]:
            with self.assertRaises(RuntimeError):
                server.validate_snapshot_response({**result, field: value}, RAW)


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_sixteen_tools_and_snapshot_write_annotation(self):
        params = StdioServerParameters(command=sys.executable, args=[str(Path(__file__).resolve().parents[3] / "aws_mcp_server.py")])
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=20)) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                self.assertEqual(len(tools), 17)
                tool = next(t for t in tools if t.name == "recompute_device_health")
                self.assertFalse(tool.annotations.readOnlyHint)
                self.assertTrue(tool.annotations.idempotentHint)
                self.assertEqual(set(tool.inputSchema["required"]), {"as_of", "idempotency_key"})
                self.assertIn("recompute_device_health", (await session.read_resource("ring://definitions")).contents[0].text)
                self.assertIn("recompute_device_health", (await session.get_prompt("investigate_devices")).messages[0].content.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
