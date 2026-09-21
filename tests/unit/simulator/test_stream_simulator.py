import contextlib
from datetime import datetime, timezone
import io
import json
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import stream_simulator as simulator

NOW = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)


class SimulatorTests(unittest.TestCase):
    def events(self, tick=0, scenario="normal"):
        return simulator.make_events("a" * 32, tick, NOW, scenario)

    def test_contract_and_identity(self):
        events = self.events()
        self.assertEqual(len(events), 3)
        for event in events:
            self.assertEqual(set(event), {"event_id", "device_id", "household_id", "event_time", "battery_percent"})
            self.assertEqual(event["household_id"], "household-001")
            self.assertEqual(json.loads(simulator.wire_record(event)["Data"]), event)
            self.assertEqual(simulator.wire_record(event)["PartitionKey"], "household-001#" + event["device_id"])
        self.assertNotEqual(events[0]["event_id"], self.events(1)[0]["event_id"])

    def test_scenarios(self):
        self.assertEqual(self.events(scenario="duplicate")[0], self.events(scenario="duplicate")[-1])
        self.assertEqual(len(self.events(1, "duplicate")), 3)
        self.assertEqual(self.events(scenario="invalid-battery")[1]["battery_percent"], 150)
        self.assertEqual(self.events(1, "invalid-battery")[1]["battery_percent"], 75)
        self.assertEqual(len(self.events(scenario="silence")), 3)
        self.assertEqual([e["device_id"] for e in self.events(1, "silence")], ["camera-001", "doorbell-001"])

    def test_retries_only_failed_entry_with_identical_payload(self):
        ok = {"ShardId": "shard-0", "SequenceNumber": "123"}
        client = Mock()
        client.put_records.side_effect = [
            {"Records": [ok, {"ErrorCode": "ProvisionedThroughputExceededException"}, ok]},
            {"Records": [ok]},
        ]
        emit = Mock()
        simulator.send_batch(client, self.events(), emit, sleep=lambda _: None)
        calls = client.put_records.call_args_list
        self.assertEqual(calls[1].kwargs["Records"], [calls[0].kwargs["Records"][1]])
        self.assertEqual(sum(c.args[0]["status"] == "sent_to_kinesis" for c in emit.call_args_list), 3)

    def test_five_attempt_bound(self):
        client = Mock()
        client.put_records.return_value = {"Records": [{"ErrorCode": "InternalFailure"}]}
        with self.assertRaisesRegex(RuntimeError, "Five attempts"):
            simulator.send_batch(client, self.events()[:1], Mock(), sleep=lambda _: None)
        self.assertEqual(client.put_records.call_count, 5)

    def test_uncertain_whole_call_stops_without_automatic_resubmission(self):
        client = Mock()
        client.put_records.side_effect = TimeoutError()
        emit = Mock()
        with self.assertRaisesRegex(RuntimeError, "Delivery uncertain"):
            simulator.send_batch(client, self.events(), emit)
        self.assertEqual(client.put_records.call_count, 1)
        self.assertEqual(emit.call_args.args[0]["status"], "delivery_unconfirmed")

    def test_malformed_response_is_not_success(self):
        client = Mock()
        client.put_records.return_value = {"Records": []}
        with self.assertRaisesRegex(RuntimeError, "Unexpected"):
            simulator.send_batch(client, self.events(), Mock())

    def test_access_denied_is_definite_denial_and_not_retried(self):
        from botocore.exceptions import ClientError
        for code in ("AccessDeniedException", "AccessDenied", "KMSAccessDeniedException"):
            with self.subTest(code=code):
                client = Mock()
                client.put_records.side_effect = ClientError(
                    {"Error": {"Code": code, "Message": "Test permission denial"}}, "PutRecords")
                emit = Mock()
                with self.assertRaisesRegex(RuntimeError, "AWS denied this batch"):
                    simulator.send_batch(client, self.events(), emit)
                self.assertEqual(client.put_records.call_count, 1)
                self.assertEqual(emit.call_args.args[0]["status"], "send_denied")
                self.assertEqual(emit.call_args.args[0]["message"], "Test permission denial")

    def test_preview_without_aws_dependencies(self):
        with patch.object(sys, "argv", ["simulator"]), patch.dict(sys.modules, {"boto3": None}), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(simulator.main(), 0)
        self.assertIn('"maximum_planned_records": 18', output.getvalue())

    def test_invalid_run_bounds(self):
        for flags in [["--duration-seconds", "901"], ["--interval-seconds", "0"], ["--duration-seconds", "10"]]:
            with self.subTest(flags=flags), patch.object(sys, "argv", ["simulator", *flags]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    simulator.main()

    def test_bounded_send_loop_writes_evidence_and_stops(self):
        clock = [0.0]
        def advance(seconds):
            clock[0] += seconds
        sts = Mock()
        sts.get_caller_identity.return_value = {"Account": simulator.ACCOUNT, "Arn": "test-actor"}
        kinesis = Mock()
        kinesis.put_records.return_value = {"Records": [{"ShardId": "shard", "SequenceNumber": "1"}] * 3}
        session = Mock()
        session.client.side_effect = [sts, kinesis]
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(sys, "argv", ["simulator", "--send"]), \
                 patch("boto3.Session", return_value=session), \
                 patch.object(simulator, "Path", return_value=Path(directory) / "simulator.py"), \
                 patch.object(simulator.time, "monotonic", side_effect=lambda: clock[0]), \
                 patch.object(simulator.time, "sleep", side_effect=advance), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(simulator.main(), 0)
            logs = list((Path(directory) / "sessions").glob("*.jsonl"))
            self.assertEqual(len(logs), 1)
            entries = [json.loads(line) for line in logs[0].read_text().splitlines()]
            self.assertEqual(sum(e["status"] == "batch_prepared" for e in entries), 6)
            self.assertEqual(sum(e["status"] == "sent_to_kinesis" for e in entries), 18)
            self.assertEqual(entries[-1]["status"], "generation_finished")
        self.assertEqual(clock[0], 180)
        self.assertEqual(kinesis.put_records.call_count, 6)
        kinesis.close.assert_called_once()
        sts.close.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
