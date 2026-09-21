import copy
import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from test_health_snapshot import MemoryS3
import recovery_lambda as recovery

RAW = {"operation": "save_quality_report", "start_time": "2026-09-18T00:00:00Z",
       "end_time": "2026-09-20T00:00:00Z", "run_limit": 100, "idempotency_key": "quality-demo-v1"}
REQUEST = recovery.parse_request(RAW)


def quality_body():
    return {"status": "ok", "operation": "get_quality_results", "household_id": recovery.HOUSEHOLD,
            "database_user": "ring_reader", "generated_at": "2026-09-20T02:00:00+00:00",
            "result": {
                "scope": "consistency_of_selected_completed_ingestion_runs",
                "start_time": REQUEST["start_time"], "end_time": REQUEST["end_time"],
                "time_basis": "run_started_at", "window_bounds": "start_inclusive_end_exclusive",
                "run_limit": 100, "selected_runs": 1, "run_states": {"completed": 1, "running": 0, "failed": 0},
                "truncated": False, "overall_status": "pass",
                "checks": [{"check_id": name, "description": name, "status": "pass", "examined_runs": 1,
                            "affected_runs": 0, "examples": [], "examples_truncated": False}
                           for name in ("run_count_balance", "accepted_row_count", "rejected_row_count", "accepted_event_validity")],
                "completed_run_totals": {"input_count": 2, "accepted_count": 1, "duplicate_count": 0,
                                         "rejected_count": 1, "stored_accepted_count": 1, "stored_rejected_count": 1},
                "operational_attention_required": False, "persisted": False, "data_modified": False,
                "limitations": ["Selected completed runs only; not proof of device health"],
            }}


class QualityReportTests(unittest.TestCase):
    def setUp(self):
        self.s3, self.client = MemoryS3(), MagicMock()
        self.body = quality_body()
        self.client.invoke.side_effect = lambda **kw: {"Payload": io.BytesIO(json.dumps(self.body).encode())}

    def save(self, request=REQUEST):
        return recovery.save_quality_report(self.s3, self.client, request, "test-invocation")

    def test_creation_and_retry_keep_original_despite_new_query_data(self):
        first = self.save()
        self.body["generated_at"] = "2026-09-21T00:00:00+00:00"
        second = self.save()
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertFalse(second["queried_this_request"])
        self.assertEqual(first["report"], second["report"])
        self.assertEqual(first["report_sha256"], second["report_sha256"])
        self.assertEqual(len(self.s3.puts), 1)
        self.assertEqual(self.client.invoke.call_count, 1)

    def test_storage_scope_conditional_encryption_and_query_routing(self):
        receipt = self.save()
        write = self.s3.puts[0]
        self.assertTrue(write["Key"].startswith("reports/data-quality/household-001/"))
        self.assertEqual(write["Bucket"], recovery.BUCKET)
        self.assertEqual(write["IfNoneMatch"], "*")
        self.assertEqual(write["ServerSideEncryption"], "AES256")
        self.assertFalse(receipt["database_modified"])
        self.assertFalse(receipt["replayed"])
        self.assertEqual(json.loads(self.client.invoke.call_args.kwargs["Payload"]),
                         {"operation": "get_quality_results", **{k: REQUEST[k] for k in ("start_time", "end_time", "run_limit")}})

    def test_changed_request_conflicts_before_query(self):
        self.save()
        with self.assertRaises(recovery.RequestError) as caught:
            self.save({**REQUEST, "run_limit": 1})
        self.assertEqual(caught.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(self.client.invoke.call_count, 1)

    def test_equivalent_timezones_normalize(self):
        normalized = recovery.parse_request({**RAW, "start_time": "2026-09-17T20:00:00-04:00"})
        self.assertEqual(normalized, REQUEST)

    def test_bad_inputs_do_not_create_aws_clients(self):
        for change in ({"start_time": "2026-09-18"}, {"end_time": RAW["start_time"]},
                       {"end_time": "2026-12-01T00:00:00Z"}, {"run_limit": True},
                       {"run_limit": 201}, {"idempotency_key": "../bad"}, {"household_id": "other"}):
            with self.subTest(change=change), patch.object(recovery.boto3, "client") as factory:
                result = recovery.lambda_handler({**RAW, **change}, SimpleNamespace(aws_request_id="test"))
                self.assertEqual(result["error_code"], "INVALID_REQUEST")
                factory.assert_not_called()

    def test_truncated_report_preserves_incomplete(self):
        self.body["result"].update(run_limit=1, truncated=True, overall_status="incomplete")
        receipt = self.save({**REQUEST, "run_limit": 1})
        self.assertEqual(receipt["report_status"], "saved")
        self.assertEqual(receipt["report"]["quality"]["overall_status"], "incomplete")

    def test_failed_check_is_saved_as_fail(self):
        quality = self.body["result"]
        quality["checks"][1].update(status="fail", affected_runs=1,
            examples=[{"run_id": "r", "source_key": "raw/test", "expected": 1, "observed": 0}])
        quality["overall_status"] = "fail"
        quality["completed_run_totals"]["stored_accepted_count"] = 0
        self.assertEqual(self.save()["report"]["quality"]["overall_status"], "fail")

    def test_no_completed_runs_remains_not_evaluated(self):
        quality = self.body["result"]
        quality.update(selected_runs=0, run_states={"completed": 0, "running": 0, "failed": 0}, overall_status="not_evaluated")
        for check in quality["checks"]:
            check.update(status="not_evaluated", examined_runs=0)
        quality["completed_run_totals"] = dict.fromkeys(quality["completed_run_totals"], 0)
        self.assertEqual(self.save()["report"]["quality"]["overall_status"], "not_evaluated")

    def test_wrong_identity_or_inconsistent_verdict_never_saved(self):
        for area, key, value in ((None, "household_id", "household-002"), (None, "database_user", "ring_admin"),
                                 ("result", "overall_status", "incomplete")):
            self.body = quality_body()
            target = self.body if area is None else self.body[area]
            target[key] = value
            with self.assertRaises(RuntimeError):
                self.save()
        self.assertEqual(self.s3.puts, [])

    def test_denied_read_is_not_absence(self):
        self.s3.get_object = MagicMock(side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject"))
        with self.assertRaises(ClientError):
            self.save()
        self.client.invoke.assert_not_called()

    def test_existing_report_corruption_rejected(self):
        receipt = self.save()
        key = receipt["storage"]["key"]
        document = json.loads(self.s3.objects[key])
        document["report"]["quality"]["overall_status"] = "fail"
        self.s3.objects[key] = json.dumps(document).encode()
        with self.assertRaises(RuntimeError):
            self.save()

    def test_conditional_race_returns_winner(self):
        self.s3.race = True
        receipt = self.save()
        self.assertFalse(receipt["created"])
        self.assertTrue(receipt["queried_this_request"])

    def test_lost_write_response_recovers_on_same_key_retry(self):
        self.s3.uncertain = True
        with self.assertRaises(TimeoutError):
            self.save()
        self.s3.uncertain = False
        receipt = self.save()
        self.assertFalse(receipt["created"])
        self.assertFalse(receipt["queried_this_request"])
        self.assertEqual(len(self.s3.puts), 1)

    def test_handler_routes_operation_and_closes_clients(self):
        s3, client = MagicMock(), MagicMock()
        with patch.object(recovery.boto3, "client", side_effect=[s3, client]), \
             patch.object(recovery, "save_quality_report", return_value={"report": {"report_id": "a" * 64}, "created": True}) as save:
            result = recovery.lambda_handler(RAW, SimpleNamespace(aws_request_id="test"))
        self.assertEqual(result["operation"], "save_quality_report")
        save.assert_called_once_with(s3, client, REQUEST, "test")
        s3.close.assert_called_once()
        client.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
