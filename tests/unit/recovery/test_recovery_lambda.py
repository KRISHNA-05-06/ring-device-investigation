import copy
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from botocore.exceptions import ClientError

import recovery_lambda as recovery


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **request):
        assert request["IfNoneMatch"] == "*"
        key = request["Key"]
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = request["Body"]
        return {}

    def get_object(self, **request):
        return {"Body": io.BytesIO(self.objects[request["Key"]])}

    def close(self):
        pass


class QueryClient:
    def __init__(self, preview, status="ready", household="household-001"):
        self.preview = preview
        self.status = status
        self.household = household

    def invoke(self, **request):
        payload = json.loads(request["Payload"])
        assert payload["operation"] == "preview_record_correction"
        response = {
            "status": "ok", "operation": "preview_record_correction",
            "household_id": self.household, "database_user": "ring_reader",
            "result": {
                "preview_status": self.status, "preview": self.preview,
                "preview_sha256": recovery.digest(self.preview),
            },
        }
        return {"Payload": io.BytesIO(json.dumps(response).encode())}

    def close(self):
        pass


class ProposalTests(unittest.TestCase):
    def setUp(self):
        self.preview = {
            "household_id": "household-001", "rejection_id": 6,
            "reason": "Explicit synthetic demo value.",
            "proposed_record": {"battery_percent": 75},
        }
        self.request = {
            "operation": "propose_record_correction", "rejection_id": 6,
            "battery_percent": 75, "reason": self.preview["reason"],
            "expected_preview_sha256": recovery.digest(self.preview),
            "idempotency_key": "demo-rejection-6-v1",
        }
        self.s3 = MemoryS3()

    def test_handler_creates_then_returns_identical_proposal(self):
        def client(service, **kwargs):
            return QueryClient(self.preview) if service == "lambda" else self.s3
        with patch.object(recovery.boto3, "client", side_effect=client):
            first = recovery.lambda_handler(self.request, SimpleNamespace(aws_request_id="request-1"))
            second = recovery.lambda_handler(self.request, SimpleNamespace(aws_request_id="request-2"))
        a, b = first["result"], second["result"]
        self.assertTrue(a["created"])
        self.assertFalse(b["created"])
        self.assertEqual(a["proposal"], b["proposal"])
        self.assertEqual(a["proposal_sha256"], b["proposal_sha256"])
        self.assertEqual(len(self.s3.objects), 1)
        self.assertEqual(a["proposal_status"], "pending_approval")
        self.assertFalse(a["applied"])
        self.assertFalse(a["replayed"])

    def test_changed_request_cannot_overwrite_key(self):
        recovery.save_proposal(self.s3, self.request, self.preview, "request-1")
        before = dict(self.s3.objects)
        different = {**self.request, "reason": "Different reason"}
        with self.assertRaises(recovery.RequestError) as caught:
            recovery.save_proposal(self.s3, different, self.preview, "request-2")
        self.assertEqual(caught.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertEqual(self.s3.objects, before)

    def test_expiry_is_not_extended_on_retry(self):
        result = recovery.save_proposal(self.s3, self.request, self.preview, "request-1")
        key = result["storage"]["key"]
        saved = json.loads(self.s3.objects[key])
        saved["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        self.s3.objects[key] = recovery.canonical_bytes(saved)
        retried = recovery.save_proposal(self.s3, self.request, self.preview, "request-2")
        self.assertEqual(retried["proposal_status"], "expired")
        self.assertEqual(retried["proposal"]["expires_at"], saved["expires_at"])

    def test_changed_preview_stops_before_s3_client(self):
        request = {**self.request, "expected_preview_sha256": "0" * 64}
        with patch.object(recovery.boto3, "client", return_value=QueryClient(self.preview)) as client:
            response = recovery.lambda_handler(request, SimpleNamespace(aws_request_id="request-1"))
        self.assertEqual(response["error_code"], "PREVIEW_CHANGED")
        self.assertEqual(client.call_count, 1)

    def test_unavailable_or_wrong_household(self):
        with self.assertRaises(recovery.RequestError) as caught:
            recovery.obtain_preview(QueryClient(self.preview, status="unavailable"), self.request)
        self.assertEqual(caught.exception.code, "PREVIEW_UNAVAILABLE")
        with self.assertRaises(RuntimeError):
            recovery.obtain_preview(QueryClient(self.preview, household="household-002"), self.request)

    def test_untrusted_preview_fingerprint_is_checked(self):
        changed = copy.deepcopy(self.preview)
        changed["proposed_record"]["battery_percent"] = 76
        with self.assertRaises(RuntimeError):
            recovery.obtain_preview(QueryClient(changed), self.request)

    def test_invalid_inputs_cannot_call_aws(self):
        cases = [
            {**self.request, "approved": True},
            {**self.request, "household_id": "household-002"},
            {**self.request, "battery_percent": True},
            {**self.request, "battery_percent": 150},
            {**self.request, "idempotency_key": "../other"},
            {**self.request, "expected_preview_sha256": "invalid"},
        ]
        with patch.object(recovery.boto3, "client") as client:
            for request in cases:
                with self.subTest(request=request):
                    response = recovery.lambda_handler(request, SimpleNamespace(aws_request_id="test"))
                    self.assertEqual(response["error_code"], "INVALID_REQUEST")
            client.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
