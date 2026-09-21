import base64
import copy
import io
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from botocore.exceptions import ClientError

import approval_lambda as app


OPERATOR = "arn:aws:iam::455646290306:user/ring-project-approver"


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.closed = False
        self.streams = []

    def get_object(self, Bucket, Key):
        assert Bucket == app.BUCKET
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        stream = io.BytesIO(self.objects[Key])
        self.streams.append(stream)
        return {"Body": stream}

    def put_object(self, **kwargs):
        assert kwargs["Bucket"] == app.BUCKET
        assert kwargs["IfNoneMatch"] == "*"
        self.puts.append(kwargs)
        if kwargs["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def close(self):
        self.closed = True


class ApprovalTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.operator = {"arn": OPERATOR, "user_id": "AIDA-OPERATOR", "account_id": app.ACCOUNT_ID}
        self.original = {
            "event_id": "evt-batch-003", "household_id": app.HOUSEHOLD,
            "device_id": "camera-002", "event_time": "2026-09-18T12:00:00Z",
            "battery_percent": 150,
        }
        preview = {
            "preview_version": 1, "household_id": app.HOUSEHOLD,
            "rejection_id": 6, "original_record": self.original,
            "proposed_record": {**self.original, "battery_percent": 75},
            "changes": [{"field": "battery_percent", "before": 150, "after": 75}],
        }
        self.proposal_id = "a" * 64
        self.proposal = {
            "proposal_version": 1, "proposal_id": self.proposal_id,
            "household_id": app.HOUSEHOLD, "approval_status": "pending",
            "preview": preview, "preview_sha256": app.digest(preview),
            "created_at": (self.now - timedelta(minutes=1)).isoformat(),
            "expires_at": (self.now + timedelta(minutes=59)).isoformat(),
        }
        self.request = {
            "operation": "approve_proposal", "proposal_id": self.proposal_id,
            "expected_proposal_sha256": app.digest(self.proposal),
            "reason": "Reviewed the synthetic battery correction from 150 to 75.",
        }
        self.key = f"recovery/proposals/{app.HOUSEHOLD}/{self.proposal_id}.json"
        self.s3 = MemoryS3()
        self.s3.objects[self.key] = app.canonical_bytes(self.proposal)
        self.env = patch.dict(os.environ, {"APPROVER_ARN": OPERATOR})
        self.env.start()
        self.addCleanup(self.env.stop)

    def event(self, request=None):
        return {
            "version": "2.0", "rawPath": "/", "rawQueryString": "",
            "requestContext": {
                "accountId": app.ACCOUNT_ID, "requestId": "url-invocation",
                "http": {"method": "POST"},
                "authorizer": {"iam": {
                    "accountId": app.ACCOUNT_ID, "userArn": OPERATOR, "userId": "AIDA-OPERATOR",
                }},
            },
            "body": json.dumps(self.request if request is None else request), "isBase64Encoded": False,
        }

    def call(self, event):
        with patch.object(app.boto3, "client", return_value=self.s3) as factory:
            response = app.lambda_handler(event, SimpleNamespace(aws_request_id="lambda-invocation"))
        return response, json.loads(response["body"]), factory

    def test_success_records_identity_hash_and_expiry_without_changing_proposal(self):
        before = self.s3.objects[self.key]
        response, body, _ = self.call(self.event())
        self.assertEqual(response["statusCode"], 200)
        result = body["result"]
        self.assertTrue(result["created"])
        self.assertEqual(result["approval_status"], "approved")
        self.assertEqual(result["approval"]["operator"], self.operator)
        self.assertEqual(result["approval"]["proposal_sha256"], self.request["expected_proposal_sha256"])
        self.assertEqual(result["approval"]["expires_at"], self.proposal["expires_at"])
        self.assertFalse(result["can_execute"])
        self.assertFalse(result["replayed"])
        self.assertFalse(result["event_data_modified"])
        self.assertEqual(self.s3.objects[self.key], before)
        self.assertEqual(len(self.s3.objects), 2)
        self.assertTrue(self.s3.closed)
        self.assertTrue(all(s.closed for s in self.s3.streams))

    def test_exact_retry_preserves_approval_and_original_invocation(self):
        first = app.save_approval(self.s3, self.request, self.operator, "url-first", "lambda-first")
        second = app.save_approval(self.s3, self.request, self.operator, "url-retry", "lambda-retry")
        self.assertFalse(second["created"])
        self.assertEqual(first["approval"], second["approval"])
        self.assertEqual(first["approval_sha256"], second["approval_sha256"])
        self.assertEqual(second["approval"]["lambda_request_id"], "lambda-first")

    def test_changed_reason_or_operator_cannot_overwrite(self):
        app.save_approval(self.s3, self.request, self.operator, "u", "l")
        before = dict(self.s3.objects)
        for request, operator in (
            ({**self.request, "reason": "Changed decision evidence"}, self.operator),
            (self.request, {**self.operator, "user_id": "recreated-operator"}),
        ):
            with self.assertRaises(app.ApprovalError) as error:
                app.save_approval(self.s3, request, operator, "u", "l")
            self.assertEqual(error.exception.code, "APPROVAL_CONFLICT")
            self.assertEqual(before, self.s3.objects)

    def test_missing_context_wrong_operator_and_wrong_account_deny_before_s3(self):
        cases = [{}, self.request, self.event(), self.event(), self.event()]
        cases[2]["requestContext"]["authorizer"] = None
        cases[3]["requestContext"]["authorizer"]["iam"]["userArn"] = "arn:aws:iam::455646290306:user/ring-project-developer"
        cases[4]["requestContext"]["authorizer"]["iam"]["accountId"] = "111111111111"
        for event in cases:
            with self.subTest(event=event):
                response, body, factory = self.call(event)
                self.assertEqual(response["statusCode"], 403)
                self.assertIn(body["error_code"], {"OPERATOR_REQUIRED", "OPERATOR_FORBIDDEN"})
                factory.assert_not_called()

    def test_extra_approval_or_identity_fields_rejected(self):
        for field, value in (("approved", True), ("approved_by", OPERATOR), ("household_id", "household-002"), ("candidate", {})):
            _, body, factory = self.call(self.event({**self.request, field: value}))
            self.assertEqual(body["error_code"], "INVALID_REQUEST")
            factory.assert_not_called()

    def test_bad_fingerprint_rejected_without_writes(self):
        _, body, _ = self.call(self.event({**self.request, "expected_proposal_sha256": "0" * 64}))
        self.assertEqual(body["error_code"], "PROPOSAL_CHANGED")
        self.assertEqual(self.s3.puts, [])

    def test_expiry_boundary_and_expired_retry_cannot_renew(self):
        app.save_approval(self.s3, self.request, self.operator, "u", "l")
        before = dict(self.s3.objects)
        expiry = datetime.fromisoformat(self.proposal["expires_at"])
        for checked in (expiry, expiry + timedelta(seconds=1)):
            with patch.object(app, "datetime", wraps=datetime) as clock:
                clock.now.return_value = checked
                _, body, _ = self.call(self.event())
            self.assertEqual(body["error_code"], "PROPOSAL_EXPIRED")
            self.assertEqual(self.s3.objects, before)

    def test_corrupt_preview_cross_household_and_other_field_changes(self):
        cases = []
        for location, field, value in (
            ("proposal", "household_id", "household-002"),
            ("proposal", "preview_sha256", "0" * 64),
            ("candidate", "device_id", "different-device"),
            ("candidate", "battery_percent", True),
        ):
            proposal = copy.deepcopy(self.proposal)
            if location == "proposal":
                proposal[field] = value
            else:
                proposal["preview"]["proposed_record"][field] = value
                proposal["preview_sha256"] = app.digest(proposal["preview"])
            cases.append(proposal)
        for proposal in cases:
            self.s3.objects[self.key] = app.canonical_bytes(proposal)
            request = {**self.request, "expected_proposal_sha256": app.digest(proposal)}
            _, body, _ = self.call(self.event(request))
            self.assertEqual(body["error_code"], "INVALID_PROPOSAL")
        self.assertEqual(self.s3.puts, [])

    def test_missing_proposal_and_access_denial_have_no_writes(self):
        self.s3.objects.clear()
        _, body, _ = self.call(self.event())
        self.assertEqual(body["error_code"], "EVIDENCE_UNAVAILABLE")
        with patch.object(self.s3, "get_object", side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")):
            _, body, _ = self.call(self.event())
        self.assertEqual(body["error_code"], "EVIDENCE_UNAVAILABLE")
        self.assertEqual(self.s3.puts, [])

    def test_conditional_conflict_requests_exact_retry(self):
        with patch.object(self.s3, "put_object", side_effect=ClientError({"Error": {"Code": "ConditionalRequestConflict"}}, "PutObject")):
            _, body, _ = self.call(self.event())
        self.assertEqual(body["error_code"], "RETRY_REQUEST")

    def test_corrupt_existing_approval_is_not_accepted(self):
        first = app.save_approval(self.s3, self.request, self.operator, "u", "l")
        key = first["storage"]["key"]
        saved = json.loads(self.s3.objects[key])
        saved["expires_at"] = (self.now + timedelta(days=1)).isoformat()
        self.s3.objects[key] = app.canonical_bytes(saved)
        _, body, _ = self.call(self.event())
        self.assertEqual(body["error_code"], "INVALID_APPROVAL")

    def test_json_duplicate_fields_size_and_nonfinite_values(self):
        for raw in ('{"operation":"approve_proposal","operation":"other"}', '{"x":NaN}', '[]', 'x' * 22001):
            event = self.event()
            event["body"] = raw
            _, body, factory = self.call(event)
            self.assertEqual(body["error_code"], "INVALID_REQUEST")
            factory.assert_not_called()

    def test_base64_and_method_validation(self):
        event = self.event()
        event["body"] = base64.b64encode(event["body"].encode()).decode()
        event["isBase64Encoded"] = True
        self.assertEqual(app.parse_request(event), self.request)
        event["requestContext"]["http"]["method"] = "GET"
        response, _, factory = self.call(event)
        self.assertEqual(response["statusCode"], 405)
        factory.assert_not_called()

    def test_storage_failure_is_not_reported_as_approval(self):
        with patch.object(self.s3, "put_object", side_effect=TimeoutError):
            response, body, _ = self.call(self.event())
        self.assertEqual(response["statusCode"], 500)
        self.assertEqual(body["error_code"], "APPROVAL_FAILED")
        self.assertTrue(self.s3.closed)

    def test_uncertain_write_then_retry_returns_original_approval(self):
        original_put = self.s3.put_object

        def write_then_timeout(**kwargs):
            original_put(**kwargs)
            raise TimeoutError("Response lost after storage accepted the write")

        with patch.object(self.s3, "put_object", side_effect=write_then_timeout):
            response, body, _ = self.call(self.event())
        self.assertEqual(response["statusCode"], 500)
        self.assertEqual(body["error_code"], "APPROVAL_FAILED")
        before = dict(self.s3.objects)
        response, body, _ = self.call(self.event())
        self.assertEqual(response["statusCode"], 200)
        self.assertFalse(body["result"]["created"])
        self.assertEqual(self.s3.objects, before)

    def test_expiry_during_storage_is_returned_as_expired(self):
        expiry = datetime.fromisoformat(self.proposal["expires_at"])
        with patch.object(app, "datetime", wraps=datetime) as clock:
            clock.now.side_effect = [expiry - timedelta(seconds=2), expiry - timedelta(seconds=1), expiry]
            response, body, _ = self.call(self.event())
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(body["result"]["approval_status"], "expired")
        self.assertFalse(body["result"]["can_execute"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
