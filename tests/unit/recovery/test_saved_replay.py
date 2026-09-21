import copy
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

import recovery_lambda as recovery


class SavedReplayTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.proposal_id = "a" * 64
        self.original = {
            "event_id": "evt-batch-003", "household_id": recovery.HOUSEHOLD,
            "device_id": "camera-002", "event_time": "2026-09-18T12:00:00Z",
            "battery_percent": 150,
        }
        self.candidate = {**self.original, "battery_percent": 75}
        preview = {
            "preview_version": 1, "household_id": recovery.HOUSEHOLD,
            "rejection_id": 6, "original_record": self.original,
            "proposed_record": self.candidate,
            "changes": [{"field": "battery_percent", "before": 150, "after": 75}],
        }
        self.proposal = {
            "proposal_version": 1, "proposal_id": self.proposal_id,
            "household_id": recovery.HOUSEHOLD, "approval_status": "pending",
            "preview": preview, "preview_sha256": recovery.digest(preview),
            "created_at": self.now.isoformat(),
            "expires_at": (self.now + timedelta(hours=1)).isoformat(),
        }
        self.prediction = {
            "scope": "one_candidate_against_current_database_snapshot",
            "predicted_outcome": "accepted", "error_code": None, "error_message": None,
            "candidate_digest": recovery.digest({**self.candidate, "event_time": "2026-09-18T12:00:00+00:00"}),
            "normalized_event_time": "2026-09-18T12:00:00+00:00",
            "validator_rules_sha256": "b" * 64, "data_modified": False,
            "replay_performed": False, "authorization_checked": False,
            "requires_revalidation": True,
        }
        self.request = {"operation": "preview_replay", "proposal_id": self.proposal_id}

    def s3_client(self, proposal=None):
        client = MagicMock()
        client.list_objects_v2.return_value = {}
        data = recovery.canonical_bytes(proposal or self.proposal)
        client.get_object.side_effect = lambda **kwargs: {"Body": io.BytesIO(data)}
        return client

    def query_client(self, prediction=None, **body_overrides):
        client = MagicMock()
        body = {
            "status": "ok", "operation": "preview_replay",
            "household_id": recovery.HOUSEHOLD, "database_user": "ring_reader",
            "result": prediction or self.prediction, **body_overrides,
        }
        client.invoke.side_effect = lambda **kwargs: {"Payload": io.BytesIO(recovery.canonical_bytes(body))}
        return client

    def test_handler_uses_exact_saved_candidate_without_writes(self):
        s3, query = self.s3_client(), self.query_client()
        before = copy.deepcopy(self.proposal)
        with patch.object(recovery.boto3, "client", side_effect=lambda name, **kw: s3 if name == "s3" else query):
            body = recovery.lambda_handler(self.request, SimpleNamespace(aws_request_id="test"))
        result = body["result"]
        self.assertEqual(body["operation"], "preview_replay")
        self.assertEqual(result["preview_status"], "completed")
        self.assertEqual(result["prediction"]["predicted_outcome"], "accepted")
        self.assertEqual(result["proposal_sha256"], recovery.digest(before))
        self.assertFalse(result["can_execute"])
        self.assertFalse(result["source_revalidated"])
        self.assertEqual(json.loads(query.invoke.call_args.kwargs["Payload"]), {
            "operation": "preview_replay", "candidate": self.candidate,
        })
        self.assertEqual(s3.get_object.call_args.kwargs, {
            "Bucket": recovery.BUCKET,
            "Key": f"recovery/proposals/{recovery.HOUSEHOLD}/{self.proposal_id}.json",
        })
        s3.put_object.assert_not_called()
        s3.delete_object.assert_not_called()
        s3.close.assert_called_once()
        query.close.assert_called_once()
        self.assertEqual(self.proposal, before)

    def test_exact_expiry_and_expired_proposals_skip_query(self):
        expiry = datetime.fromisoformat(self.proposal["expires_at"])
        for now in (expiry, expiry + timedelta(seconds=1)):
            with self.subTest(now=now), patch.object(recovery, "datetime", wraps=datetime) as clock, patch.object(recovery.boto3, "client") as factory:
                clock.now.return_value = now
                result = recovery.preview_saved_replay(self.s3_client(), self.proposal_id)
                self.assertEqual(result["block_reason"], "PROPOSAL_EXPIRED")
                self.assertIsNone(result["prediction"])
                factory.assert_not_called()

    def test_expiry_during_query_is_reported(self):
        expiry = datetime.fromisoformat(self.proposal["expires_at"])
        with patch.object(recovery, "datetime", wraps=datetime) as clock, patch.object(recovery.boto3, "client", return_value=self.query_client()):
            clock.now.side_effect = [expiry - timedelta(seconds=1), expiry]
            result = recovery.preview_saved_replay(self.s3_client(), self.proposal_id)
        self.assertEqual(result["preview_status"], "blocked")
        self.assertEqual(result["proposal_status"], "expired")
        self.assertFalse(result["can_execute"])

    def test_missing_or_unreadable_proposal_never_queries(self):
        with patch.object(recovery.boto3, "client") as factory:
            s3 = self.s3_client()
            s3.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            result = recovery.preview_saved_replay(s3, self.proposal_id)
            self.assertFalse(result["found"])
            self.assertEqual(result["block_reason"], "PROPOSAL_NOT_FOUND")
            s3.get_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
            with self.assertRaises(recovery.RequestError) as error:
                recovery.preview_saved_replay(s3, self.proposal_id)
            self.assertEqual(error.exception.code, "PROPOSAL_UNAVAILABLE")
            factory.assert_not_called()

    def test_corruption_and_changes_outside_battery_are_rejected(self):
        cases = []
        bad_hash = copy.deepcopy(self.proposal)
        bad_hash["preview_sha256"] = "0" * 64
        cases.append(bad_hash)
        for field, value in (("household_id", "household-002"), ("device_id", "other")):
            proposal = copy.deepcopy(self.proposal)
            proposal["preview"]["proposed_record"][field] = value
            proposal["preview_sha256"] = recovery.digest(proposal["preview"])
            cases.append(proposal)
        for proposal in cases:
            with self.subTest(proposal=proposal), patch.object(recovery.boto3, "client") as factory:
                with self.assertRaises(recovery.RequestError) as error:
                    recovery.preview_saved_replay(self.s3_client(proposal), self.proposal_id)
                self.assertEqual(error.exception.code, "INVALID_PROPOSAL")
                factory.assert_not_called()

    def test_duplicate_and_rejection_predictions_remain_predictions(self):
        for outcome in ("duplicate", "rejected"):
            prediction = {**self.prediction, "predicted_outcome": outcome}
            if outcome == "rejected":
                prediction.update(error_code="EVENT_ID_CONFLICT", error_message="Different content")
            with patch.object(recovery.boto3, "client", return_value=self.query_client(prediction)):
                result = recovery.preview_saved_replay(self.s3_client(), self.proposal_id)
            self.assertEqual(result["prediction"]["predicted_outcome"], outcome)
            self.assertFalse(result["can_execute"])
            self.assertFalse(result["replay_performed"])

    def test_wrong_query_identity_digest_and_flags_fail_closed(self):
        clients = [self.query_client(household_id="household-002"), self.query_client(database_user="ring_admin")]
        for field, value in (("candidate_digest", "0" * 64), ("data_modified", True), ("predicted_outcome", "unknown")):
            clients.append(self.query_client({**self.prediction, field: value}))
        failed = MagicMock()
        failed.invoke.return_value = {"FunctionError": "Unhandled", "Payload": io.BytesIO(b'{}')}
        clients.append(failed)
        for query in clients:
            with self.subTest(query=query), patch.object(recovery.boto3, "client", return_value=query):
                with self.assertRaises(RuntimeError):
                    recovery.preview_saved_replay(self.s3_client(), self.proposal_id)
                query.close.assert_called_once()

    def test_invalid_requests_do_not_access_aws(self):
        for request in (
            {**self.request, "candidate": self.candidate},
            {**self.request, "approved": True},
            {**self.request, "proposal_id": "../other"},
            {**self.request, "household_id": "household-002"},
        ):
            with self.subTest(request=request), patch.object(recovery.boto3, "client") as factory:
                result = recovery.lambda_handler(request, None)
                self.assertEqual(result["error_code"], "INVALID_REQUEST")
                factory.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
