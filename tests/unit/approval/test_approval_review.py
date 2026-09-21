import copy
import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import approval_client as client
import approval_lambda as backend
import test_approval_lambda as approval_tests


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.fixture = approval_tests.ApprovalTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.proposal = self.fixture.proposal
        self.reason = "Reviewed synthetic battery correction from 150 to 75."
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "proposal.json"
        self.envelope = {
            "status": "ok", "operation": "propose_record_correction",
            "household_id": "household-001", "result": {
                "proposal": self.proposal, "proposal_sha256": client.digest(self.proposal),
            },
        }
        self.path.write_text(json.dumps(self.envelope), encoding="utf-8")

    def patches(self, answer):
        stack = ExitStack()
        stack.enter_context(patch.object(client, "verified_session", return_value=MagicMock()))
        stack.enter_context(patch.object(client.sys.stdin, "isatty", return_value=True))
        stack.enter_context(patch("builtins.input", return_value=answer))
        stack.enter_context(patch("sys.stdout", new_callable=io.StringIO))
        stack.enter_context(patch("sys.stderr", new_callable=io.StringIO))
        return stack

    def backend_response(self, request):
        result = backend.save_approval(
            self.fixture.s3, request, self.fixture.operator, "url-test", "lambda-test",
        )
        return 200, {"status": "ok", "operation": "approve_proposal", "household_id": "household-001", "result": result}, ""

    def test_reads_valid_complete_response_and_rejects_tampered_file(self):
        self.assertEqual(client.read_proposal_file(self.path), self.proposal)
        changed = copy.deepcopy(self.envelope)
        changed["result"]["proposal"]["preview"]["proposed_record"]["battery_percent"] = 76
        self.path.write_text(json.dumps(changed), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            client.read_proposal_file(self.path)

    def test_expired_proposal_stops_before_identity_or_http(self):
        proposal = copy.deepcopy(self.proposal)
        proposal["created_at"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        proposal["expires_at"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        self.envelope["result"] = {"proposal": proposal, "proposal_sha256": client.digest(proposal)}
        self.path.write_text(json.dumps(self.envelope), encoding="utf-8")
        with patch.object(client, "verified_session") as identity, patch.object(client, "send_request") as send:
            with self.assertRaisesRegex(ValueError, "expired"):
                client.approve_proposal(self.path, self.reason)
            identity.assert_not_called()
            send.assert_not_called()

    def test_cancel_sends_no_approval(self):
        for answer in ("", "yes", "APPROVE other-id"):
            with self.patches(answer), patch.object(client, "send_request") as send:
                client.approve_proposal(self.path, self.reason)
                send.assert_not_called()

    def test_confirmed_request_and_exact_retry_preserve_approval(self):
        expected_answer = f"APPROVE {self.proposal['proposal_id']}"
        with self.patches(expected_answer), patch.object(client, "build_signed_request", side_effect=lambda session, request: request), patch.object(client, "send_request", side_effect=self.backend_response) as send:
            client.approve_proposal(self.path, self.reason)
            before = dict(self.fixture.s3.objects)
            client.approve_proposal(self.path, self.reason)
        self.assertEqual(before, self.fixture.s3.objects)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(send.call_args_list[0].args[0], {
            "operation": "approve_proposal", "proposal_id": self.proposal["proposal_id"],
            "expected_proposal_sha256": client.digest(self.proposal), "reason": self.reason,
        })

    def test_expiry_during_review_stops_before_http(self):
        with self.patches(f"APPROVE {self.proposal['proposal_id']}"), patch.object(client, "require_unexpired", side_effect=[None, ValueError("Proposal expired")]), patch.object(client, "send_request") as send:
            with self.assertRaisesRegex(ValueError, "expired"):
                client.approve_proposal(self.path, self.reason)
            send.assert_not_called()

    def test_noninteractive_approval_is_rejected(self):
        with self.patches(f"APPROVE {self.proposal['proposal_id']}"), patch.object(client.sys.stdin, "isatty", return_value=False), patch.object(client, "send_request") as send:
            with self.assertRaisesRegex(RuntimeError, "interactive terminal"):
                client.approve_proposal(self.path, self.reason)
            send.assert_not_called()

    def test_wrong_receipt_identity_or_execution_claim_rejected(self):
        request = {"operation": "approve_proposal", "proposal_id": self.proposal["proposal_id"], "expected_proposal_sha256": client.digest(self.proposal), "reason": self.reason}
        _, body, _ = self.backend_response(request)
        for field, value in (("can_execute", True), ("replayed", True)):
            wrong = copy.deepcopy(body)
            wrong["result"][field] = value
            with self.assertRaises(RuntimeError):
                client.validate_approval_result(wrong, self.proposal, self.reason)
        wrong = copy.deepcopy(body)
        wrong["result"]["approval"]["operator"]["arn"] = client.EXPECTED_IDENTITIES["ring-mcp"]
        wrong["result"]["approval_sha256"] = client.digest(wrong["result"]["approval"])
        with self.assertRaises(RuntimeError):
            client.validate_approval_result(wrong, self.proposal, self.reason)

    def test_timeout_does_not_automatically_resubmit(self):
        with self.patches(f"APPROVE {self.proposal['proposal_id']}"), patch.object(client, "build_signed_request"), patch.object(client, "send_request", side_effect=TimeoutError) as send:
            with self.assertRaises(TimeoutError):
                client.approve_proposal(self.path, self.reason)
            send.assert_called_once()

    def test_duplicate_json_and_other_household_rejected(self):
        self.path.write_text('{"status":"ok","status":"ok"}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            client.read_proposal_file(self.path)
        self.envelope["household_id"] = "household-002"
        self.path.write_text(json.dumps(self.envelope), encoding="utf-8")
        with self.assertRaises(ValueError):
            client.read_proposal_file(self.path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
