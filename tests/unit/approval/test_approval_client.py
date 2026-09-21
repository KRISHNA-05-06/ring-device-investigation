import io
import json
import unittest
import urllib.error
from unittest.mock import MagicMock, patch

from botocore.credentials import Credentials
from botocore.exceptions import ClientError

import approval_client as app


class ClientTests(unittest.TestCase):
    def test_signed_request_uses_fixed_endpoint_and_payload(self):
        session = MagicMock()
        session.get_credentials.return_value = Credentials("example-access-key", "example-secret-key", "example-session-token")
        request = app.build_signed_request(session, {})
        self.assertEqual(request.full_url, app.APPROVAL_URL)
        self.assertEqual(request.data, b"{}")
        self.assertEqual(request.method, "POST")
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertIn("AWS4-HMAC-SHA256", headers["authorization"])
        self.assertIn("/us-east-1/lambda/aws4_request", headers["authorization"])
        self.assertIn("x-amz-security-token", headers)
        self.assertIn("x-amz-content-sha256", headers)

    def test_wrong_profile_identity_is_rejected(self):
        client = MagicMock()
        client.get_caller_identity.return_value = {"Account": app.ACCOUNT_ID, "Arn": app.EXPECTED_IDENTITIES["ring-mcp"]}
        with patch.object(app.boto3, "Session") as session:
            session.return_value.client.return_value = client
            with self.assertRaisesRegex(RuntimeError, "wrong identity"):
                app.verified_session("ring-operator")
        client.close.assert_called_once()

    def test_denial_checks_do_not_accept_application_or_authentication_failures(self):
        app.require_iam_denial(403, {"Message": "Forbidden"}, "", "test")
        for status, body, kind in (
            (403, {"error_code": "OPERATOR_FORBIDDEN"}, ""),
            (403, {"Message": "The security token included in the request is expired"}, "ExpiredTokenException"),
            (403, {"Message": "Signature mismatch"}, "InvalidSignatureException"),
            (500, {"Message": "Forbidden"}, ""),
            (400, {"error_code": "INVALID_REQUEST"}, ""),
        ):
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                app.require_iam_denial(status, body, kind, "test")

    def test_direct_access_uses_dry_run_and_only_accepts_access_denied(self):
        session = MagicMock()
        client = session.client.return_value
        client.invoke.side_effect = ClientError({"Error": {"Code": "AccessDeniedException"}}, "Invoke")
        app.check_direct_invocation(session, "ring-operator")
        self.assertEqual(client.invoke.call_args.kwargs["InvocationType"], "DryRun")
        client.close.assert_called_once()
        client.invoke.side_effect = None
        with self.assertRaisesRegex(RuntimeError, "can invoke"):
            app.check_direct_invocation(session, "ring-operator")

    def test_http_error_body_is_read_and_closed(self):
        stream = io.BytesIO(b'{"Message":"Forbidden"}')
        error = urllib.error.HTTPError(app.APPROVAL_URL, 403, "Forbidden", {}, stream)
        with patch.object(app.urllib.request, "build_opener") as factory:
            factory.return_value.open.side_effect = error
            status, body, _ = app.send_request(MagicMock())
        self.assertEqual(status, 403)
        self.assertEqual(body["Message"], "Forbidden")
        self.assertTrue(stream.closed)
        self.assertIsInstance(factory.call_args.args[0], app.NoRedirect)

    def test_complete_access_check_uses_only_empty_bodies(self):
        operator, mcp = MagicMock(), MagicMock()
        sessions = {"ring-operator": operator, "ring-mcp": mcp}
        responses = [
            (400, {"status": "error", "error_code": "INVALID_REQUEST"}, ""),
            (403, {"Message": "Forbidden"}, ""),
            (403, {"Message": "Forbidden"}, ""),
        ]
        with patch.object(app, "verified_session", side_effect=lambda profile: sessions[profile]), patch.object(app, "build_signed_request") as sign, patch.object(app, "send_request", side_effect=responses) as send, patch.object(app, "check_direct_invocation") as direct:
            app.check_access()
        self.assertEqual([call.args[1] for call in sign.call_args_list], [{}, {}])
        self.assertEqual(send.call_args_list[-1].args[0].data, b"{}")
        self.assertEqual(direct.call_count, 2)

    def test_redirects_are_not_followed(self):
        self.assertIsNone(app.NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://example.com"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
