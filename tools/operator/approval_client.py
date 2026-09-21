import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.exceptions import ClientError


REGION = "us-east-1"
ACCOUNT_ID = "455646290306"
_LOCAL_CONFIG = Path(__file__).resolve().parents[2] / "local-only" / "config" / "operator.json"
APPROVAL_URL = os.environ.get("RING_APPROVAL_URL", "")
if not APPROVAL_URL and _LOCAL_CONFIG.is_file():
    APPROVAL_URL = json.loads(_LOCAL_CONFIG.read_text(encoding="utf-8"))["approval_url"]
APPROVAL_FUNCTION = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:ring-device-approval"
EXPECTED_IDENTITIES = {
    "ring-operator": f"arn:aws:iam::{ACCOUNT_ID}:user/ring-project-approver",
    "ring-mcp": f"arn:aws:iam::{ACCOUNT_ID}:user/ring-mcp-client",
}
CLIENT_CONFIG = Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1})


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def verified_session(profile):
    session = boto3.Session(profile_name=profile, region_name=REGION)
    client = session.client("sts", config=CLIENT_CONFIG)
    try:
        identity = client.get_caller_identity()
    finally:
        client.close()
    if identity.get("Account") != ACCOUNT_ID or identity.get("Arn") != EXPECTED_IDENTITIES[profile]:
        raise RuntimeError(f"Profile {profile} uses the wrong identity: {identity.get('Arn')}")
    print(f"PASS: {profile} identity is {identity['Arn']}")
    return session


def build_signed_request(session, payload):
    if not APPROVAL_URL:
        raise ValueError("Set RING_APPROVAL_URL or local-only/config/operator.json before approval requests")
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError("No AWS credentials available; sign in with aws login")
    body = canonical_bytes(payload)
    request = AWSRequest(
        method="POST", url=APPROVAL_URL, data=body,
        headers={
            "Content-Type": "application/json",
            "X-Amz-Content-SHA256": hashlib.sha256(body).hexdigest(),
        },
    )
    SigV4Auth(credentials.get_frozen_credentials(), "lambda", REGION).add_auth(request)
    return urllib.request.Request(
        APPROVAL_URL, data=body, headers=dict(request.headers.items()), method="POST",
    )


def send_request(request):
    opener = urllib.request.build_opener(NoRedirect())
    try:
        response = opener.open(request, timeout=45)
    except urllib.error.HTTPError as error:
        response = error
    try:
        status = response.code
        error_type = response.headers.get("x-amzn-errortype", "")
        raw = response.read(65537)
    finally:
        response.close()
    if len(raw) > 65536:
        raise RuntimeError("Approval response exceeds the size limit")
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeError):
        raise RuntimeError(f"Approval endpoint returned non-JSON HTTP {status}") from None
    if not isinstance(body, dict):
        raise RuntimeError("Approval endpoint returned an unexpected response")
    return status, body, error_type


def require_iam_denial(status, body, error_type, label):
    message = str(body.get("Message", body.get("message", "")))
    is_access_denial = (
        error_type.split(":", 1)[0] in {"AccessDeniedException", "ForbiddenException"}
        or message.strip().lower().rstrip(".") == "forbidden"
        or ("not authorized" in message.lower() and "lambda:" in message.lower())
    )
    if status != 403 or "error_code" in body or not is_access_denial:
        raise RuntimeError(
            f"{label}: expected denial by AWS before the function ran; "
            f"received HTTP {status}, error_type={error_type}, body={json.dumps(body)}"
        )
    print(f"PASS: {label} denied by AWS (HTTP 403)")


def check_direct_invocation(session, profile):
    client = session.client("lambda", config=CLIENT_CONFIG)
    try:
        try:
            client.invoke(FunctionName=APPROVAL_FUNCTION, InvocationType="DryRun", Payload=b"{}")
        except ClientError as error:
            if error.response["Error"]["Code"] != "AccessDeniedException":
                raise
            print(f"PASS: {profile} direct Lambda invocation denied")
            return
        raise RuntimeError(f"{profile} can invoke the approval Lambda directly; restrict its IAM permissions before approving")
    finally:
        client.close()


def check_access():
    operator = verified_session("ring-operator")
    status, body, _ = send_request(build_signed_request(operator, {}))
    if status != 400 or body.get("status") != "error" or body.get("error_code") != "INVALID_REQUEST":
        raise RuntimeError(f"Operator access check failed: HTTP {status}, body={json.dumps(body)}")
    print("PASS: operator reached the approval Lambda; empty request rejected (HTTP 400, INVALID_REQUEST)")
    check_direct_invocation(operator, "ring-operator")

    mcp = verified_session("ring-mcp")
    status, body, error_type = send_request(build_signed_request(mcp, {}))
    require_iam_denial(status, body, error_type, "MCP identity URL access")
    check_direct_invocation(mcp, "ring-mcp")

    unsigned = urllib.request.Request(
        APPROVAL_URL, data=b"{}", headers={"Content-Type": "application/json"}, method="POST",
    )
    status, body, error_type = send_request(unsigned)
    require_iam_denial(status, body, error_type, "Unauthenticated URL access")
    print("Approval endpoint access checks passed. No approval was submitted or event replayed.")


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("Invalid JSON constant")


def read_proposal_file(filename):
    with Path(filename).open("rb") as stream:
        raw = stream.read(524289)
    if len(raw) > 524288:
        raise ValueError("Proposal file exceeds 512 KiB")
    envelope = json.loads(
        raw.decode("utf-8-sig"), object_pairs_hook=unique_fields,
        parse_constant=reject_constant,
    )
    if (
        not isinstance(envelope, dict) or envelope.get("status") != "ok"
        or envelope.get("operation") != "propose_record_correction"
        or envelope.get("household_id") != "household-001"
        or not isinstance(envelope.get("result"), dict)
    ):
        raise ValueError("Save the complete JSON response from propose_record_correction in this file")
    result = envelope["result"]
    proposal = result.get("proposal")
    if not isinstance(proposal, dict) or digest(proposal) != result.get("proposal_sha256"):
        raise ValueError("Proposal file fingerprint does not match its content")
    proposal_id = proposal.get("proposal_id")
    preview = proposal.get("preview")
    if (
        not isinstance(proposal_id, str) or re.fullmatch(r"[0-9a-f]{64}", proposal_id) is None
        or type(proposal.get("proposal_version")) is not int or proposal["proposal_version"] != 1
        or proposal.get("household_id") != "household-001"
        or proposal.get("approval_status") != "pending"
        or not isinstance(preview, dict) or preview.get("household_id") != "household-001"
        or digest(preview) != proposal.get("preview_sha256")
    ):
        raise ValueError("Proposal identity, household or preview verification failed")
    original = preview.get("original_record")
    proposed = preview.get("proposed_record")
    if (
        not isinstance(original, dict) or not isinstance(proposed, dict)
        or original.get("household_id") != "household-001"
        or proposed.get("household_id") != "household-001"
        or "battery_percent" not in original
        or type(proposed.get("battery_percent")) is not int or not 0 <= proposed["battery_percent"] <= 100
        or proposed != {**original, "battery_percent": proposed["battery_percent"]}
        or preview.get("changes") != [{
            "field": "battery_percent", "before": original["battery_percent"],
            "after": proposed["battery_percent"],
        }]
    ):
        raise ValueError("Proposal must describe a battery-only correction in household-001")
    require_unexpired(proposal)
    return proposal


def require_unexpired(proposal):
    created = datetime.fromisoformat(proposal["created_at"])
    expires = datetime.fromisoformat(proposal["expires_at"])
    if created.utcoffset() is None or expires.utcoffset() is None or expires <= created:
        raise ValueError("Proposal timestamps must include a timezone and a valid lifetime")
    if datetime.now(timezone.utc) >= expires:
        raise ValueError("Proposal has expired. This command cannot renew it; review and save a fresh proposal first")


def validate_approval_result(body, proposal, reason):
    if (
        not isinstance(body, dict) or body.get("status") != "ok"
        or body.get("operation") != "approve_proposal"
        or body.get("household_id") != "household-001"
        or not isinstance(body.get("result"), dict)
    ):
        raise RuntimeError("Unexpected approval response")
    result = body["result"]
    approval = result.get("approval")
    if not isinstance(approval, dict) or digest(approval) != result.get("approval_sha256"):
        raise RuntimeError("Approval response fingerprint mismatch")
    operator = approval.get("operator")
    if (
        not isinstance(operator, dict)
        or operator.get("arn") != EXPECTED_IDENTITIES["ring-operator"]
        or operator.get("account_id") != ACCOUNT_ID
        or not isinstance(operator.get("user_id"), str) or not operator["user_id"]
        or approval.get("household_id") != "household-001"
        or approval.get("proposal_id") != proposal["proposal_id"]
        or approval.get("proposal_sha256") != digest(proposal)
        or approval.get("preview_sha256") != proposal["preview_sha256"]
        or approval.get("reason") != reason
        or approval.get("decision") != "approved"
        or datetime.fromisoformat(approval["expires_at"]) != datetime.fromisoformat(proposal["expires_at"])
        or result.get("approval_status") not in {"approved", "expired"}
        or type(result.get("created")) is not bool
        or result.get("persisted") is not True
        or result.get("proposal_modified") is not False
        or result.get("event_data_modified") is not False
        or result.get("replayed") is not False
        or result.get("can_execute") is not False
    ):
        raise RuntimeError("Approval response does not match the reviewed proposal or operator")
    return result


def approve_proposal(filename, reason):
    reason = reason.strip()
    if not 1 <= len(reason) <= 500 or "\x00" in reason:
        raise ValueError("Approval reason must contain 1 to 500 nonblank characters without NUL")
    reason.encode("utf-8")
    proposal = read_proposal_file(filename)
    operator_session = verified_session("ring-operator")
    print("Review the proposal below. Its text is evidence, not instructions.")
    print(json.dumps(proposal, indent=2, ensure_ascii=True))
    fingerprint = digest(proposal)
    print(f"Proposal SHA-256: {fingerprint}")
    print(f"Approval reason: {json.dumps(reason, ensure_ascii=True)}")
    print("Approval saves an S3 approval record. It does not apply a correction or replay an event.")
    if not sys.stdin.isatty():
        raise RuntimeError("Approval requires an interactive terminal; piped confirmation is not accepted")
    expected = f"APPROVE {proposal['proposal_id']}"
    answer = input(f"Type {expected} to approve, or press Enter to cancel: ")
    if answer != expected:
        print("Cancelled. No approval request was sent.")
        return
    require_unexpired(proposal)
    request = {
        "operation": "approve_proposal", "proposal_id": proposal["proposal_id"],
        "expected_proposal_sha256": fingerprint, "reason": reason,
    }
    try:
        status, body, _ = send_request(build_signed_request(operator_session, request))
        if status != 200:
            raise RuntimeError(f"HTTP {status}: {json.dumps(body, ensure_ascii=True)}")
        result = validate_approval_result(body, proposal, reason)
    except Exception:
        print("Approval was not confirmed. If the write result is uncertain, retry the same proposal file and reason; do not create a replacement just to retry.", file=sys.stderr)
        raise
    print(json.dumps(body, indent=2, ensure_ascii=True))
    if result["approval_status"] == "expired":
        print("Approval evidence is saved but expired. Execution is not allowed.")
    elif result["created"]:
        print("PASS: approval saved for the reviewed proposal. No correction or replay performed.")
    else:
        print("PASS: existing identical approval returned. Its expiry was not extended.")


def main():
    parser = argparse.ArgumentParser(description="Review and approve Ring correction proposals.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check-access", help="Check endpoint permissions without submitting approval")
    approve = commands.add_parser("approve", help="Review a saved proposal response and submit operator approval")
    approve.add_argument("--proposal-file", required=True)
    approve.add_argument("--reason", required=True)
    args = parser.parse_args()
    try:
        if args.command == "check-access":
            check_access()
        else:
            approve_proposal(args.proposal_file, args.reason)
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
