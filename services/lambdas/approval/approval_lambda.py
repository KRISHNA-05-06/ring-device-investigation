import base64
import binascii
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


ACCOUNT_ID = "455646290306"
HOUSEHOLD = "household-001"
BUCKET = "ring-device-investigation-455646290306-us-east-1"
HEX64 = re.compile(r"[0-9a-f]{64}")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ApprovalError(Exception):
    def __init__(self, code, message, status_code=400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def canonical_bytes(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")


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


def decode_json(raw):
    return json.loads(raw, object_pairs_hook=unique_fields, parse_constant=reject_constant)


def authenticated_operator(event):
    """Use AWS_IAM URL context; IAM must restrict callers to URL invocation."""
    context = event.get("requestContext") if isinstance(event, dict) else None
    authorizer = context.get("authorizer") if isinstance(context, dict) else None
    identity = authorizer.get("iam") if isinstance(authorizer, dict) else None
    if (
        not isinstance(identity, dict)
        or event.get("version") != "2.0"
        or context.get("accountId") != ACCOUNT_ID
        or identity.get("accountId") != ACCOUNT_ID
    ):
        raise ApprovalError("OPERATOR_REQUIRED", "Use the AWS-authenticated approval URL with the operator identity", 403)
    allowed = os.environ.get("APPROVER_ARN", "")
    if not re.fullmatch(r"arn:aws:iam::455646290306:user/[A-Za-z0-9+=,.@_/-]+", allowed):
        raise RuntimeError("APPROVER_ARN must identify the dedicated IAM operator user")
    if identity.get("userArn") != allowed or not isinstance(identity.get("userId"), str) or not identity["userId"]:
        raise ApprovalError("OPERATOR_FORBIDDEN", "This AWS identity is not the configured operator", 403)
    http = context.get("http", {})
    if not isinstance(http, dict) or http.get("method") != "POST" or event.get("rawPath") != "/":
        raise ApprovalError("INVALID_REQUEST", "Use POST on the approval URL root", 405)
    if event.get("rawQueryString"):
        raise ApprovalError("INVALID_REQUEST", "Query parameters are not supported")
    request_id = context.get("requestId")
    if not isinstance(request_id, str) or not request_id:
        raise ApprovalError("INVALID_REQUEST", "Missing URL request identity")
    return {
        "arn": identity["userArn"], "user_id": identity["userId"],
        "account_id": identity["accountId"],
    }, request_id


def parse_request(event):
    body = event.get("body")
    if not isinstance(body, str) or len(body) > 22000:
        raise ApprovalError("INVALID_REQUEST", "Expected a JSON body of at most 16000 bytes")
    try:
        encoded = event.get("isBase64Encoded", False)
        if type(encoded) is not bool:
            raise ValueError("Invalid encoding flag")
        raw = base64.b64decode(body, validate=True) if encoded else body.encode("utf-8")
        if len(raw) > 16000:
            raise ValueError("Body exceeds limit")
        request = decode_json(raw.decode("utf-8"))
    except (ValueError, UnicodeError, binascii.Error, RecursionError):
        raise ApprovalError("INVALID_REQUEST", "Body must be valid JSON within the size limit") from None
    fields = {"operation", "proposal_id", "expected_proposal_sha256", "reason"}
    if not isinstance(request, dict) or set(request) != fields or request["operation"] != "approve_proposal":
        raise ApprovalError("INVALID_REQUEST", "Require only operation, proposal_id, expected_proposal_sha256, and reason")
    for field in ("proposal_id", "expected_proposal_sha256"):
        if not isinstance(request[field], str) or HEX64.fullmatch(request[field]) is None:
            raise ApprovalError("INVALID_REQUEST", f"{field} must be 64 lowercase hexadecimal characters")
    reason = request["reason"]
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500 or "\x00" in reason:
        raise ApprovalError("INVALID_REQUEST", "reason must contain 1 to 500 nonblank characters without NUL")
    try:
        reason.encode("utf-8")
    except UnicodeError:
        raise ApprovalError("INVALID_REQUEST", "reason must contain valid Unicode") from None
    return {**request, "reason": reason.strip()}


def read_object(s3, key):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in {"NoSuchKey", "AccessDenied", "403", "404"}:
            raise ApprovalError("EVIDENCE_UNAVAILABLE", "Cannot read the requested evidence; it may be missing or access may be denied", 409) from None
        raise
    stream = response["Body"]
    try:
        raw = stream.read(524289)
    finally:
        stream.close()
    try:
        if len(raw) > 524288:
            raise ValueError("Evidence too large")
        value = decode_json(raw)
        if not isinstance(value, dict):
            raise ValueError("Expected object")
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise ApprovalError("INVALID_EVIDENCE", "Stored evidence is not a valid JSON object", 409) from None


def verify_proposal(proposal, request, now):
    if digest(proposal) != request["expected_proposal_sha256"]:
        raise ApprovalError("PROPOSAL_CHANGED", "The saved proposal does not match the fingerprint you reviewed", 409)
    preview = proposal.get("preview")
    if (
        type(proposal.get("proposal_version")) is not int or proposal["proposal_version"] != 1
        or proposal.get("proposal_id") != request["proposal_id"]
        or proposal.get("household_id") != HOUSEHOLD
        or proposal.get("approval_status") != "pending"
        or not isinstance(preview, dict)
        or preview.get("household_id") != HOUSEHOLD
        or type(preview.get("preview_version")) is not int or preview["preview_version"] != 1
        or proposal.get("preview_sha256") != digest(preview)
    ):
        raise ApprovalError("INVALID_PROPOSAL", "Proposal identity, household, or preview verification failed", 409)
    original = preview.get("original_record")
    proposed = preview.get("proposed_record")
    if (
        not isinstance(original, dict) or not isinstance(proposed, dict)
        or original.get("household_id") != HOUSEHOLD or proposed.get("household_id") != HOUSEHOLD
        or "battery_percent" not in original
        or type(proposed.get("battery_percent")) is not int or not 0 <= proposed["battery_percent"] <= 100
        or proposed != {**original, "battery_percent": proposed["battery_percent"]}
        or preview.get("changes") != [{
            "field": "battery_percent", "before": original["battery_percent"], "after": proposed["battery_percent"],
        }]
        or type(preview.get("rejection_id")) is not int or preview["rejection_id"] < 1
    ):
        raise ApprovalError("INVALID_PROPOSAL", "Expected a battery-only correction for this household", 409)
    try:
        created = datetime.fromisoformat(proposal["created_at"])
        expires = datetime.fromisoformat(proposal["expires_at"])
        if created.utcoffset() is None or expires.utcoffset() is None or expires - created != timedelta(hours=1) or created > now:
            raise ValueError("Invalid lifetime")
    except (KeyError, TypeError, ValueError):
        raise ApprovalError("INVALID_PROPOSAL", "Proposal timestamps are invalid", 409) from None
    if now >= expires:
        raise ApprovalError("PROPOSAL_EXPIRED", "The proposal has expired; this approval cannot renew it", 409)
    return expires


def save_approval(s3, request, operator, url_request_id, invocation_id):
    proposal = read_object(s3, f"recovery/proposals/{HOUSEHOLD}/{request['proposal_id']}.json")
    now = datetime.now(timezone.utc)
    expires = verify_proposal(proposal, request, now)
    fingerprint = digest({"request": request, "operator": operator})
    key = f"recovery/approvals/{HOUSEHOLD}/{request['proposal_id']}.json"
    approval = {
        "approval_version": 1, "approval_id": fingerprint,
        "proposal_id": request["proposal_id"], "proposal_sha256": request["expected_proposal_sha256"],
        "preview_sha256": proposal["preview_sha256"], "household_id": HOUSEHOLD,
        "decision": "approved", "operator": operator, "reason": request["reason"],
        "approved_at": now.isoformat(), "expires_at": expires.isoformat(),
        "url_request_id": url_request_id, "lambda_request_id": invocation_id,
    }
    created = True
    if datetime.now(timezone.utc) >= expires:
        raise ApprovalError("PROPOSAL_EXPIRED", "The proposal expired before the approval write", 409)
    try:
        s3.put_object(
            Bucket=BUCKET, Key=key, Body=canonical_bytes(approval),
            ContentType="application/json", ServerSideEncryption="AES256", IfNoneMatch="*",
        )
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in {"ConditionalRequestConflict", "409"}:
            raise ApprovalError("RETRY_REQUEST", "Retry the exact approval request", 409) from None
        if code not in {"PreconditionFailed", "412"}:
            raise
        saved = read_object(s3, key)
        if saved.get("approval_id") != fingerprint:
            raise ApprovalError("APPROVAL_CONFLICT", "An approval already exists for a different request or operator", 409)
        stable_fields = set(approval) - {"approved_at", "url_request_id", "lambda_request_id"}
        if set(saved) != set(approval) or any(saved[field] != approval[field] for field in stable_fields):
            raise ApprovalError("INVALID_APPROVAL", "Existing approval does not match the reviewed proposal", 409)
        try:
            approved_at = datetime.fromisoformat(saved["approved_at"])
            proposal_created = datetime.fromisoformat(proposal["created_at"])
            if approved_at.utcoffset() is None or not proposal_created <= approved_at <= now or approved_at >= expires:
                raise ValueError("Invalid approval time")
            if any(not isinstance(saved[field], str) or not saved[field] for field in ("url_request_id", "lambda_request_id")):
                raise ValueError("Missing invocation evidence")
        except (TypeError, ValueError):
            raise ApprovalError("INVALID_APPROVAL", "Existing approval has invalid creation evidence", 409) from None
        approval = saved
        created = False
    expired = datetime.now(timezone.utc) >= expires
    return {
        "approval_status": "expired" if expired else "approved", "created": created,
        "persisted": True, "approval": approval, "approval_sha256": digest(approval),
        "storage": {"bucket": BUCKET, "key": key},
        "proposal_modified": False, "event_data_modified": False,
        "replayed": False, "can_execute": False,
        "execution_tracking": "not_implemented", "source_revalidated": False,
        "message": "Approval evidence saved. No correction or replay was performed; execution checks are still required.",
    }


def http_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json", "cache-control": "no-store"},
        "body": canonical_bytes(body).decode("utf-8"), "isBase64Encoded": False,
    }


def lambda_handler(event, context):
    s3 = None
    try:
        operator, url_request_id = authenticated_operator(event)
        request = parse_request(event)
        s3 = boto3.client(
            "s3", region_name="us-east-1",
            config=Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 2}),
        )
        result = save_approval(s3, request, operator, url_request_id, context.aws_request_id)
        logger.info(json.dumps({
            "operation": "approve_proposal", "proposal_id": request["proposal_id"],
            "operator_arn": operator["arn"], "created": result["created"],
            "request_id": context.aws_request_id,
        }))
        return http_response(200, {"status": "ok", "operation": "approve_proposal", "household_id": HOUSEHOLD, "result": result})
    except ApprovalError as error:
        logger.info(json.dumps({"operation": "approve_proposal", "error_code": error.code, "request_id": context.aws_request_id}))
        return http_response(error.status_code, {"status": "error", "error_code": error.code, "message": str(error)})
    except Exception as error:
        logger.error(json.dumps({"operation": "approve_proposal", "error_type": type(error).__name__, "request_id": context.aws_request_id}))
        return http_response(500, {"status": "error", "error_code": "APPROVAL_FAILED", "message": "Approval failed; check configuration, permissions, and CloudWatch logs. An uncertain write must be retried with the same request."})
    finally:
        if s3 is not None:
            s3.close()
