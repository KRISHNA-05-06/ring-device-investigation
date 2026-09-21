import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


HOUSEHOLD = "household-001"
BUCKET = "ring-device-investigation-455646290306-us-east-1"
QUERY_FUNCTION = "arn:aws:lambda:us-east-1:455646290306:function:ring-device-query"
OPERATION = "propose_record_correction"
APPROVER = {
    "account_id": "455646290306",
    "arn": "arn:aws:iam::455646290306:user/ring-project-approver",
    "user_id": "AIDAWUFVCUGBNXT6GDULH",
}
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class RequestError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def canonical_bytes(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def parse_request(event):
    if isinstance(event, dict) and event.get("operation") == "save_quality_report":
        return parse_quality_report_request(event)
    if isinstance(event, dict) and event.get("operation") == "recompute_device_health":
        return parse_snapshot_request(event)
    if (
        isinstance(event, dict)
        and isinstance(event.get("operation"), str)
        and event["operation"] in {"get_operation_status", "get_audit_history", "preview_replay", "apply_record_correction", "replay_records"}
    ):
        if set(event) != {"operation", "proposal_id"}:
            raise RequestError("INVALID_REQUEST", "This operation requires only operation and proposal_id")
        proposal_id = event["proposal_id"]
        if not isinstance(proposal_id, str) or re.fullmatch(r"[0-9a-f]{64}", proposal_id) is None:
            raise RequestError("INVALID_REQUEST", "proposal_id must be 64 lowercase hexadecimal characters")
        return dict(event)

    fields = {
        "operation", "rejection_id", "battery_percent", "reason",
        "expected_preview_sha256", "idempotency_key",
    }
    if not isinstance(event, dict) or set(event) != fields:
        raise RequestError("INVALID_REQUEST", "Request must contain exactly the six supported fields")
    if event["operation"] != OPERATION:
        raise RequestError("INVALID_REQUEST", "Unsupported operation")
    if type(event["rejection_id"]) is not int or not 1 <= event["rejection_id"] <= 9223372036854775807:
        raise RequestError("INVALID_REQUEST", "rejection_id must be a positive 64-bit integer")
    if type(event["battery_percent"]) is not int or not 0 <= event["battery_percent"] <= 100:
        raise RequestError("INVALID_REQUEST", "battery_percent must be an integer from 0 to 100")
    reason = event["reason"]
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise RequestError("INVALID_REQUEST", "reason must contain 1 to 500 nonblank characters")
    for field, pattern in (
        ("expected_preview_sha256", r"[0-9a-f]{64}"),
        ("idempotency_key", r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}"),
    ):
        if not isinstance(event[field], str) or re.fullmatch(pattern, event[field]) is None:
            raise RequestError("INVALID_REQUEST", f"Invalid {field}")
    return {**event, "reason": reason.strip()}


def unique_json_fields(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON field")
        value[key] = item
    return value


def reject_json_constant(value):
    raise ValueError("Invalid JSON constant")


def read_json_body(stream):
    try:
        raw = stream.read(524289)
    finally:
        stream.close()
    if len(raw) > 524288:
        raise RuntimeError("Response exceeds the size limit")
    result = json.loads(raw, object_pairs_hook=unique_json_fields, parse_constant=reject_json_constant)
    if not isinstance(result, dict):
        raise RuntimeError("Expected a JSON object")
    return result


def obtain_preview(client, request):
    response = client.invoke(
        FunctionName=QUERY_FUNCTION,
        InvocationType="RequestResponse",
        Payload=canonical_bytes({
            "operation": "preview_record_correction",
            "rejection_id": request["rejection_id"],
            "battery_percent": request["battery_percent"],
            "reason": request["reason"],
        }),
    )
    body = read_json_body(response["Payload"])
    if response.get("FunctionError"):
        raise RuntimeError("Query Lambda failed")
    if (
        body.get("status") != "ok"
        or body.get("operation") != "preview_record_correction"
        or body.get("household_id") != HOUSEHOLD
        or body.get("database_user") != "ring_reader"
    ):
        raise RuntimeError("Unexpected query response or identity")
    result = body["result"]
    if result.get("preview_status") != "ready":
        raise RequestError("PREVIEW_UNAVAILABLE", "This rejection cannot produce a correction proposal")
    preview = result["preview"]
    if (
        preview.get("household_id") != HOUSEHOLD
        or preview.get("rejection_id") != request["rejection_id"]
        or preview.get("reason") != request["reason"]
        or preview["proposed_record"].get("battery_percent") != request["battery_percent"]
        or digest(preview) != result["preview_sha256"]
    ):
        raise RuntimeError("Preview content does not match the request")
    if result["preview_sha256"] != request["expected_preview_sha256"]:
        raise RequestError("PREVIEW_CHANGED", "Obtain and review a fresh preview before saving")
    return preview


def save_proposal(s3, request, preview, invocation_id):
    proposal_id = digest({"household_id": HOUSEHOLD, "idempotency_key": request["idempotency_key"]})
    key = f"recovery/proposals/{HOUSEHOLD}/{proposal_id}.json"
    now = datetime.now(timezone.utc)
    proposal = {
        "proposal_version": 1,
        "proposal_id": proposal_id,
        "household_id": HOUSEHOLD,
        "request_sha256": digest(request),
        "preview_sha256": digest(preview),
        "preview": preview,
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(),
        "creation_request_id": invocation_id,
        "approval_status": "pending",
    }
    created = True
    try:
        s3.put_object(
            Bucket=BUCKET, Key=key, Body=canonical_bytes(proposal),
            ContentType="application/json", ServerSideEncryption="AES256",
            IfNoneMatch="*",
        )
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in {"ConditionalRequestConflict", "409"}:
            raise RequestError("RETRY_REQUEST", "Retry the exact request with the same idempotency_key") from None
        if code not in {"PreconditionFailed", "412"}:
            raise
        proposal = read_json_body(s3.get_object(Bucket=BUCKET, Key=key)["Body"])
        if proposal.get("request_sha256") != digest(request):
            raise RequestError("IDEMPOTENCY_CONFLICT", "This idempotency_key already identifies a different request")
        if (
            proposal.get("proposal_id") != proposal_id
            or proposal.get("household_id") != HOUSEHOLD
            or proposal.get("preview_sha256") != digest(preview)
            or digest(proposal["preview"]) != digest(preview)
            or proposal.get("approval_status") != "pending"
        ):
            raise RuntimeError("Stored proposal verification failed")
        created = False

    expired = datetime.now(timezone.utc) >= datetime.fromisoformat(proposal["expires_at"])
    return {
        "proposal_status": "expired" if expired else "pending_approval",
        "created": created,
        "persisted": True,
        "proposal": proposal,
        "proposal_sha256": digest(proposal),
        "storage": {"bucket": BUCKET, "key": key},
        "applied": False,
        "replayed": False,
    }


def load_verified_proposal(s3, proposal_id):
    key = f"recovery/proposals/{HOUSEHOLD}/{proposal_id}.json"
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code == "NoSuchKey":
            return None
        if code in {"AccessDenied", "403"}:
            raise RequestError(
                "PROPOSAL_UNAVAILABLE",
                "Cannot read this proposal. It may be missing or access may be denied.",
            ) from None
        raise

    proposal = read_json_body(response["Body"])
    preview = proposal.get("preview")
    if (
        proposal.get("proposal_version") != 1
        or proposal.get("proposal_id") != proposal_id
        or proposal.get("household_id") != HOUSEHOLD
        or proposal.get("approval_status") != "pending"
        or not isinstance(preview, dict)
        or preview.get("household_id") != HOUSEHOLD
        or preview.get("preview_version") != 1
        or proposal.get("preview_sha256") != digest(preview)
    ):
        raise RequestError("INVALID_PROPOSAL", "Stored proposal failed identity or content checks")

    try:
        created_at = datetime.fromisoformat(proposal["created_at"])
        expires_at = datetime.fromisoformat(proposal["expires_at"])
        if (
            created_at.utcoffset() is None or expires_at.utcoffset() is None
            or expires_at - created_at != timedelta(hours=1)
        ):
            raise ValueError("Invalid proposal lifetime")
    except (KeyError, TypeError, ValueError):
        raise RequestError("INVALID_PROPOSAL", "Stored proposal has invalid timestamps") from None

    return proposal


def load_verified_approval(s3, proposal):
    proposal_id = proposal["proposal_id"]
    key = f"recovery/approvals/{HOUSEHOLD}/{proposal_id}.json"
    try:
        listing = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1)
        contents = listing.get("Contents", [])
        if not isinstance(contents, list):
            raise RuntimeError("Invalid approval listing response")
        if not any(item.get("Key") == key for item in contents):
            return None
        approval = read_json_body(s3.get_object(Bucket=BUCKET, Key=key)["Body"])
    except ClientError as error:
        raise RequestError("APPROVAL_UNAVAILABLE", "Approval evidence could not be read; this does not mean approval is absent") from error
    except (ValueError, TypeError, RecursionError, RuntimeError) as error:
        raise RequestError("INVALID_APPROVAL", "Approval evidence is not valid JSON within the size limit") from error

    fields = {
        "approval_version", "approval_id", "proposal_id", "proposal_sha256",
        "preview_sha256", "household_id", "decision", "operator", "reason",
        "approved_at", "expires_at", "url_request_id", "lambda_request_id",
    }
    reason = approval.get("reason")
    if (
        set(approval) != fields
        or type(approval.get("approval_version")) is not int or approval["approval_version"] != 1
        or approval.get("household_id") != HOUSEHOLD
        or approval.get("proposal_id") != proposal_id
        or approval.get("proposal_sha256") != digest(proposal)
        or approval.get("preview_sha256") != proposal["preview_sha256"]
        or approval.get("operator") != APPROVER
        or approval.get("decision") != "approved"
        or not isinstance(reason, str) or not 1 <= len(reason) <= 500
        or reason != reason.strip() or "\x00" in reason
        or any(not isinstance(approval.get(field), str) or not approval[field].strip() for field in ("url_request_id", "lambda_request_id"))
    ):
        raise RequestError("INVALID_APPROVAL", "Approval does not match the proposal, household, or configured operator")
    request = {
        "operation": "approve_proposal", "proposal_id": proposal_id,
        "expected_proposal_sha256": digest(proposal), "reason": reason,
    }
    if approval["approval_id"] != digest({"request": request, "operator": APPROVER}):
        raise RequestError("INVALID_APPROVAL", "Approval request fingerprint does not match its content")
    try:
        reason.encode("utf-8")
        approved = datetime.fromisoformat(approval["approved_at"])
        expires = datetime.fromisoformat(approval["expires_at"])
        created = datetime.fromisoformat(proposal["created_at"])
        proposal_expiry = datetime.fromisoformat(proposal["expires_at"])
        if (
            approved.utcoffset() is None or expires.utcoffset() is None
            or not created <= approved < expires
            or expires != proposal_expiry or approved > datetime.now(timezone.utc)
        ):
            raise ValueError("Invalid approval lifetime")
    except (TypeError, ValueError, UnicodeError):
        raise RequestError("INVALID_APPROVAL", "Approval timestamps or reason are invalid") from None
    return approval


def approval_summary(approval, now):
    if approval is None:
        return {"found": False, "status": "not_recorded"}
    expires = datetime.fromisoformat(approval["expires_at"])
    return {
        "found": True,
        "status": "expired" if now >= expires else "approved",
        "approval_id": approval["approval_id"],
        "approval_sha256": digest(approval),
        "recorded_decision": approval["decision"],
        "approved_at": approval["approved_at"], "expires_at": approval["expires_at"],
        "operator": approval["operator"], "reason": approval["reason"],
        "storage": {
            "bucket": BUCKET,
            "key": f"recovery/approvals/{HOUSEHOLD}/{approval['proposal_id']}.json",
        },
    }


def get_operation_status(s3, proposal_id):
    proposal = load_verified_proposal(s3, proposal_id)
    if proposal is None:
        return {"found": False, "proposal_id": proposal_id, "scope": "proposal_approval_correction_and_replay"}
    approval = load_verified_approval(s3, proposal)
    key = f"recovery/proposals/{HOUSEHOLD}/{proposal_id}.json"
    preview = proposal["preview"]
    created_at = datetime.fromisoformat(proposal["created_at"])
    expires_at = datetime.fromisoformat(proposal["expires_at"])
    correction = application_evidence(s3, proposal, approval)
    replay = replay_evidence(s3, proposal, approval, correction)
    now = datetime.now(timezone.utc)
    expired = now >= expires_at
    return {
        "found": True, "scope": "proposal_approval_correction_and_replay", "proposal_id": proposal_id,
        "proposal_status": "expired" if expired else ("approved" if approval else "pending_approval"),
        "stored_approval_status": proposal["approval_status"],
        "created_at": created_at.isoformat(), "expires_at": expires_at.isoformat(),
        "checked_at": now.isoformat(),
        "seconds_until_expiry": max(0, int((expires_at - now).total_seconds())),
        "proposal_sha256": digest(proposal), "preview_sha256": proposal["preview_sha256"],
        "rejection_id": preview["rejection_id"], "changes": preview["changes"], "reason": preview["reason"],
        "storage": {"bucket": BUCKET, "key": key},
        "approval_tracking": "implemented", "approval": approval_summary(approval, now),
        "correction": correction,
        "replay": replay,
        "execution_tracking": "correction_and_replay", "replay_tracking": "implemented", "can_execute": False,
    }


def get_audit_history(s3, proposal_id):
    proposal = load_verified_proposal(s3, proposal_id)
    result = {
        "found": proposal is not None, "scope": "proposal_approval_correction_and_replay",
        "proposal_id": proposal_id, "events": [], "returned_events": 0,
        "history_complete": False,
        "coverage": {
            "source": "saved_proposal_approval_correction_and_replay", "included": ["proposal_creation"] if proposal else [],
            "not_included": ["save_retries", "approval_retries", "reads", "failed_requests", "expiry_events"],
            "approval_tracking": "implemented", "execution_tracking": "correction_and_replay",
            "replay_tracking": "implemented",
            "operator_identity": "approval_and_executor_if_recorded",
        },
    }
    if proposal is None:
        return result
    approval = load_verified_approval(s3, proposal)
    invocation_id = proposal.get("creation_request_id")
    if not isinstance(invocation_id, str) or not invocation_id.strip():
        raise RequestError("INVALID_PROPOSAL", "Stored proposal is missing creation evidence")
    preview = proposal["preview"]
    result["proposal_sha256"] = digest(proposal)
    result["storage"] = {
        "bucket": BUCKET, "key": f"recovery/proposals/{HOUSEHOLD}/{proposal_id}.json",
    }
    result["events"] = [{
        "event_type": "proposal_created", "occurred_at": proposal["created_at"],
        "lambda_request_id": invocation_id, "actor_identity": None,
        "rejection_id": preview["rejection_id"], "preview_sha256": proposal["preview_sha256"],
        "changes": preview["changes"], "reason": preview["reason"],
        "approval_status_at_creation": proposal["approval_status"],
    }]
    if approval is not None:
        result["coverage"]["included"].append("operator_approval")
        result["approval_sha256"] = digest(approval)
        result["approval_storage"] = {
            "bucket": BUCKET, "key": f"recovery/approvals/{HOUSEHOLD}/{proposal_id}.json",
        }
        result["events"].append({
            "event_type": "proposal_approved", "occurred_at": approval["approved_at"],
            "actor_identity": approval["operator"],
            "lambda_request_id": approval["lambda_request_id"], "url_request_id": approval["url_request_id"],
            "approval_id": approval["approval_id"], "proposal_sha256": approval["proposal_sha256"],
            "preview_sha256": approval["preview_sha256"], "reason": approval["reason"],
            "decision": approval["decision"], "expires_at": approval["expires_at"],
        })
    correction = application_evidence(s3, proposal, approval)
    if correction["found"]:
        result["coverage"]["included"].append("correction_application")
        result["events"].append({
            "event_type": "correction_artifact_created",
            "occurred_at": None,
            "time_note": "Exact S3 commit time was not recorded; write_authorized_at is the pre-write authorization check.",
            "write_authorized_at": correction["write_authorized_at"],
            "actor_identity": None, "executor_role": correction["executor_role"],
            "lambda_request_id": correction["lambda_request_id"],
            "correction_id": correction["correction_id"],
            "correction_sha256": correction["correction_sha256"],
            "proposal_sha256": digest(proposal), "approval_sha256": digest(approval),
            "storage": correction["storage"],
            "database_modified": False, "replayed": False,
        })
    replay = replay_evidence(s3, proposal, approval, correction)
    result["replay"] = replay
    if replay["found"]:
        metadata = replay["submission"]["metadata"]
        result["coverage"]["included"].append("replay_submission")
        result["events"].append({
            "event_type": "replay_submitted", "occurred_at": None,
            "time_note": "write_authorized_at is the pre-write authorization check, not the S3 commit time.",
            "write_authorized_at": metadata["write-authorized-at"],
            "actor_identity": None, "executor_role": metadata["executor-role"],
            "lambda_request_id": metadata["lambda-request-id"],
            "proposal_sha256": metadata["proposal-sha256"], "approval_sha256": metadata["approval-sha256"],
            "correction_id": metadata["correction-id"], "correction_sha256": metadata["correction-sha256"],
            "payload_sha256": metadata["payload-sha256"], "storage": replay["storage"],
        })
    result["coverage"]["ingestion_observations"] = "current_database_snapshot_separate_from_recorded_events"
    result["returned_events"] = len(result["events"])
    return result




def obtain_replay_prediction(client, candidate):
    response = client.invoke(
        FunctionName=QUERY_FUNCTION,
        InvocationType="RequestResponse",
        Payload=canonical_bytes({"operation": "preview_replay", "candidate": candidate}),
    )
    body = read_json_body(response["Payload"])
    if response.get("FunctionError"):
        raise RuntimeError("Query Lambda failed")
    if (
        body.get("status") != "ok"
        or body.get("operation") != "preview_replay"
        or body.get("household_id") != HOUSEHOLD
        or body.get("database_user") != "ring_reader"
    ):
        raise RuntimeError("Unexpected replay prediction response or identity")
    prediction = body.get("result")
    if not isinstance(prediction, dict):
        raise RuntimeError("Missing replay prediction")
    outcome = prediction.get("predicted_outcome")
    if (
        prediction.get("scope") != "one_candidate_against_current_database_snapshot"
        or outcome not in {"accepted", "duplicate", "rejected"}
        or prediction.get("data_modified") is not False
        or prediction.get("replay_performed") is not False
        or prediction.get("authorization_checked") is not False
        or prediction.get("requires_revalidation") is not True
    ):
        raise RuntimeError("Invalid replay prediction contract")
    for field in ("validator_rules_sha256",):
        if not isinstance(prediction.get(field), str) or re.fullmatch(r"[0-9a-f]{64}", prediction[field]) is None:
            raise RuntimeError("Invalid validator fingerprint")
    if outcome == "rejected":
        if not isinstance(prediction.get("error_code"), str) or not prediction["error_code"]:
            raise RuntimeError("Rejected prediction is missing its error code")
    else:
        normalized = dict(candidate)
        timestamp = datetime.fromisoformat(candidate["event_time"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise RuntimeError("Accepted prediction has no timezone")
        normalized["event_time"] = timestamp.astimezone(timezone.utc).isoformat()
        if (
            prediction.get("candidate_digest") != digest(normalized)
            or prediction.get("normalized_event_time") != normalized["event_time"]
            or prediction.get("error_code") is not None
            or prediction.get("error_message") is not None
        ):
            raise RuntimeError("Prediction does not match the saved candidate")
    return prediction


def preview_saved_replay(s3, proposal_id):
    proposal = load_verified_proposal(s3, proposal_id)
    result = {
        "found": proposal is not None,
        "scope": "saved_proposal_replay_preview",
        "proposal_id": proposal_id,
        "preview_status": "unavailable",
        "prediction": None,
        "data_modified": False,
        "replay_performed": False,
        "can_execute": False,
        "approval_tracking": "implemented",
        "execution_tracking": "correction_and_replay",
        "replay_tracking": "implemented",
        "source_revalidated": False,
        "requires_revalidation": True,
    }
    if proposal is None:
        result["block_reason"] = "PROPOSAL_NOT_FOUND"
        return result

    approval = load_verified_approval(s3, proposal)
    preview = proposal["preview"]
    candidate = preview.get("proposed_record")
    original = preview.get("original_record")
    if (
        not isinstance(candidate, dict) or not isinstance(original, dict)
        or candidate.get("household_id") != HOUSEHOLD
        or original.get("household_id") != HOUSEHOLD
        or type(candidate.get("battery_percent")) is not int
        or not 0 <= candidate["battery_percent"] <= 100
        or "battery_percent" not in original
        or candidate != {**original, "battery_percent": candidate["battery_percent"]}
        or preview.get("changes") != [{
            "field": "battery_percent", "before": original["battery_percent"],
            "after": candidate["battery_percent"],
        }]
    ):
        raise RequestError("INVALID_PROPOSAL", "Saved correction is not a battery-only change in this household")

    expires_at = datetime.fromisoformat(proposal["expires_at"])
    result.update({
        "proposal_sha256": digest(proposal),
        "preview_sha256": proposal["preview_sha256"],
        "rejection_id": preview["rejection_id"],
        "changes": preview["changes"],
        "expires_at": proposal["expires_at"],
        "stored_approval_status": proposal["approval_status"],
        "storage": {
            "bucket": BUCKET,
            "key": f"recovery/proposals/{HOUSEHOLD}/{proposal_id}.json",
        },
    })
    now = datetime.now(timezone.utc)
    result["approval"] = approval_summary(approval, now)
    if now >= expires_at:
        result.update({
            "preview_status": "blocked", "proposal_status": "expired",
            "block_reason": "PROPOSAL_EXPIRED", "checked_at": now.isoformat(),
        })
        return result

    query_client = boto3.client(
        "lambda", region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=40, retries={"total_max_attempts": 1}),
    )
    try:
        prediction = obtain_replay_prediction(query_client, candidate)
    finally:
        query_client.close()
    now = datetime.now(timezone.utc)
    expired = now >= expires_at
    result.update({
        "preview_status": "blocked" if expired else "completed",
        "proposal_status": "expired" if expired else ("approved" if approval else "pending_approval"),
        "approval": approval_summary(approval, now),
        "block_reason": "PROPOSAL_EXPIRED" if expired else None,
        "checked_at": now.isoformat(),
        "prediction": prediction,
        "message": "Prediction for the saved candidate only. No approval or replay was performed. Source evidence and execution checks must be repeated before replay.",
    })
    return result


def application_evidence(s3, proposal, approval):
    key = f"recovery/corrections/{HOUSEHOLD}/{proposal['proposal_id']}.json"
    try:
        contents = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1).get("Contents", [])
        present = any(item.get("Key") == key for item in contents)
    except (ClientError, BotoCoreError):
        raise RequestError("CORRECTION_UNAVAILABLE", "Could not check for correction evidence") from None
    if not present:
        return {"found": False, "application_status": "not_applied"}
    if approval is None:
        raise RequestError("INVALID_CORRECTION", "A correction exists without verified approval evidence")
    saved = load_correction(s3, key, correction_binding(proposal, approval), approval, known_present=True)
    return {
        "found": True, "application_status": "correction_saved",
        "correction_id": saved["correction_id"], "correction_sha256": digest(saved),
        "write_authorized_at": saved["write_authorized_at"],
        "lambda_request_id": saved["lambda_request_id"], "executor_role": saved["executor_role"],
        "invoking_actor": saved["invoking_actor"],
        "storage": {"bucket": BUCKET, "key": key},
        "database_modified": False, "replayed": False,
    }


def correction_binding(proposal, approval):
    preview = proposal["preview"]
    original, candidate, source = (preview.get(name) for name in ("original_record", "proposed_record", "source"))
    if (
        not isinstance(original, dict) or not isinstance(candidate, dict) or not isinstance(source, dict)
        or original.get("household_id") != HOUSEHOLD or candidate.get("household_id") != HOUSEHOLD
        or type(candidate.get("battery_percent")) is not int or not 0 <= candidate["battery_percent"] <= 100
        or "battery_percent" not in original
        or candidate != {**original, "battery_percent": candidate["battery_percent"]}
        or preview.get("changes") != [{"field": "battery_percent", "before": original["battery_percent"], "after": candidate["battery_percent"]}]
        or type(preview.get("rejection_id")) is not int or preview["rejection_id"] <= 0
        or not isinstance(preview.get("reason"), str) or not preview["reason"].strip()
        or source.get("bucket") != BUCKET
        or not isinstance(source.get("key"), str) or not source["key"].startswith("raw/")
        or type(source.get("line_number")) is not int or source["line_number"] <= 0
        or not isinstance(source.get("raw_record_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", source["raw_record_sha256"]) is None
    ):
        raise RequestError("INVALID_PROPOSAL", "Correction must preserve identity and change only the approved battery value")
    binding = {
        "household_id": HOUSEHOLD, "proposal_id": proposal["proposal_id"],
        "proposal_sha256": digest(proposal), "preview_sha256": proposal["preview_sha256"],
        "approval_id": approval["approval_id"], "approval_sha256": digest(approval),
        "rejection_id": preview["rejection_id"], "source": source,
        "original_record": original, "corrected_record": candidate,
        "changes": preview["changes"], "reason": preview["reason"],
        "approved_by": approval["operator"], "approval_expires_at": approval["expires_at"],
    }
    return {**binding, "correction_id": digest(binding)}


def load_correction(s3, key, binding, approval, known_present=False):
    try:
        if not known_present:
            contents = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1).get("Contents", [])
            if not any(item.get("Key") == key for item in contents):
                return None
        saved = read_json_body(s3.get_object(Bucket=BUCKET, Key=key)["Body"])
    except (ClientError, BotoCoreError):
        raise RequestError("CORRECTION_UNAVAILABLE", "Could not read correction evidence; retry the same proposal ID") from None
    except (ValueError, TypeError, RuntimeError, RecursionError):
        raise RequestError("INVALID_CORRECTION", "Stored correction is not valid bounded JSON") from None
    try:
        evidence = saved["source_verification"]
        checked = datetime.fromisoformat(saved["write_authorized_at"])
        if (
            type(saved.get("correction_version")) is not int or saved["correction_version"] != 1
            or any(digest(saved.get(name)) != digest(value) for name, value in binding.items())
            or saved.get("recorded_action") != "correction_artifact_created"
            or saved.get("executor_role") != "arn:aws:iam::455646290306:role/ring-recovery-role"
            or saved.get("invoking_actor") is not None
            or not isinstance(saved.get("lambda_request_id"), str) or not saved["lambda_request_id"]
            or checked.utcoffset() is None
            or not datetime.fromisoformat(approval["approved_at"]) <= checked < datetime.fromisoformat(approval["expires_at"])
            or checked > datetime.now(timezone.utc)
            or not isinstance(evidence, dict)
            or evidence.get("raw_record_sha256") != binding["source"]["raw_record_sha256"]
            or not isinstance(evidence.get("object_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", evidence["object_sha256"]) is None
            or evidence.get("rejection_preview_sha256") != binding["preview_sha256"]
        ):
            raise ValueError("Binding mismatch")
    except (KeyError, ValueError, TypeError):
        raise RequestError("INVALID_CORRECTION", "Existing correction does not match the approved content and evidence") from None
    return saved


def verify_original_source(s3, preview):
    source = preview["source"]
    try:
        response = s3.get_object(Bucket=BUCKET, Key=source["key"])
        stream = response["Body"]
        try:
            raw = stream.read(1_048_577)
        finally:
            stream.close()
    except (ClientError, BotoCoreError):
        raise RequestError("SOURCE_UNAVAILABLE", "The original source could not be read") from None
    try:
        if len(raw) > 1_048_576:
            raise ValueError("Source too large")
        text = raw.decode("utf-8-sig")
        if "\x00" in text:
            raise ValueError("Unsupported null character")
        lines = text.splitlines()
        line = lines[source["line_number"] - 1]
        raw_digest = hashlib.sha256(line.encode("utf-8")).hexdigest()
        parsed = json.loads(line, object_pairs_hook=unique_json_fields, parse_constant=reject_json_constant)
        if raw_digest != source["raw_record_sha256"] or parsed != preview["original_record"]:
            raise ValueError("Source mismatch")
    except (UnicodeError, ValueError, TypeError, IndexError, RecursionError):
        raise RequestError("SOURCE_CHANGED", "The original source line no longer matches the reviewed rejection") from None
    return {
        "raw_record_sha256": raw_digest, "object_sha256": hashlib.sha256(raw).hexdigest(),
        "object_etag": response.get("ETag"), "object_version_id": response.get("VersionId"),
        "rejection_preview_sha256": digest(preview),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def correction_receipt(saved, key, created):
    now = datetime.now(timezone.utc)
    expired = now >= datetime.fromisoformat(saved["approval_expires_at"])
    return {
        "application_status": "correction_saved", "created": created, "persisted": True,
        "correction_id": saved["correction_id"], "correction_sha256": digest(saved),
        "correction": saved, "storage": {"bucket": BUCKET, "key": key},
        "approval_current_status": "expired" if expired else "approved",
        "checked_at": now.isoformat(),
        "source_revalidated_this_request": created,
        "original_data_modified": False, "database_modified": False, "replayed": False,
        "can_replay": False, "replay_tracking": "implemented",
        "message": "Correction artifact saved only. No source was overwritten or event ingested. Replay requires fresh authorization, source checks, and validation. Existing-artifact reads do not renew approval.",
    }


def apply_record_correction(s3, proposal_id, invocation_id):
    proposal = load_verified_proposal(s3, proposal_id)
    if proposal is None:
        raise RequestError("PROPOSAL_NOT_FOUND", "No accessible saved proposal exists")
    approval = load_verified_approval(s3, proposal)
    if approval is None:
        raise RequestError("APPROVAL_REQUIRED", "No verified operator approval is recorded")
    binding = correction_binding(proposal, approval)
    key = f"recovery/corrections/{HOUSEHOLD}/{proposal_id}.json"
    existing = load_correction(s3, key, binding, approval)
    if existing is not None:
        return correction_receipt(existing, key, False)
    if datetime.now(timezone.utc) >= datetime.fromisoformat(approval["expires_at"]):
        raise RequestError("PROPOSAL_EXPIRED", "Expired approval cannot authorize a new correction artifact")
    query = boto3.client("lambda", region_name="us-east-1", config=Config(
        connect_timeout=5, read_timeout=40, retries={"total_max_attempts": 1}))
    try:
        preview = obtain_preview(query, {
            "rejection_id": proposal["preview"]["rejection_id"],
            "battery_percent": proposal["preview"]["proposed_record"]["battery_percent"],
            "reason": proposal["preview"]["reason"], "expected_preview_sha256": proposal["preview_sha256"],
        })
    finally:
        query.close()
    evidence = verify_original_source(s3, preview)
    # Re-read proposal and approval after external source/query calls.
    fresh_proposal = load_verified_proposal(s3, proposal_id)
    if fresh_proposal is None or digest(fresh_proposal) != digest(proposal):
        raise RequestError("PROPOSAL_CHANGED", "Proposal evidence changed during application")
    fresh_approval = load_verified_approval(s3, fresh_proposal)
    if fresh_approval is None or digest(fresh_approval) != digest(approval):
        raise RequestError("APPROVAL_CHANGED", "Approval evidence changed during application")
    authorized_at = datetime.now(timezone.utc)
    if authorized_at >= datetime.fromisoformat(approval["expires_at"]):
        raise RequestError("PROPOSAL_EXPIRED", "Approval expired while checking the source")
    saved = {
        "correction_version": 1, **binding, "recorded_action": "correction_artifact_created",
        "write_authorized_at": authorized_at.isoformat(), "source_verification": evidence,
        "lambda_request_id": invocation_id,
        "executor_role": "arn:aws:iam::455646290306:role/ring-recovery-role",
        "invoking_actor": None,
    }
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=canonical_bytes(saved), ContentType="application/json",
                      ServerSideEncryption="AES256", IfNoneMatch="*")
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in {"PreconditionFailed", "412"}:
            existing = load_correction(s3, key, binding, approval)
            if existing is not None:
                return correction_receipt(existing, key, False)
        if code in {"AccessDenied", "403"}:
            raise RequestError("CORRECTION_WRITE_DENIED", "The correction write was denied") from None
        raise RequestError("RETRY_SAME_PROPOSAL", "Write outcome is unresolved; retry only the same proposal ID") from None
    except BotoCoreError:
        raise RequestError("RETRY_SAME_PROPOSAL", "Write response was not confirmed; retry only the same proposal ID") from None
    return correction_receipt(saved, key, True)


REPLAY_EXECUTOR = "arn:aws:iam::455646290306:role/ring-recovery-role"
MAX_REPLAY_BYTES = 16000


def replay_binding(proposal, approval, correction):
    """String metadata binds a one-record payload to previously verified evidence."""
    payload = canonical_bytes(correction["corrected_record"]) + b"\n"
    if len(payload) > MAX_REPLAY_BYTES:
        raise RequestError("REPLAY_TOO_LARGE", "Corrected replay exceeds the single-record size limit")
    metadata = {
        "replay-version": "1", "replay-id": proposal["proposal_id"],
        "household-id": HOUSEHOLD, "proposal-sha256": digest(proposal),
        "approval-id": approval["approval_id"], "approval-sha256": digest(approval),
        "correction-id": correction["correction_id"], "correction-sha256": digest(correction),
        "payload-sha256": hashlib.sha256(payload).hexdigest(),
        "approval-expires-at": approval["expires_at"],
        "executor-role": REPLAY_EXECUTOR,
    }
    return payload, metadata


def load_replay_submission(s3, key, payload, binding, approval, known_present=False):
    try:
        if not known_present:
            items = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1).get("Contents", [])
            if not any(item.get("Key") == key for item in items):
                return None
        response = s3.get_object(Bucket=BUCKET, Key=key)
        stream = response["Body"]
        try:
            raw = stream.read(MAX_REPLAY_BYTES + 1)
        finally:
            stream.close()
    except (ClientError, BotoCoreError):
        raise RequestError("REPLAY_UNAVAILABLE", "Could not confirm the replay object; it may already be submitted. Retry only the same proposal ID") from None
    metadata = response.get("Metadata", {})
    try:
        authorized = datetime.fromisoformat(metadata["write-authorized-at"])
        required = set(binding) | {"write-authorized-at", "lambda-request-id", "source-object-sha256", "validator-rules-sha256", "candidate-digest", "predicted-outcome"}
        normalized = dict(json.loads(payload))
        normalized["event_time"] = datetime.fromisoformat(normalized["event_time"].replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        if (
            raw != payload or set(metadata) != required
            or any(metadata.get(name) != value for name, value in binding.items())
            or response.get("ContentType") != "application/x-ndjson"
            or response.get("ServerSideEncryption") != "AES256"
            or authorized.utcoffset() is None
            or not datetime.fromisoformat(approval["approved_at"]) <= authorized < datetime.fromisoformat(approval["expires_at"])
            or authorized > datetime.now(timezone.utc)
            or not re.fullmatch(r"[A-Za-z0-9-]{1,128}", metadata["lambda-request-id"])
            or metadata["candidate-digest"] != digest(normalized)
            or metadata["predicted-outcome"] not in {"accepted", "duplicate"}
            or any(re.fullmatch(r"[0-9a-f]{64}", metadata[field]) is None for field in ("source-object-sha256", "validator-rules-sha256"))
        ):
            raise ValueError("Submission binding mismatch")
    except (KeyError, ValueError, TypeError, AttributeError):
        raise RequestError("REPLAY_CONFLICT", "Existing replay bytes or metadata do not match the approved correction; no overwrite was attempted") from None
    return {
        "metadata": metadata,
        "etag": response.get("ETag"), "version_id": response.get("VersionId"),
    }


def replay_receipt(submission, key, created, remaining_millis=None):
    metadata = submission["metadata"]
    if remaining_millis is not None and remaining_millis() < 22000:
        ingestion = {"available": False, "observed_outcome": "unknown", "error_code": "INGESTION_OBSERVATION_UNAVAILABLE",
                     "message": "Submission verified. Insufficient invocation time to query ingestion; read status with the same proposal ID."}
    else:
        ingestion = observe_replay_ingestion(metadata["replay-id"])
    now = datetime.now(timezone.utc)
    return {
        "submission_status": "submitted", "replay_id": metadata["replay-id"],
        "created": created, "persisted": True,
        "storage": {"bucket": BUCKET, "key": key}, "submission": submission,
        "approval_current_status": "expired" if now >= datetime.fromisoformat(metadata["approval-expires-at"]) else "approved",
        "checked_at": now.isoformat(),
        "source_revalidated_this_request": created,
        "ingestion_status": ingestion["observed_outcome"],
        "ingestion": ingestion,
        "original_data_modified": False, "database_written_by_this_function": False,
        "pipeline_may_modify_database": True,
        "message": "Replay source exists with verified bytes and submission metadata. The ingestion section reports observed runs separately, not a verification of the current event row. An existing-object retry does not resubmit or renew approval.",
    }


def replay_records(s3, proposal_id, invocation_id, remaining_millis=None):
    proposal = load_verified_proposal(s3, proposal_id)
    if proposal is None:
        raise RequestError("PROPOSAL_NOT_FOUND", "No accessible saved proposal exists")
    approval = load_verified_approval(s3, proposal)
    if approval is None:
        raise RequestError("APPROVAL_REQUIRED", "No verified operator approval is recorded")
    correction_key = f"recovery/corrections/{HOUSEHOLD}/{proposal_id}.json"
    correction = load_correction(s3, correction_key, correction_binding(proposal, approval), approval)
    if correction is None:
        raise RequestError("CORRECTION_REQUIRED", "Apply the approved correction before requesting replay")
    payload, binding = replay_binding(proposal, approval, correction)
    key = f"raw/replays/{HOUSEHOLD}/{proposal_id}.jsonl"
    existing = load_replay_submission(s3, key, payload, binding, approval)
    if existing is not None:
        return replay_receipt(existing, key, False, remaining_millis)
    if datetime.now(timezone.utc) >= datetime.fromisoformat(approval["expires_at"]):
        raise RequestError("PROPOSAL_EXPIRED", "Expired approval cannot authorize a new replay submission")
    query = boto3.client("lambda", region_name="us-east-1", config=Config(
        connect_timeout=5, read_timeout=25, retries={"total_max_attempts": 1}))
    try:
        preview = obtain_preview(query, {
            "rejection_id": proposal["preview"]["rejection_id"],
            "battery_percent": correction["corrected_record"]["battery_percent"],
            "reason": proposal["preview"]["reason"],
            "expected_preview_sha256": proposal["preview_sha256"],
        })
        prediction = obtain_replay_prediction(query, correction["corrected_record"])
    finally:
        query.close()
    if prediction["predicted_outcome"] == "rejected":
        raise RequestError("REPLAY_VALIDATION_FAILED", f"Current validation predicts rejection: {prediction['error_code']}")
    evidence = verify_original_source(s3, preview)
    # Repeat approval/proposal/correction reads after external calls. These are
    # snapshots, not a distributed transaction or a guarantee about later ingestion.
    fresh_proposal = load_verified_proposal(s3, proposal_id)
    if fresh_proposal is None or digest(fresh_proposal) != digest(proposal):
        raise RequestError("PROPOSAL_CHANGED", "Proposal evidence changed before submission")
    fresh_approval = load_verified_approval(s3, fresh_proposal)
    if fresh_approval is None or digest(fresh_approval) != digest(approval):
        raise RequestError("APPROVAL_CHANGED", "Approval evidence changed before submission")
    fresh_correction = load_correction(s3, correction_key, correction_binding(fresh_proposal, fresh_approval), fresh_approval)
    if fresh_correction is None or digest(fresh_correction) != digest(correction):
        raise RequestError("CORRECTION_CHANGED", "Correction evidence changed before submission")
    if remaining_millis is not None and remaining_millis() < 35000:
        raise RequestError("RETRY_SAME_PROPOSAL", "Insufficient time to submit safely; retry only the same proposal ID")
    authorized_at = datetime.now(timezone.utc)
    if authorized_at >= datetime.fromisoformat(approval["expires_at"]):
        raise RequestError("PROPOSAL_EXPIRED", "Approval expired during replay checks")
    metadata = {
        **binding, "write-authorized-at": authorized_at.isoformat(),
        "lambda-request-id": invocation_id,
        "source-object-sha256": evidence["object_sha256"],
        "validator-rules-sha256": prediction["validator_rules_sha256"],
        "candidate-digest": prediction["candidate_digest"],
        "predicted-outcome": prediction["predicted_outcome"],
    }
    if not re.fullmatch(r"[A-Za-z0-9-]{1,128}", invocation_id):
        raise RequestError("INVALID_REQUEST", "Invalid invocation identity")
    if sum(len(k.encode("ascii")) + len(v.encode("ascii")) for k, v in metadata.items()) > 2048:
        raise RequestError("REPLAY_TOO_LARGE", "Replay submission metadata exceeds the S3 limit")
    created = True
    try:
        # This is the only submission write. Metadata and event bytes are saved together.
        s3.put_object(Bucket=BUCKET, Key=key, Body=payload, Metadata=metadata,
                      ContentType="application/x-ndjson", ServerSideEncryption="AES256", IfNoneMatch="*")
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in {"PreconditionFailed", "412"}:
            created = False
        elif code in {"AccessDenied", "403"}:
            raise RequestError("REPLAY_WRITE_DENIED", "The replay submission write was denied") from None
        else:
            raise RequestError("RETRY_SAME_PROPOSAL", "Replay write outcome is unresolved; retry only the same proposal ID") from None
    except BotoCoreError:
        raise RequestError("RETRY_SAME_PROPOSAL", "Replay write response was not confirmed; retry only the same proposal ID") from None
    saved = load_replay_submission(s3, key, payload, binding, approval, known_present=True)
    return replay_receipt(saved, key, created, remaining_millis)


def observe_replay_ingestion(proposal_id):
    query = None
    try:
        query = boto3.client("lambda", region_name="us-east-1", config=Config(
            connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1}))
        response = query.invoke(FunctionName=QUERY_FUNCTION, InvocationType="RequestResponse",
                                Payload=canonical_bytes({"operation": "get_replay_ingestion_status", "replay_id": proposal_id}))
        body = read_json_body(response["Payload"])
        if (response.get("FunctionError") or body.get("status") != "ok"
            or body.get("operation") != "get_replay_ingestion_status"
            or body.get("household_id") != HOUSEHOLD or body.get("database_user") != "ring_reader"):
            raise RuntimeError("Invalid ingestion query envelope")
        outcome = checked_ingestion_outcome(body["result"], proposal_id)
        evaluated = datetime.fromisoformat(body["generated_at"])
        if evaluated.utcoffset() is None:
            raise RuntimeError("Invalid query observation time")
        return {"available": True, "observed_outcome": outcome, "queried_at": body["generated_at"],
                "observation": body["result"], "event_content_verified": False}
    except Exception as error:
        logger.warning(json.dumps({"operation": "observe_replay_ingestion", "proposal_id": proposal_id, "error_type": type(error).__name__}))
        return {"available": False, "observed_outcome": "unknown", "error_code": "INGESTION_OBSERVATION_UNAVAILABLE",
                "message": "Submission evidence is separate. The ingestion result could not be verified; read status again using the same proposal ID."}
    finally:
        if query is not None:
            try:
                query.close()
            except Exception:
                logger.warning("Ingestion observation client close failed")


def replay_evidence(s3, proposal, approval, correction_summary=None):
    proposal_id = proposal["proposal_id"]
    key = f"raw/replays/{HOUSEHOLD}/{proposal_id}.jsonl"
    try:
        items = s3.list_objects_v2(Bucket=BUCKET, Prefix=key, MaxKeys=1).get("Contents", [])
    except (ClientError, BotoCoreError):
        raise RequestError("REPLAY_UNAVAILABLE", "Could not check replay evidence") from None
    if not any(item.get("Key") == key for item in items):
        return {"found": False, "submission_status": "not_observed"}
    if approval is None:
        raise RequestError("REPLAY_CONFLICT", "Replay exists without verified approval evidence")
    correction = load_correction(s3, f"recovery/corrections/{HOUSEHOLD}/{proposal_id}.json",
                                 correction_binding(proposal, approval), approval)
    if correction is None:
        raise RequestError("REPLAY_CONFLICT", "Replay exists without correction evidence")
    if correction_summary is not None and (not correction_summary["found"] or correction_summary["correction_sha256"] != digest(correction)):
        raise RequestError("CORRECTION_CHANGED", "Correction evidence changed while reading replay status")
    payload, binding = replay_binding(proposal, approval, correction)
    submission = load_replay_submission(s3, key, payload, binding, approval, known_present=True)
    return {"found": True, "submission_status": "submitted", "storage": {"bucket": BUCKET, "key": key},
            "submission": submission, "ingestion": observe_replay_ingestion(proposal_id)}


def checked_ingestion_outcome(observation, proposal_id):
    """Validate bounded query evidence and derive a conservative observed outcome."""
    expected_key = f"raw/replays/household-001/{proposal_id}.jsonl"
    if not isinstance(observation, dict):
        raise RuntimeError("Missing ingestion observation")
    runs = observation.get("runs")
    total = observation.get("matching_run_count")
    if (
        observation.get("scope") != "ingestion_attempts_for_one_replay_source"
        or observation.get("replay_id") != proposal_id
        or observation.get("source_bucket") != BUCKET or observation.get("source_key") != expected_key
        or observation.get("submission_checked") is not False or observation.get("data_modified") is not False
        or not isinstance(runs, list) or type(total) is not int or total < 0
        or len(runs) != min(total, 50) or observation.get("returned_runs") != len(runs)
        or observation.get("limit") != 50 or type(observation.get("truncated")) is not bool
        or observation["truncated"] != (total > len(runs))
        or observation.get("observation") != ("runs_observed" if total else "no_runs_observed")
    ):
        raise RuntimeError("Unexpected ingestion scope, counts, or limits")
    outcomes = []
    ids = set()
    for run in runs:
        if not isinstance(run, dict) or not isinstance(run.get("run_id"), str) or not run["run_id"] or run["run_id"] in ids:
            raise RuntimeError("Invalid or duplicate ingestion run ID")
        ids.add(run["run_id"])
        counts = tuple(run.get(k) for k in ("input_count", "accepted_count", "duplicate_count", "rejected_count"))
        if any(type(n) is not int or n < 0 for n in counts) or counts[0] != sum(counts[1:]):
            raise RuntimeError("Invalid ingestion run counts")
        started = datetime.fromisoformat(run["started_at"])
        if started.utcoffset() is None:
            raise RuntimeError("Ingestion timestamp needs a timezone")
        status = run.get("status")
        if status == "running":
            if run.get("completed_at") is not None:
                raise RuntimeError("Running run has a completion time")
            expected = "processing"
        else:
            completed = datetime.fromisoformat(run["completed_at"])
            if completed.utcoffset() is None or completed < started:
                raise RuntimeError("Invalid completion timestamp")
            if status == "failed":
                expected = "failed"
            elif status == "completed":
                expected = {(1, 1, 0, 0): "accepted", (1, 0, 1, 0): "duplicate", (1, 0, 0, 1): "rejected"}.get(counts, "unexpected_run_shape")
            else:
                raise RuntimeError("Unknown ingestion status")
        if run.get("record_outcome") != expected:
            raise RuntimeError("Ingestion outcome conflicts with its counts")
        outcomes.append(expected)
    if observation["truncated"]:
        return "indeterminate"
    if not outcomes:
        return "awaiting_ingestion"
    if "unexpected_run_shape" in outcomes or outcomes.count("accepted") > 1 or ("rejected" in outcomes and any(o in outcomes for o in ("accepted", "duplicate"))):
        return "inconsistent"
    if "processing" in outcomes:
        return "processing"
    for outcome in ("accepted", "duplicate", "rejected", "failed"):
        if outcome in outcomes:
            return outcome
    raise RuntimeError("Unrecognized ingestion evidence")


SNAPSHOT_PREFIX = "reports/device-health/household-001/"


def snapshot_timestamp(value):
    try:
        if not isinstance(value, str) or len(value) > 64:
            raise ValueError()
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ValueError("Timestamp must include a valid timezone") from None


def parse_snapshot_request(event):
    if (set(event) - {"operation", "as_of", "silence_minutes", "idempotency_key"}
        or not {"operation", "as_of", "idempotency_key"} <= set(event)):
        raise RequestError("INVALID_REQUEST", "Snapshot requires operation, as_of, idempotency_key and optional silence_minutes")
    try:
        cutoff = snapshot_timestamp(event["as_of"]).isoformat()
    except ValueError:
        raise RequestError("INVALID_REQUEST", "as_of must be an ISO timestamp with a timezone") from None
    silence = event.get("silence_minutes", 10)
    if type(silence) is not int or not 1 <= silence <= 1440:
        raise RequestError("INVALID_REQUEST", "silence_minutes must be an integer from 1 to 1440")
    key = event["idempotency_key"]
    if not isinstance(key, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", key) is None:
        raise RequestError("INVALID_REQUEST", "idempotency_key must be 1..64 letters, digits, underscores or hyphens, starting with a letter or digit")
    return {"operation": "recompute_device_health", "as_of": cutoff, "silence_minutes": silence, "idempotency_key": key}


def snapshot_id(request):
    return digest({"household_id": HOUSEHOLD, "operation": "recompute_device_health", "idempotency_key": request["idempotency_key"]})


def validate_snapshot_health(health, request):
    if (not isinstance(health, dict) or health.get("as_of") != request["as_of"]
        or type(health.get("silence_minutes")) is not int or health["silence_minutes"] != request["silence_minutes"]):
        raise RuntimeError("Health response does not match requested cutoff and threshold")
    if health.get("truncated") is True:
        raise RequestError("HEALTH_RESULT_TRUNCATED", "A partial device list cannot be saved as a complete health snapshot")
    devices = health.get("devices")
    if (health.get("truncated") is not False or not isinstance(devices, list) or len(devices) > 100
        or type(health.get("returned_devices")) is not int or health["returned_devices"] != len(devices)):
        raise RuntimeError("Invalid bounded health response")
    cutoff = snapshot_timestamp(request["as_of"])
    counts = {"reporting": 0, "suspected_offline": 0, "unknown": 0}
    ids = set()
    for device in devices:
        if not isinstance(device, dict):
            raise RuntimeError("Invalid device in health response")
        identifier = device.get("device_id")
        if (not isinstance(identifier, str) or not identifier or identifier in ids
            or device.get("device_type") not in {"camera", "doorbell"}):
            raise RuntimeError("Invalid or duplicate device registration")
        ids.add(identifier)
        registered = snapshot_timestamp(device.get("registered_at"))
        if registered > cutoff:
            raise RuntimeError("Registration exceeds requested cutoff")
        if device.get("last_heartbeat") is None:
            if (device.get("health_status") != "unknown" or device.get("minutes_since_heartbeat") is not None
                or any(device.get(k) is not None for k in ("event_id", "battery_percent", "received_at", "run_id"))):
                raise RuntimeError("Unknown health contradicts heartbeat evidence")
            status = "unknown"
        else:
            event_time = snapshot_timestamp(device["last_heartbeat"])
            age = (cutoff - event_time).total_seconds()
            status = "suspected_offline" if age > request["silence_minutes"] * 60 else "reporting"
            if (event_time < registered or age < 0
                or type(device.get("battery_percent")) is not int or not 0 <= device["battery_percent"] <= 100
                or any(not isinstance(device.get(k), str) or not device[k] for k in ("event_id", "run_id"))
                or device.get("health_status") != status
                or type(device.get("minutes_since_heartbeat")) not in {int, float}
                or device["minutes_since_heartbeat"] != round(age / 60, 2)):
                raise RuntimeError("Health result contradicts heartbeat evidence")
            snapshot_timestamp(device.get("received_at"))
        counts[status] += 1
    return counts


def load_health_snapshot(s3, request):
    key = SNAPSHOT_PREFIX + snapshot_id(request) + ".json"
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    document = read_json_body(response["Body"])
    saved = document.get("snapshot")
    if (not isinstance(saved, dict) or document.get("snapshot_sha256") != digest(saved)
        or saved.get("snapshot_version") != 1 or saved.get("snapshot_id") != snapshot_id(request)
        or saved.get("household_id") != HOUSEHOLD):
        raise RuntimeError("Saved snapshot content or scope is invalid")
    if saved.get("request") != request:
        raise RequestError("IDEMPOTENCY_CONFLICT", "This key already belongs to a different snapshot request; it was not overwritten")
    if (saved.get("data_basis") != "currently_stored_accepted_events_including_late_arrivals_and_replays"
        or saved.get("database_user") != "ring_reader"
        or saved.get("summary") != validate_snapshot_health(saved.get("health"), request)
        or not isinstance(saved.get("lambda_request_id"), str)):
        raise RuntimeError("Saved snapshot evidence is invalid")
    snapshot_timestamp(saved.get("queried_at"))
    snapshot_timestamp(saved.get("prepared_at"))
    return document


def snapshot_receipt(document, created, recomputed=False):
    saved = document["snapshot"]
    return {"snapshot_status": "saved", "created": created, "persisted": True,
            "snapshot": saved, "snapshot_sha256": document["snapshot_sha256"],
            "storage": {"bucket": BUCKET, "key": SNAPSHOT_PREFIX + saved["snapshot_id"] + ".json"},
            "database_modified": False, "replayed": False, "recomputed_this_request": recomputed,
            "message": "Health report saved in S3. Existing identical retries return the original observation; use a new key only for an intentional fresh report."}


def recompute_device_health(s3, query_client, request, invocation_id):
    existing = load_health_snapshot(s3, request)
    if existing is not None:
        return snapshot_receipt(existing, False)
    response = query_client.invoke(FunctionName=QUERY_FUNCTION, InvocationType="RequestResponse",
                                   Payload=canonical_bytes({"operation": "get_device_health", "as_of": request["as_of"],
                                                            "silence_minutes": request["silence_minutes"]}))
    body = read_json_body(response["Payload"])
    if (response.get("FunctionError") or body.get("status") != "ok" or body.get("operation") != "get_device_health"
        or body.get("household_id") != HOUSEHOLD or body.get("database_user") != "ring_reader"):
        raise RuntimeError("Unexpected health query response or identity")
    summary = validate_snapshot_health(body.get("result"), request)
    snapshot_timestamp(body.get("generated_at"))
    saved = {"snapshot_version": 1, "snapshot_id": snapshot_id(request), "household_id": HOUSEHOLD,
             "request": request, "queried_at": body["generated_at"], "prepared_at": datetime.now(timezone.utc).isoformat(),
             "database_user": "ring_reader", "lambda_request_id": invocation_id,
             "data_basis": "currently_stored_accepted_events_including_late_arrivals_and_replays",
             "summary": summary, "health": body["result"]}
    document = {"snapshot": saved, "snapshot_sha256": digest(saved)}
    payload = canonical_bytes(document)
    if len(payload) > 524288:
        raise RuntimeError("Snapshot exceeds storage size limit")
    try:
        s3.put_object(Bucket=BUCKET, Key=SNAPSHOT_PREFIX + saved["snapshot_id"] + ".json",
                      Body=payload, ContentType="application/json", ServerSideEncryption="AES256", IfNoneMatch="*")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
            raise
        existing = load_health_snapshot(s3, request)
        if existing is None:
            raise RequestError("SNAPSHOT_RETRY_REQUIRED", "Concurrent write outcome is unresolved; retry the identical request and key") from None
        return snapshot_receipt(existing, False, True)
    return snapshot_receipt(document, True, True)


def validate_quality_results(result, arguments):
    if (not isinstance(result, dict) or result.get("scope") != "consistency_of_selected_completed_ingestion_runs"
        or result.get("time_basis") != "run_started_at" or result.get("window_bounds") != "start_inclusive_end_exclusive"
        or result.get("persisted") is not False or result.get("data_modified") is not False):
        raise RuntimeError("Invalid quality scope or write state")
    for name in ("start_time", "end_time"):
        if snapshot_timestamp(result.get(name)) != snapshot_timestamp(arguments[name]):
            raise RuntimeError("Quality window does not match request")
    limit, count = result.get("run_limit"), result.get("selected_runs")
    if (type(limit) is not int or not 1 <= limit <= 200 or limit != arguments.get("run_limit", 100)
        or type(count) is not int or not 0 <= count <= limit or type(result.get("truncated")) is not bool
        or (result["truncated"] and count != limit)):
        raise RuntimeError("Invalid quality selection size")
    states = result.get("run_states")
    if (not isinstance(states, dict) or set(states) != {"completed", "running", "failed"}
        or any(type(v) is not int or v < 0 for v in states.values()) or sum(states.values()) != count
        or result.get("operational_attention_required") is not bool(states["running"] or states["failed"])):
        raise RuntimeError("Invalid quality run states")
    checks = result.get("checks")
    expected_ids = {"run_count_balance", "accepted_row_count", "rejected_row_count", "accepted_event_validity"}
    if (not isinstance(checks, list) or len(checks) != 4 or any(not isinstance(c, dict) for c in checks)
        or {c.get("check_id") for c in checks} != expected_ids):
        raise RuntimeError("Missing quality checks")
    for check in checks:
        failures, examples = check.get("affected_runs"), check.get("examples")
        if (type(failures) is not int or not 0 <= failures <= states["completed"]
            or check.get("examined_runs") != states["completed"] or not isinstance(examples, list)
            or len(examples) != min(failures, 5) or check.get("examples_truncated") is not (failures > 5)
            or check.get("status") != ("fail" if failures else "pass" if states["completed"] else "not_evaluated")):
            raise RuntimeError("Invalid quality check evidence")
        for example in examples:
            if (not isinstance(example, dict) or any(not isinstance(example.get(k), str) for k in ("run_id", "source_key"))
                or any(type(example.get(k)) is not int or example[k] < 0 for k in ("expected", "observed"))
                or example["expected"] == example["observed"]):
                raise RuntimeError("Invalid quality failure example")
    totals = result.get("completed_run_totals")
    if (not isinstance(totals, dict) or set(totals) != {"input_count", "accepted_count", "duplicate_count", "rejected_count", "stored_accepted_count", "stored_rejected_count"}
        or any(type(v) is not int or v < 0 for v in totals.values())):
        raise RuntimeError("Invalid quality totals")
    overall = ("fail" if any(c["status"] == "fail" for c in checks) else "incomplete" if result["truncated"] else
               "not_evaluated" if not states["completed"] else "pass")
    if result.get("overall_status") != overall:
        raise RuntimeError("Quality verdict contradicts its evidence")


# Inlined into recovery_lambda.py for a single-file Lambda deployment.
QUALITY_PREFIX = "reports/data-quality/household-001/"


def parse_quality_report_request(event):
    required = {"operation", "start_time", "end_time", "idempotency_key"}
    if set(event) - (required | {"run_limit"}) or not required <= set(event):
        raise RequestError("INVALID_REQUEST", "Quality report requires operation, start_time, end_time, idempotency_key and optional run_limit")
    try:
        start, end = snapshot_timestamp(event["start_time"]), snapshot_timestamp(event["end_time"])
    except ValueError:
        raise RequestError("INVALID_REQUEST", "Report timestamps must include a timezone") from None
    if not timedelta(0) < end - start <= timedelta(days=31):
        raise RequestError("INVALID_REQUEST", "Report window must be positive and at most 31 days")
    limit = event.get("run_limit", 100)
    if type(limit) is not int or not 1 <= limit <= 200:
        raise RequestError("INVALID_REQUEST", "run_limit must be an integer from 1 to 200")
    key = event["idempotency_key"]
    if not isinstance(key, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", key) is None:
        raise RequestError("INVALID_REQUEST", "idempotency_key must be 1..64 letters, digits, underscores or hyphens, starting with a letter or digit")
    return {"operation": "save_quality_report", "start_time": start.isoformat(), "end_time": end.isoformat(),
            "run_limit": limit, "idempotency_key": key}


def quality_report_id(request):
    return digest({"household_id": HOUSEHOLD, "operation": "save_quality_report", "idempotency_key": request["idempotency_key"]})


def validate_quality_document(document, request):
    saved = document.get("report")
    if (not isinstance(saved, dict) or document.get("report_sha256") != digest(saved)
        or saved.get("report_version") != 1 or saved.get("report_id") != quality_report_id(request)
        or saved.get("household_id") != HOUSEHOLD):
        raise RuntimeError("Saved quality report fingerprint or scope is invalid")
    if saved.get("request") != request:
        raise RequestError("IDEMPOTENCY_CONFLICT", "This key belongs to a different quality report request; it was not overwritten")
    if saved.get("database_user") != "ring_reader" or not isinstance(saved.get("lambda_request_id"), str) or not saved["lambda_request_id"]:
        raise RuntimeError("Saved quality report identity metadata is invalid")
    snapshot_timestamp(saved.get("query_generated_at"))
    snapshot_timestamp(saved.get("prepared_at"))
    validate_quality_results(saved.get("quality"), request)
    quality = saved["quality"]
    if not isinstance(quality.get("limitations"), list) or not quality["limitations"]:
        raise RuntimeError("Quality report lacks scope limitations")
    totals = quality["completed_run_totals"]
    checks = {c["check_id"]: c for c in quality["checks"]}
    # Fail reports are valid evidence. Reject contradictions, not failed checks.
    for check_id, expected, observed in (
        ("run_count_balance", totals["input_count"], totals["accepted_count"] + totals["duplicate_count"] + totals["rejected_count"]),
        ("accepted_row_count", totals["accepted_count"], totals["stored_accepted_count"]),
        ("rejected_row_count", totals["rejected_count"], totals["stored_rejected_count"]),
    ):
        if checks[check_id]["status"] == "pass" and expected != observed:
            raise RuntimeError("Passing quality check contradicts report totals")


def load_quality_report(s3, request):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=QUALITY_PREFIX + quality_report_id(request) + ".json")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    document = read_json_body(response["Body"])
    validate_quality_document(document, request)
    return document


def quality_report_receipt(document, created, queried=False):
    saved = document["report"]
    return {"report_status": "saved", "created": created, "persisted": True,
            "report": saved, "report_sha256": document["report_sha256"],
            "storage": {"bucket": BUCKET, "key": QUALITY_PREFIX + saved["report_id"] + ".json"},
            "database_modified": False, "replayed": False, "queried_this_request": queried,
            "message": "Quality report saved, including its original verdict and coverage. Identical retries return the original observation. Use a new key only for an intentional fresh report."}


def save_quality_report(s3, query_client, request, invocation_id):
    existing = load_quality_report(s3, request)
    if existing is not None:
        return quality_report_receipt(existing, False)
    response = query_client.invoke(FunctionName=QUERY_FUNCTION, InvocationType="RequestResponse",
        Payload=canonical_bytes({"operation": "get_quality_results", "start_time": request["start_time"],
                                 "end_time": request["end_time"], "run_limit": request["run_limit"]}))
    body = read_json_body(response["Payload"])
    if (response.get("FunctionError") or body.get("status") != "ok" or body.get("operation") != "get_quality_results"
        or body.get("household_id") != HOUSEHOLD or body.get("database_user") != "ring_reader"):
        raise RuntimeError("Unexpected quality query response or identity")
    saved = {"report_version": 1, "report_id": quality_report_id(request), "household_id": HOUSEHOLD,
             "request": request, "query_generated_at": body["generated_at"],
             "prepared_at": datetime.now(timezone.utc).isoformat(), "database_user": "ring_reader",
             "lambda_request_id": invocation_id, "quality": body["result"]}
    document = {"report": saved, "report_sha256": digest(saved)}
    validate_quality_document(document, request)
    payload = canonical_bytes(document)
    if len(payload) > 524288:
        raise RuntimeError("Quality report exceeds storage size limit")
    try:
        s3.put_object(Bucket=BUCKET, Key=QUALITY_PREFIX + saved["report_id"] + ".json",
                      Body=payload, ContentType="application/json", ServerSideEncryption="AES256", IfNoneMatch="*")
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
            raise
        existing = load_quality_report(s3, request)
        if existing is None:
            raise RequestError("REPORT_RETRY_REQUIRED", "Concurrent write outcome unresolved; retry the identical request and key") from None
        return quality_report_receipt(existing, False, True)
    return quality_report_receipt(document, True, True)



def lambda_handler(event, context):
    query_client = None
    s3 = None
    try:
        request = parse_request(event)
        if request["operation"] == "save_quality_report":
            s3 = boto3.client("s3", region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1}))
            query_client = boto3.client("lambda", region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=40, retries={"total_max_attempts": 1}))
            result = save_quality_report(s3, query_client, request, context.aws_request_id)
            logger.info(json.dumps({"operation": request["operation"], "report_id": result["report"]["report_id"],
                                    "created": result["created"], "request_id": context.aws_request_id}))
            return {"status": "ok", "operation": request["operation"], "household_id": HOUSEHOLD, "result": result}
        if request["operation"] == "recompute_device_health":
            s3 = boto3.client("s3", region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1}))
            query_client = boto3.client("lambda", region_name="us-east-1", config=Config(connect_timeout=5, read_timeout=40, retries={"total_max_attempts": 1}))
            result = recompute_device_health(s3, query_client, request, context.aws_request_id)
            logger.info(json.dumps({"operation": request["operation"], "snapshot_id": result["snapshot"]["snapshot_id"],
                                    "created": result["created"], "request_id": context.aws_request_id}))
            return {"status": "ok", "operation": request["operation"], "household_id": HOUSEHOLD, "result": result}
        if request["operation"] in {"get_operation_status", "get_audit_history", "preview_replay", "apply_record_correction", "replay_records"}:
            s3 = boto3.client(
                "s3", region_name="us-east-1",
                config=Config(connect_timeout=5, read_timeout=10, retries={"mode": "standard", "total_max_attempts": 1 if request["operation"] in {"apply_record_correction", "replay_records"} else 3}),
            )
            if request["operation"] == "get_operation_status":
                result = get_operation_status(s3, request["proposal_id"])
            elif request["operation"] == "get_audit_history":
                result = get_audit_history(s3, request["proposal_id"])
            elif request["operation"] == "preview_replay":
                result = preview_saved_replay(s3, request["proposal_id"])
            elif request["operation"] == "replay_records":
                result = replay_records(s3, request["proposal_id"], context.aws_request_id, context.get_remaining_time_in_millis)
            else:
                result = apply_record_correction(s3, request["proposal_id"], context.aws_request_id)
            logger.info(json.dumps({
                "operation": request["operation"], "proposal_id": request["proposal_id"],
                "found": result.get("found"), "created": result.get("created"), "request_id": context.aws_request_id,
            }))
            return {
                "status": "ok", "operation": request["operation"],
                "household_id": HOUSEHOLD, "result": result,
            }

        query_client = boto3.client(
            "lambda", region_name="us-east-1",
            config=Config(connect_timeout=5, read_timeout=40, retries={"total_max_attempts": 1}),
        )
        preview = obtain_preview(query_client, request)
        s3 = boto3.client(
            "s3", region_name="us-east-1",
            config=Config(connect_timeout=5, read_timeout=10, retries={"mode": "standard", "total_max_attempts": 3}),
        )
        result = save_proposal(s3, request, preview, context.aws_request_id)
        logger.info(json.dumps({
            "operation": OPERATION, "proposal_id": result["proposal"]["proposal_id"],
            "created": result["created"], "request_id": context.aws_request_id,
        }))
        return {"status": "ok", "operation": OPERATION, "household_id": HOUSEHOLD, "result": result}
    except RequestError as error:
        return {"status": "error", "error_code": error.code, "message": str(error)}
    except Exception as error:
        logger.error(json.dumps({"error_type": type(error).__name__, "request_id": context.aws_request_id}))
        raise RuntimeError("Recovery operation failed; check permissions and CloudWatch logs") from None
    finally:
        for client in (query_client, s3):
            if client is not None:
                client.close()
