import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


PROJECT_DIR = Path(__file__).resolve().parents[2]
EXPIRED_PROPOSAL_ID = "9f36517d8d3f3b4f113bb686ea993c2eaa70948f552d02df7733cd558fe36f29"
APPROVED_PROPOSAL_ID = "3e52eb3ec762d3b5fac3a9323caa890369b0b0bb425d6dab9750f55f0505d08f"
APPROVED_PROPOSAL_SHA256 = "52491473ca8c0bd990179b31cb982f9491d4cf9725e46369f57ea1d585f2f5e6"
APPROVAL_SHA256 = "289db59ca3bf6d742affc5d74bc0ad6e675f198105b78e57af4ba8d42bfc9d0d"
APPROVER = {
    "account_id": "455646290306",
    "arn": "arn:aws:iam::455646290306:user/ring-project-approver",
    "user_id": "AIDAWUFVCUGBNXT6GDULH",
}
REPLAY_PROPOSAL_ID = os.environ.get(
    "RING_REPLAY_PROPOSAL_ID",
    "188423d9c8a25fe5e19e256b7e03369d78172a388016f501980c625a0b2bda5c",
)


async def main():
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[str(PROJECT_DIR / "aws_mcp_server.py")],
        env={
            **os.environ,
            "RING_AWS_PROFILE": "ring-mcp",
        },
    )

    async with stdio_client(parameters) as (read, write):
        async with ClientSession(
            read,
            write,
            read_timeout_seconds=timedelta(seconds=120),
        ) as session:
            await session.initialize()

            discovered = await session.list_tools()
            names = {tool.name for tool in discovered.tools}

            expected = {
                "get_daily_device_metrics",
                "get_device_health",
                "get_device_details",
                "get_event_history",
                "get_quality_results",
                "recompute_device_health",
                "get_pipeline_status",
                "get_rejection_summary",
                "list_rejected_records",
                "inspect_rejected_record",
                "preview_record_correction",
                "propose_record_correction",
                "get_operation_status",
                "get_audit_history",
                "preview_replay",
                "apply_record_correction",
                "replay_records",
            }

            if names != expected:
                raise AssertionError(f"Unexpected tools: {names}")

            replay_tool = next(tool for tool in discovered.tools if tool.name == "preview_replay")
            if (
                set(replay_tool.inputSchema.get("properties", {})) != {"proposal_id"}
                or replay_tool.inputSchema.get("required") != ["proposal_id"]
                or replay_tool.annotations is None
                or replay_tool.annotations.readOnlyHint is not True
            ):
                raise AssertionError("Unexpected replay tool inputs or annotations")
            submit_tool = next(tool for tool in discovered.tools if tool.name == "replay_records")
            if (set(submit_tool.inputSchema.get("properties", {})) != {"proposal_id"}
                or submit_tool.inputSchema.get("required") != ["proposal_id"]
                or submit_tool.annotations is None or submit_tool.annotations.readOnlyHint is not False
                or submit_tool.annotations.idempotentHint is not True):
                raise AssertionError("Unexpected replay submission schema or annotations")
            details_tool = next(t for t in discovered.tools if t.name == "get_device_details")
            assert set(details_tool.inputSchema["properties"]) == {"device_id", "as_of", "silence_minutes"}
            assert details_tool.inputSchema["required"] == ["device_id"]
            assert details_tool.annotations.readOnlyHint is True
            history_tool = next(t for t in discovered.tools if t.name == "get_event_history")
            assert set(history_tool.inputSchema["properties"]) == {"device_id", "start_time", "end_time", "limit"}
            assert set(history_tool.inputSchema["required"]) == {"device_id", "start_time", "end_time"}
            assert history_tool.annotations.readOnlyHint is True
            quality_tool = next(t for t in discovered.tools if t.name == "get_quality_results")
            assert set(quality_tool.inputSchema["properties"]) == {"start_time", "end_time", "run_limit"}
            assert set(quality_tool.inputSchema["required"]) == {"start_time", "end_time"}
            assert quality_tool.annotations.readOnlyHint is True
            snapshot_tool = next(t for t in discovered.tools if t.name == "recompute_device_health")
            assert set(snapshot_tool.inputSchema["properties"]) == {"as_of", "idempotency_key", "silence_minutes"}
            assert set(snapshot_tool.inputSchema["required"]) == {"as_of", "idempotency_key"}
            assert snapshot_tool.annotations.readOnlyHint is False
            assert snapshot_tool.annotations.idempotentHint is True
            print("PASS: discovered all seventeen MCP tools")
            print("NOT EXERCISED: snapshot persistence; use the dedicated snapshot test when intentionally saving a report.")

            calls = [
                ("get_daily_device_metrics", {"report_day": "2026-09-18"}),
                ("get_daily_device_metrics", {"report_day": "2026-09-17"}),
                ("get_daily_device_metrics", {"report_day": "2026-09-18", "device_limit": 1}),
                ("get_quality_results", {"start_time": "2026-09-18T00:00:00Z", "end_time": "2026-09-20T00:00:00Z", "run_limit": 100}),
                ("get_quality_results", {"start_time": "2026-09-18T00:00:00Z", "end_time": "2026-09-20T00:00:00Z", "run_limit": 1}),
                ("get_event_history", {"device_id": "camera-002", "start_time": "2026-09-18T12:00:00Z", "end_time": "2026-09-18T12:01:00Z", "limit": 20}),
                ("get_event_history", {"device_id": "camera-002", "start_time": "2026-09-18T11:59:00Z", "end_time": "2026-09-18T12:00:00Z", "limit": 20}),
                ("get_event_history", {"device_id": "missing-device-history-test", "start_time": "2026-09-18T12:00:00Z", "end_time": "2026-09-18T12:01:00Z", "limit": 20}),
                ("get_event_history", {"device_id": "camera-001", "start_time": "2026-09-01T00:00:00Z", "end_time": "2026-09-19T00:00:00Z", "limit": 1}),
                ("get_device_details", {"device_id": "camera-002", "as_of": "2026-09-18T12:05:00Z", "silence_minutes": 10}),
                ("get_device_details", {"device_id": "camera-002", "as_of": "2026-09-18T11:59:00Z"}),
                ("get_device_details", {"device_id": "camera-002", "as_of": "2026-09-01T08:59:00Z"}),
                ("get_device_details", {"device_id": "missing-device-details-test", "as_of": "2026-09-18T12:05:00Z"}),
                ("get_operation_status", {"proposal_id": APPROVED_PROPOSAL_ID}),
                ("get_audit_history", {"proposal_id": APPROVED_PROPOSAL_ID}),
                ("get_audit_history", {"proposal_id": APPROVED_PROPOSAL_ID}),
                ("preview_replay", {"proposal_id": APPROVED_PROPOSAL_ID}),
                ("preview_replay", {"proposal_id": EXPIRED_PROPOSAL_ID}),
                ("preview_replay", {"proposal_id": REPLAY_PROPOSAL_ID}),
                (
                    "get_device_health",
                    {
                        "as_of": "2026-09-18T12:05:00Z",
                        "silence_minutes": 10,
                    },
                ),
                (
                    "get_operation_status",
                    {"proposal_id": "9f36517d8d3f3b4f113bb686ea993c2eaa70948f552d02df7733cd558fe36f29"},
                ),
                (
                    "get_audit_history",
                    {"proposal_id": "9f36517d8d3f3b4f113bb686ea993c2eaa70948f552d02df7733cd558fe36f29"},
                ),
                (
                    "get_audit_history",
                    {"proposal_id": "9f36517d8d3f3b4f113bb686ea993c2eaa70948f552d02df7733cd558fe36f29"},
                ),
                ("get_pipeline_status", {}),
                ("get_rejection_summary", {}),
                (
                    "list_rejected_records",
                    {"limit": 20, "error_code": "INVALID_BATTERY"},
                ),
                (
                    "list_rejected_records",
                    {"limit": 1, "error_code": "INVALID_BATTERY"},
                ),
                ("inspect_rejected_record", {"rejection_id": 6}),
                ("inspect_rejected_record", {"rejection_id": 5}),
                ("inspect_rejected_record", {"rejection_id": 2}),
                ("inspect_rejected_record", {"rejection_id": 999999}),
                (
                    "preview_record_correction",
                    {
                        "rejection_id": 6,
                        "battery_percent": 75,
                        "reason": "Synthetic demo: explicitly choose 75 as the replacement value.",
                    },
                ),
                (
                    "preview_record_correction",
                    {
                        "rejection_id": 5,
                        "battery_percent": 75,
                        "reason": "Test the household restriction.",
                    },
                ),
                ("inspect_rejected_record", {"rejection_id": 6}),
            ]

            audit_by_proposal = {}
            for name, arguments in calls:
                result = await session.call_tool(name, arguments)

                if result.isError:
                    raise AssertionError(
                        f"{name} failed: {result.content}"
                    )

                text = next(
                    block.text
                    for block in result.content
                    if block.type == "text"
                )
                body = json.loads(text)

                if (
                    body.get("operation") != name
                    or body.get("status") != "ok"
                    or body.get("backend") != "aws"
                    or body.get("household_id") != "household-001"
                    or (
                        name not in {"get_operation_status", "get_audit_history", "preview_replay"}
                        and body.get("database_user") != "ring_reader"
                    )
                ):
                    raise AssertionError(f"Unexpected response: {body}")

                if name == "get_quality_results":
                    quality = body["result"]
                    assert quality["data_modified"] is False and quality["persisted"] is False
                    if arguments["run_limit"] == 1:
                        assert quality["truncated"] is True
                        assert quality["overall_status"] in {"incomplete", "fail"}
                    else:
                        assert quality["truncated"] is False
                        assert quality["overall_status"] == "pass", quality
                        assert quality["completed_run_totals"]["rejected_count"] >= 6
                        assert quality["completed_run_totals"]["accepted_count"] == quality["completed_run_totals"]["stored_accepted_count"]
                        assert quality["completed_run_totals"]["rejected_count"] == quality["completed_run_totals"]["stored_rejected_count"]
                    print("PASS: bounded quality consistency checks", arguments)

                if name == "get_event_history":
                    history = body["result"]
                    if arguments["device_id"] == "missing-device-history-test":
                        assert history["device_found"] is False and history["records"] == []
                    elif arguments["device_id"] == "camera-001":
                        assert history["truncated"] is True and history["returned_records"] == 1
                        assert history["records"][0]["event_id"] == "evt-batch-002"
                    elif arguments["end_time"] == "2026-09-18T12:00:00Z":
                        assert history["device_found"] is True and history["records"] == []
                    else:
                        assert history["device_found"] is True and history["returned_records"] == 1
                        heartbeat = history["records"][0]
                        assert heartbeat["event_id"] == "evt-batch-003" and heartbeat["battery_percent"] == 75
                        assert heartbeat["source_key"] == "raw/replays/household-001/744e5f5447a1df812bdd323a2df276662ffac250f0a4f3d92dc65a5238480f19.jsonl"
                        assert history["truncated"] is False
                    print("PASS: accepted event history and window boundaries", arguments)

                if name == "get_device_details":
                    details = body["result"]
                    if arguments["device_id"] == "missing-device-details-test" or arguments["as_of"] == "2026-09-01T08:59:00Z":
                        assert details["found"] is False
                        assert details["device"] is None and details["latest_heartbeat"] is None
                    elif arguments["as_of"] == "2026-09-18T11:59:00Z":
                        assert details["found"] is True
                        assert details["latest_heartbeat"] is None
                        assert details["health"]["status"] == "unknown"
                    else:
                        assert details["found"] is True
                        heartbeat = details["latest_heartbeat"]
                        assert heartbeat["event_id"] == "evt-batch-003"
                        assert heartbeat["battery_percent"] == 75
                        assert heartbeat["run_status"] == "completed"
                        assert heartbeat["source_key"] == "raw/replays/household-001/744e5f5447a1df812bdd323a2df276662ffac250f0a4f3d92dc65a5238480f19.jsonl"
                        assert details["health"] == {"status": "reporting", "minutes_since_heartbeat": 5}
                    print("PASS: device details, cutoff and lineage", arguments)

                if name == "preview_replay":
                    replay = body["result"]
                    if (
                        replay.get("found") is not True
                        or replay["proposal_id"] != arguments["proposal_id"]
                        or replay["scope"] != "saved_proposal_replay_preview"
                        or replay["can_execute"] is not False
                        or replay["data_modified"] is not False
                        or replay["replay_performed"] is not False
                        or replay["source_revalidated"] is not False
                        or replay["approval_tracking"] != "implemented"
                        or replay["execution_tracking"] != "correction_and_replay"
                    ):
                        raise AssertionError("Unexpected saved replay preview state")
                    checked = datetime.fromisoformat(replay["checked_at"])
                    expiry = datetime.fromisoformat(replay["expires_at"])
                    if checked >= expiry:
                        if (
                            replay["preview_status"] != "blocked"
                            or replay["proposal_status"] != "expired"
                            or replay["block_reason"] != "PROPOSAL_EXPIRED"
                        ):
                            raise AssertionError("Expired proposal was not blocked")
                        print("PASS: expired replay proposal blocked")
                        if arguments["proposal_id"] == REPLAY_PROPOSAL_ID:
                            print("NOT EXERCISED: active-proposal prediction; this proposal has expired.")
                    else:
                        prediction = replay["prediction"]
                        if (
                            replay["preview_status"] != "completed"
                            or replay["proposal_status"] != ("approved" if replay["approval"]["found"] else "pending_approval")
                            or prediction["predicted_outcome"] != "accepted"
                            or prediction["candidate_digest"] != "f7a6502bedb6066e49c7a179bc0cd22cd0a4395f6e8e60db490779a6ef12ceec"
                            or prediction["authorization_checked"] is not False
                            or prediction["data_modified"] is not False
                            or prediction["replay_performed"] is not False
                        ):
                            raise AssertionError("Unexpected prediction for the battery-75 demo proposal")
                        print("PASS: saved candidate predicted accepted without execution")

                if name == "list_rejected_records":
                    listing = body["result"]
                    records = listing["records"]
                    if (
                        listing["error_code_filter"] != "INVALID_BATTERY"
                        or listing["limit"] != arguments["limit"]
                        or listing["returned_records"] != len(records)
                        or len(records) > arguments["limit"]
                        or any(
                            record["error_code"] != "INVALID_BATTERY"
                            for record in records
                        )
                    ):
                        raise AssertionError("Rejection filter or limit is incorrect")
                    if arguments["limit"] == 20:
                        if len(records) < 2:
                            raise AssertionError(
                                "This test needs at least two INVALID_BATTERY rejections"
                            )
                        newest_rejection_id = records[0]["rejection_id"]
                    elif (
                        len(records) != 1
                        or listing["truncated"] is not True
                        or records[0]["rejection_id"] != newest_rejection_id
                    ):
                        raise AssertionError("Limited listing or truncation is incorrect")

                if name == "inspect_rejected_record":
                    inspection = body["result"]
                    rejection_id = arguments["rejection_id"]
                    if rejection_id == 999999:
                        if inspection != {"found": False, "rejection_id": rejection_id}:
                            raise AssertionError("Expected missing rejection ID 999999")
                    else:
                        if inspection.get("found") is not True:
                            raise AssertionError(f"Missing seeded rejection {rejection_id}")
                        record = inspection["record"]
                        if record["rejection_id"] != rejection_id:
                            raise AssertionError("Wrong rejection returned")
                        if rejection_id == 6:
                            if (
                                record["raw_content_status"] != "included"
                                or record["error_code"] != "INVALID_BATTERY"
                                or record["parsed_record"]["household_id"] != "household-001"
                                or record["parsed_record"]["battery_percent"] != 150
                                or json.loads(record["raw_record"]) != record["parsed_record"]
                            ):
                                raise AssertionError("Unexpected battery rejection content")
                        elif (
                            record["raw_content_status"] != "withheld"
                            or record["raw_record"] is not None
                            or record["parsed_record"] is not None
                            or not record["withheld_reason"]
                        ):
                            raise AssertionError("Unverifiable raw content was not withheld")

                if name == "preview_record_correction":
                    preview_result = body["result"]
                    if arguments["rejection_id"] == 5:
                        if (
                            preview_result["preview_status"] != "unavailable"
                            or preview_result["rejection_id"] != 5
                            or "preview" in preview_result
                        ):
                            raise AssertionError("Other-household preview was not blocked")
                    else:
                        if preview_result["preview_status"] != "ready":
                            raise AssertionError("Expected a ready battery correction preview")
                        preview = preview_result["preview"]
                        original = preview["original_record"]
                        proposed = preview["proposed_record"]
                        if (
                            preview["household_id"] != "household-001"
                            or preview["rejection_id"] != 6
                            or original["event_id"] != "evt-batch-003"
                            or original["battery_percent"] != 150
                            or proposed != {**original, "battery_percent": 75}
                            or preview["reason"] != arguments["reason"]
                            or preview["changes"] != [{
                                "field": "battery_percent", "before": 150, "after": 75,
                            }]
                            or preview_result["persisted"] is not False
                            or preview_result["approval_status"] != "not_requested"
                            or preview_result["replay_validation"] != "not_performed"
                        ):
                            raise AssertionError("Unexpected correction preview content or state")
                        canonical = json.dumps(
                            preview, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=True, allow_nan=False,
                        )
                        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                        if preview_result["preview_sha256"] != digest:
                            raise AssertionError("Preview fingerprint does not match its content")

                if name == "get_operation_status":
                    status = body["result"]
                    if status.get("found") is not True:
                        raise AssertionError("Expected the saved demo proposal to exist")
                    checked = datetime.fromisoformat(status["checked_at"])
                    expiry = datetime.fromisoformat(status["expires_at"])
                    has_approval = arguments["proposal_id"] == APPROVED_PROPOSAL_ID
                    expected_status = "expired" if checked >= expiry else ("approved" if has_approval else "pending_approval")
                    expected_hash = APPROVED_PROPOSAL_SHA256 if has_approval else "baeb4795d220a96d218e7a3becfb669cb2766606ec65543b873ab6e2a82be9c3"
                    approval = status["approval"]
                    if status.get("correction") != {"found": False, "application_status": "not_applied"}:
                        raise AssertionError("Unexpected correction artifact for the original test fixture")
                    if has_approval:
                        if (
                            approval["found"] is not True
                            or approval["operator"] != APPROVER
                            or approval["approval_sha256"] != APPROVAL_SHA256
                            or approval["approved_at"] != "2026-09-19T18:56:44.793372+00:00"
                            or approval["recorded_decision"] != "approved"
                            or approval["status"] != ("expired" if checked >= expiry else "approved")
                        ):
                            raise AssertionError("Saved operator approval evidence is incorrect")
                    elif approval != {"found": False, "status": "not_recorded"}:
                        raise AssertionError("Unexpected approval for the unapproved fixture")
                    if (
                        status["proposal_id"] != arguments["proposal_id"]
                        or status["proposal_status"] != expected_status
                        or status["seconds_until_expiry"] != max(0, int((expiry - checked).total_seconds()))
                        or status["scope"] != "proposal_approval_correction_and_replay"
                        or status["stored_approval_status"] != "pending"
                        or status["can_execute"] is not False
                        or status["approval_tracking"] != "implemented"
                        or status["execution_tracking"] != "correction_and_replay"
                        or status["proposal_sha256"] != expected_hash
                        or status["preview_sha256"] != "c977c2ec6721a127f9f6e1460c4baeec8a26867bd389449ad16f691f00594b75"
                    ):
                        raise AssertionError("Proposal status, expiry, or fingerprint is incorrect")

                if name == "get_audit_history":
                    history = body["result"]
                    has_approval = arguments["proposal_id"] == APPROVED_PROPOSAL_ID
                    expected_count = 2 if has_approval else 1
                    expected_hash = APPROVED_PROPOSAL_SHA256 if has_approval else "baeb4795d220a96d218e7a3becfb669cb2766606ec65543b873ab6e2a82be9c3"
                    if (
                        history.get("found") is not True
                        or history["proposal_id"] != arguments["proposal_id"]
                        or history["scope"] != "proposal_approval_correction_and_replay"
                        or history["history_complete"] is not False
                        or history["returned_events"] != expected_count
                        or len(history["events"]) != expected_count
                        or history["proposal_sha256"] != expected_hash
                        or history["coverage"]["approval_tracking"] != "implemented"
                    ):
                        raise AssertionError("Unexpected audit coverage or saved proposal")
                    creation = history["events"][0]
                    if (
                        creation["event_type"] != "proposal_created"
                        or creation["actor_identity"] is not None
                        or creation["approval_status_at_creation"] != "pending"
                        or creation["rejection_id"] != 6
                        or creation["changes"] != [{"field": "battery_percent", "before": 150, "after": 75}]
                    ):
                        raise AssertionError("Creation evidence was changed")
                    if has_approval:
                        approval = history["events"][1]
                        if (
                            approval["event_type"] != "proposal_approved"
                            or approval["actor_identity"] != APPROVER
                            or approval["occurred_at"] != "2026-09-19T18:56:44.793372+00:00"
                            or approval["lambda_request_id"] != "070e280a-e32e-4ba5-9a52-0ee6bdaf5376"
                            or approval["approval_id"] != "585f171173a88243a4062d316f1aa360c09f9eb54db62fee1ce2c4f63037e20a"
                            or history["approval_sha256"] != APPROVAL_SHA256
                        ):
                            raise AssertionError("Approval evidence differs from the original receipt")
                    previous = audit_by_proposal.setdefault(arguments["proposal_id"], history)
                    if previous != history:
                        raise AssertionError("Repeated read changed the audit history")

                print(f"PASS: {name} {arguments}")
                print(json.dumps(body["result"], indent=2))

            invalid = await session.call_tool(
                "list_rejected_records", {"limit": 0}
            )
            if not invalid.isError:
                raise AssertionError("An invalid limit was accepted")
            print("PASS: invalid rejection limit returns an MCP tool error")

            invalid_inspection = await session.call_tool(
                "inspect_rejected_record", {"rejection_id": 0}
            )
            if not invalid_inspection.isError:
                raise AssertionError("An invalid rejection ID was accepted")
            print("PASS: invalid rejection ID returns an MCP tool error")

            invalid_preview = await session.call_tool(
                "preview_record_correction",
                {
                    "rejection_id": 6,
                    "battery_percent": 150,
                    "reason": "Test rejection of an invalid replacement.",
                },
            )
            if not invalid_preview.isError:
                raise AssertionError("An invalid replacement battery was accepted")
            print("PASS: invalid replacement battery returns an MCP tool error")

            invalid_status = await session.call_tool(
                "get_operation_status", {"proposal_id": "invalid-id"}
            )
            error_text = " ".join(
                block.text for block in invalid_status.content if block.type == "text"
            )
            if not invalid_status.isError or "INVALID_REQUEST" not in error_text:
                raise AssertionError("Expected INVALID_REQUEST for malformed proposal ID")
            print("PASS: invalid proposal ID returns an MCP tool error")

            invalid_audit = await session.call_tool(
                "get_audit_history", {"proposal_id": "invalid-id"}
            )
            error_text = " ".join(
                block.text for block in invalid_audit.content if block.type == "text"
            )
            if not invalid_audit.isError or "INVALID_REQUEST" not in error_text:
                raise AssertionError("Expected INVALID_REQUEST for malformed audit proposal ID")
            print("PASS: invalid audit proposal ID returns an MCP tool error")

            invalid_replay = await session.call_tool(
                "preview_replay", {"proposal_id": "invalid-id"}
            )
            error_text = " ".join(
                block.text for block in invalid_replay.content if block.type == "text"
            )
            if not invalid_replay.isError or "INVALID_REQUEST" not in error_text:
                raise AssertionError("Expected INVALID_REQUEST for malformed replay proposal ID")
            print("PASS: invalid replay proposal ID returns an MCP tool error")

            for proposal_id, expected_code in (
                (APPROVED_PROPOSAL_ID, "PROPOSAL_EXPIRED"),
                (EXPIRED_PROPOSAL_ID, "APPROVAL_REQUIRED"),
                ("invalid-id", "INVALID_REQUEST"),
            ):
                blocked = await session.call_tool("apply_record_correction", {"proposal_id": proposal_id})
                error_text = " ".join(block.text for block in blocked.content if block.type == "text")
                if not blocked.isError or expected_code not in error_text:
                    raise AssertionError(f"Expected {expected_code} for blocked application: {error_text}")
                print(f"PASS: apply_record_correction blocked with {expected_code}")

            applied_id = "987d6f004086868297234141091fd0db704219732f884ebed709e13397aa47ed"
            for proposal_id, expected_code in ((applied_id, "PROPOSAL_EXPIRED"), (EXPIRED_PROPOSAL_ID, "APPROVAL_REQUIRED"), ("invalid-id", "INVALID_REQUEST")):
                blocked = await session.call_tool("replay_records", {"proposal_id": proposal_id})
                message = " ".join(block.text for block in blocked.content if block.type == "text")
                if not blocked.isError or expected_code not in message:
                    raise AssertionError(f"Expected blocked replay {expected_code}, received {message}")
                print(f"PASS: replay_records blocked with {expected_code}")

            for name in ("get_operation_status", "get_audit_history"):
                response = await session.call_tool(name, {"proposal_id": applied_id})
                if response.isError:
                    raise AssertionError(f"{name} failed: {response.content}")
                result = json.loads(next(b.text for b in response.content if b.type == "text"))["result"]
                if result["replay"] != {"found": False, "submission_status": "not_observed"}:
                    raise AssertionError("Blocked replay unexpectedly has submission evidence")
                if name == "get_operation_status":
                    if not result["correction"]["found"] or result["proposal_status"] != "expired":
                        raise AssertionError("Expected expired approval and saved correction")
                elif [e["event_type"] for e in result["events"]] != ["proposal_created", "proposal_approved", "correction_artifact_created"]:
                    raise AssertionError("Blocked replay changed the recorded audit events")
                print(f"PASS: {name} preserves correction evidence without a replay submission")

            resource = await session.read_resource(
                "ring://definitions"
            )
            if not resource.contents:
                raise AssertionError("Definitions resource is empty")
            if "approval actor is recorded separately" not in str(resource):
                raise AssertionError("Definitions do not describe recorded approval identity")
            print("PASS: definitions resource")

            for uri, required in (
                ("ring://schema", "CREATE TABLE public.heartbeat_events"),
                ("ring://runbook", "Approval is not an MCP tool"),
            ):
                reference = await session.read_resource(uri)
                if not reference.contents or required not in reference.contents[0].text:
                    raise AssertionError(f"Missing or incomplete resource: {uri}")
                print(f"PASS: {uri}")

            prompt = await session.get_prompt("investigate_devices")
            if not prompt.messages:
                raise AssertionError("Investigation prompt is empty")
            if "proposal_approval_correction_and_replay" not in str(prompt) or "creation actor is still unknown" not in str(prompt):
                raise AssertionError("Investigation prompt has outdated approval guidance")
            print("PASS: investigation prompt")

            print("AWS MCP connection test passed")


if __name__ == "__main__":
    asyncio.run(main())
